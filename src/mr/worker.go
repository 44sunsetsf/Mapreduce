package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/http"
	"net/rpc"
	"os"
	"sort"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func reportDone(t TaskType, id int) {
	args := ReportArgs{
		Type:   t,
		TaskId: id,
	}
	// 只有 map 任务在高级模式下需要汇报自己的地址
    if t == MapTask && os.Getenv("MR_USE_TCP") == "1" {
        addr := os.Getenv("MR_WORKER_ADDR")
        if addr == "" {
            log.Fatal("MR_WORKER_ADDR not set in distributed mode")
        }
        args.WorkerAddr = addr
    }
	reply := ReportReply{}
	call("Coordinator.ReportTask", &args, &reply)
}


func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// 如果是高级模式，就起一个 HTTP 文件服务器
    if os.Getenv("MR_USE_TCP") == "1" {
        addr := os.Getenv("MR_WORKER_ADDR") // 例如 "10.0.0.5:8000"
        if addr == "" {
            log.Fatal("MR_WORKER_ADDR not set in distributed mode")
        }
        go func() {
            // 把当前目录作为静态文件目录提供出去
            http.Handle("/", http.FileServer(http.Dir(".")))
            log.Printf("worker file server listen on %v\n", addr)
            if err := http.ListenAndServe(addr, nil); err != nil {
                log.Fatalf("file server error: %v", err)
            }
        }()
    }

	for {
		// 1. 向 coordinator 请求任务
		req := TaskRequest{}
		reply := TaskReply{}
		ok := call("Coordinator.AssignTask", &req, &reply)
		if !ok {
			// 一般是 coordinator 退出了
			// 在基础/高级模式下，这都意味着：协调者没活了或网络不通
        	log.Println("cannot reach coordinator, assume it has exited; worker will exit")
			return
		}

		switch reply.Type {
		case MapTask:
			doMapTask(&reply, mapf)
			reportDone(MapTask, reply.TaskId)

		case ReduceTask:
			doReduceTask(&reply, reducef)
			reportDone(ReduceTask, reply.TaskId)

		case WaitTask:
			time.Sleep(time.Second)

		case ExitTask:
			return
		}
	}
}


// 处理 Map 任务
func doMapTask(task *TaskReply, mapf func(string, string) []KeyValue) {
	filename := task.File
	content, err := ioutil.ReadFile(filename)
	if err != nil {
		log.Fatalf("cannot read %v: %v", filename, err)
	}

	// 调用用户提供的 map 函数
	kva := mapf(filename, string(content))

	// 按 key 分桶到不同的 reduce
	buckets := make([][]KeyValue, task.NReduce)
	for _, kv := range kva {
		r := ihash(kv.Key) % task.NReduce
		buckets[r] = append(buckets[r], kv)
	}

	// 每个 reduceId 对应一个中间文件 mr-MapId-ReduceId
	for r := 0; r < task.NReduce; r++ {
		oname := fmt.Sprintf("mr-%d-%d", task.TaskId, r)
		tmpfile, err := os.CreateTemp("", "mr-tmp-*")
		if err != nil {
			log.Fatalf("cannot create temp file: %v", err)
		}
		enc := json.NewEncoder(tmpfile)
		for _, kv := range buckets[r] {
			if err := enc.Encode(&kv); err != nil {
				log.Fatalf("encode error: %v", err)
			}
		}
		tmpname := tmpfile.Name()
		tmpfile.Close()

		// 原子重命名，避免 crash 时留下半截文件
		if err := os.Rename(tmpname, oname); err != nil {
			log.Fatalf("rename error: %v", err)
		}
	}
}

func doReduceTask(task *TaskReply, reducef func(string, []string) string) {
    kva := []KeyValue{}

    useDistributed := os.Getenv("MR_USE_TCP") == "1"

    for m := 0; m < task.NMap; m++ {
        iname := fmt.Sprintf("mr-%d-%d", m, task.TaskId)

        if useDistributed {
            // 高级模式：通过 HTTP 从对应的 map worker 拉取文件
            if m >= len(task.MapLocations) || task.MapLocations[m] == "" {
                // 没有记录位置，跳过（可能是失败的 map，或基础模式）
                continue
            }
            addr := task.MapLocations[m] // "ip:8000"
            url := fmt.Sprintf("http://%s/%s", addr, iname)
            resp, err := http.Get(url)
            if err != nil {
                log.Printf("cannot fetch %v from %v: %v", iname, addr, err)
                continue
            }
            dec := json.NewDecoder(resp.Body)
            for {
                var kv KeyValue
                if err := dec.Decode(&kv); err != nil {
                    break
                }
                kva = append(kva, kv)
            }
            resp.Body.Close()
        } else {
            // 基础模式：本地直接打开
            file, err := os.Open(iname)
            if err != nil {
                continue
            }
            dec := json.NewDecoder(file)
            for {
                var kv KeyValue
                if err := dec.Decode(&kv); err != nil {
                    break
                }
                kva = append(kva, kv)
            }
            file.Close()
        }
    }

    // 后面的排序 + reducef 不变
    sort.Slice(kva, func(i, j int) bool {
        return kva[i].Key < kva[j].Key
    })

    oname := fmt.Sprintf("mr-out-%d", task.TaskId)
    tmpfile, err := os.CreateTemp("", "mr-out-tmp-*")
    if err != nil {
        log.Fatalf("cannot create temp output: %v", err)
    }

    i := 0
    for i < len(kva) {
        j := i + 1
        for j < len(kva) && kva[j].Key == kva[i].Key {
            j++
        }
        var values []string
        for k := i; k < j; k++ {
            values = append(values, kva[k].Value)
        }
        output := reducef(kva[i].Key, values)
        fmt.Fprintf(tmpfile, "%v %v\n", kva[i].Key, output)
        i = j
    }

    tmpname := tmpfile.Name()
    tmpfile.Close()
    if err := os.Rename(tmpname, oname); err != nil {
        log.Fatalf("rename output error: %v", err)
    }
}


// 处理 Reduce 任务
// func doReduceTask(task *TaskReply, reducef func(string, []string) string) {
// 	// 读取所有 mr-X-Y，其中 Y = task.TaskId
// 	kva := []KeyValue{}
// 	for m := 0; m < task.NMap; m++ {
// 		iname := fmt.Sprintf("mr-%d-%d", m, task.TaskId)
// 		file, err := os.Open(iname)
// 		if err != nil {
// 			// 有可能这个 map 任务没生成该文件，直接略过
// 			continue
// 		}
// 		dec := json.NewDecoder(file)
// 		for {
// 			var kv KeyValue
// 			if err := dec.Decode(&kv); err != nil {
// 				break
// 			}
// 			kva = append(kva, kv)
// 		}
// 		file.Close()
// 	}

// 	// 按 key 排序
// 	sort.Slice(kva, func(i, j int) bool {
// 		return kva[i].Key < kva[j].Key
// 	})

// 	// 写入最终输出 mr-out-X
// 	oname := fmt.Sprintf("mr-out-%d", task.TaskId)
// 	tmpfile, err := os.CreateTemp("", "mr-out-tmp-*")
// 	if err != nil {
// 		log.Fatalf("cannot create temp output: %v", err)
// 	}

// 	i := 0
// 	for i < len(kva) {
// 		j := i + 1
// 		for j < len(kva) && kva[j].Key == kva[i].Key {
// 			j++
// 		}
// 		var values []string
// 		for k := i; k < j; k++ {
// 			values = append(values, kva[k].Value)
// 		}
// 		output := reducef(kva[i].Key, values)
// 		// 注意：格式必须是 "%v %v\n"
// 		fmt.Fprintf(tmpfile, "%v %v\n", kva[i].Key, output)
// 		i = j
// 	}

// 	tmpname := tmpfile.Name()
// 	tmpfile.Close()
// 	if err := os.Rename(tmpname, oname); err != nil {
// 		log.Fatalf("rename output error: %v", err)
// 	}
// }



//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//

func call(rpcname string, args interface{}, reply interface{}) bool {
    var c *rpc.Client
    var err error

    if os.Getenv("MR_USE_TCP") == "1" {
        // AWS / 多机模式
        addr := os.Getenv("MR_COORD_ADDR") // 例如 "3.88.12.34:1234"
        c, err = rpc.DialHTTP("tcp", addr)
    } else {
        // 本地基础测试模式
        sockname := coordinatorSock()
        c, err = rpc.DialHTTP("unix", sockname)
    }

    if err != nil {
        return false
    }
    defer c.Close()

    err = c.Call(rpcname, args, reply)
    return err == nil
}

