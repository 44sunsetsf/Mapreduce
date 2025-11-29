package mr

// import "log"
// import "net"
// import "os"
// import "net/rpc"
// import "net/http"

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// type Coordinator struct {
// 	// Your definitions here.

// }

// 任务状态
type TaskState int

const (
	Idle TaskState = iota
	InProgress
	Completed
)

// 每个 Map / Reduce 任务的信息
type Task struct {
	State     TaskState
	StartTime time.Time // 记录开始时间，用来判断是否超时
}

// 协调器整体状态
type Coordinator struct {
	mu       sync.Mutex
	files    []string // 输入文件列表
	nReduce  int
	mapTasks []Task
	redTasks []Task
	phase TaskType // 当前阶段：MapTask / ReduceTask / (Done 用 ExitTask 表示)
	mapLocations []string
}


// Your code here -- RPC handlers for the worker to call.

// worker 请求任务
func (c *Coordinator) AssignTask(args *TaskRequest, reply *TaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 先检查有没有超时任务（>10s 就重置为 Idle）
	now := time.Now()
	if c.phase == MapTask {
		for i := range c.mapTasks {
			if c.mapTasks[i].State == InProgress && now.Sub(c.mapTasks[i].StartTime) > 10*time.Second {
				c.mapTasks[i].State = Idle
			}
		}
	} else if c.phase == ReduceTask {
		for i := range c.redTasks {
			if c.redTasks[i].State == InProgress && now.Sub(c.redTasks[i].StartTime) > 10*time.Second {
				c.redTasks[i].State = Idle
			}
		}
	}

	// 如果还在 Map 阶段
	if c.phase == MapTask {
		for i := range c.mapTasks {
			if c.mapTasks[i].State == Idle {
				c.mapTasks[i].State = InProgress
				c.mapTasks[i].StartTime = now

				reply.Type = MapTask
				reply.File = c.files[i]
				reply.TaskId = i
				reply.NMap = len(c.files)
				reply.NReduce = c.nReduce
				return nil
			}
		}
		// 没空闲任务，但还没全部完成，让 worker 等一下
		reply.Type = WaitTask
		return nil
	}

	// Map 完成，进入 Reduce 阶段
	if c.phase == ReduceTask {
		for i := range c.redTasks {
			if c.redTasks[i].State == Idle {
				c.redTasks[i].State = InProgress
				c.redTasks[i].StartTime = now

				reply.Type = ReduceTask
				reply.TaskId = i
				reply.NMap = len(c.files)
				reply.NReduce = c.nReduce
				// 把所有 map 任务的位置发给 worker（高级模式下使用）
				reply.MapLocations = make([]string, len(c.mapLocations))
				copy(reply.MapLocations, c.mapLocations)
				return nil
			}
		}
		reply.Type = WaitTask
		return nil
	}

	// 所有工作完成，让 worker 退出
	reply.Type = ExitTask
	return nil
}

// worker 汇报任务完成
func (c *Coordinator) ReportTask(args *ReportArgs, reply *ReportReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch args.Type {
	case MapTask:
		if c.phase != MapTask {
			return nil
		}
		if args.TaskId >= 0 && args.TaskId < len(c.mapTasks) &&
			c.mapTasks[args.TaskId].State == InProgress {
			c.mapTasks[args.TaskId].State = Completed
			// 记录 map 的 worker 地址（只有高级模式会有这个值）
			if args.WorkerAddr != "" {
				c.mapLocations[args.TaskId] = args.WorkerAddr
			}
		}

		// 检查是否所有 Map 任务都完成
		allDone := true
		for _, t := range c.mapTasks {
			if t.State != Completed {
				allDone = false
				break
			}
		}
		if allDone {
			c.phase = ReduceTask
		}

	case ReduceTask:
		if c.phase != ReduceTask {
			return nil
		}
		if args.TaskId >= 0 && args.TaskId < len(c.redTasks) &&
			c.redTasks[args.TaskId].State == InProgress {
			c.redTasks[args.TaskId].State = Completed
		}

		// 检查是否所有 Reduce 完成
		allDone := true
		for _, t := range c.redTasks {
			if t.State != Completed {
				allDone = false
				break
			}
		}
		if allDone {
			c.phase = ExitTask
		}
	}

	return nil
}


//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//

func (c *Coordinator) server() {
    rpc.Register(c)
    rpc.HandleHTTP()

    if os.Getenv("MR_USE_TCP") == "1" {
        // 高级模式：监听 TCP
        l, e := net.Listen("tcp", ":1234")
        if e != nil {
            log.Fatal("listen error:", e)
        }
        go http.Serve(l, nil)
        return
    }

    // 基础模式：监听 Unix Socket
    sockname := coordinatorSock()
    os.Remove(sockname)
    l, e := net.Listen("unix", sockname)
    if e != nil {
        log.Fatal("listen error:", e)
    }
    go http.Serve(l, nil)
}


//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//

func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	// phase 进入 ExitTask，说明所有 Reduce 任务完成
	return c.phase == ExitTask
}


//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//

// 本函数用于“worker 去连谁”。
// 本地测试：默认连 127.0.0.1:1234
// 多机部署：通过环境变量 MR_COORD_ADDR 指定，比如 "1.2.3.4:1234"
func coordinatorAddr() string {
    if addr := os.Getenv("MR_COORD_ADDR"); addr != "" {
        return addr
    }
    return "127.0.0.1:1234"
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:    files,
		nReduce:  nReduce,
		mapTasks: make([]Task, len(files)),
		redTasks: make([]Task, nReduce),
		phase:    MapTask, // 先从 Map 阶段开始
		mapLocations: make([]string, len(files)),
	}

	c.server()
	return &c
}
