package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// 任务类型
type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	WaitTask  // 让 worker 等一会再来问
	ExitTask  // 所有任务结束，让 worker 退出
)

// 请求任务：worker -> coordinator
type TaskRequest struct{}

// coordinator 给 worker 的任务
type TaskReply struct {
	Type    TaskType // Map / Reduce / Wait / Exit
	File    string   // Map 任务的输入文件名
	TaskId  int      // 任务编号（mapId 或 reduceId）
	NMap    int      // map 任务总数
	NReduce int      // reduce 任务总数
	// 只有 ReduceTask 时有意义：每个 map 任务对应的 worker 地址
    // 下标 = mapId，值 = "ip:port"（HTTP 文件服务地址）
    MapLocations []string
}

// 上报任务完成：worker -> coordinator
type ReportArgs struct {
	Type   TaskType
	TaskId int
	WorkerAddr string // MR_WORKER_ADDR，高级模式下汇报用
}

type ReportReply struct{}


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
