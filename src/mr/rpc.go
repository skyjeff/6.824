package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

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

// Add RPC definitions here.
// TYPE：
// 0 从master获取tasks
type Args struct {
	TYPE int
	TEXT string
}

type Reply struct {
	
}

type RegisterArgs struct {}

type RegisterReply struct{
	WorkId int
}

type RegTaskArgs struct {
	WorkerId int
}

type RegTaskReply struct {
	Task *Task
}

type ReportTaskArgs struct {
	Task Task
	State bool
	Err error
	WorkId int
}

type ReportTaskReply struct {

}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
