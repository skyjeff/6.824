package mr

import (
	"fmt"
	"path/filepath"
	"runtime"
	"time"
)

// "fmt"

type TaskState int
type TaskType int

type WorkerType int

// task phase
const (
	Map    TaskType = 0
	Reduce TaskType = 1
	Done   TaskType = 3
)

const (
	MaxTaskRunTime   = time.Second * 5
	ScheduleInterval = time.Millisecond * 100
)

const (
	TaskStatusReady     TaskState = 0
	TaskStatusWaiting   TaskState = 1
	TaskStatusRunning   TaskState = 2
	TaskStatusFinished  TaskState = 3
	TaskStatusErr       TaskState = 4
)

const (
	Idle    WorkerType = iota
	Running
)

// 只存放task的信息
type Task struct{
	FileName string
	Seq int //这里对应
	NReduce int
	NMaps int
	Phase TaskType
	Alive bool
}

type TaskStat struct{
	State TaskState
	StartTime time.Time
	WorkerId int
}

const Debug = false
func DPrintf(format string, a ...interface{}) {
	if Debug {
		_, path, lineno, ok := runtime.Caller(1)
		_, file := filepath.Split(path)

		if ok {
			t := time.Now()
			a = append([]interface{}{t.Format("2006-01-02 15:04:05.00"), file, lineno}, a...)
			fmt.Printf("%s [%s:%d] "+format+"\n", a...)
		}
	}
}
