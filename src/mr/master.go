package mr

import (
	_ "fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
	_ "net/http/pprof"
)

type Master struct {
	mu sync.Mutex
	MasterState int

	files []string
	nWorker int
	nRuduce int
	nTask int
	workerSeq int 
	taskPhase TaskType
	taskSeq int // 对应task的序号，对应在stats和files中
	taskStats []TaskStat // 用seq和Task对应
	taskCh chan Task // 用作一个先进先出的队列存放Task，这里存放的task和序号有关
	finishedTask int // 用来保存已经完成的任务的个数，可以方便判断nTask和其关系，判断是否完成
	isDone bool
}


// RPC handlers for the worker to call.

// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}
func (m *Master) Pong(args *Args, reply *Reply) error {
	return nil
}

func (m *Master) Register(args *RegisterArgs, reply *RegisterReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	reply.WorkId = m.workerSeq
	m.workerSeq++
	m.nWorker++
	DPrintf("a new worker registered, seq is %d", reply.WorkId)
	return nil
}

func (m *Master) HandleGetTask(args *RegTaskArgs, reply *RegTaskReply) error {
	// todo
	task := <- m.taskCh

	m.taskStats[task.Seq].State = TaskStatusRunning
	m.taskStats[task.Seq].WorkerId = args.WorkerId
	m.taskStats[task.Seq].StartTime = time.Now()
	reply.Task = &task
	return nil
}

func (m *Master) HandleReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	//m.mu.Lock()
	//defer m.mu.Unlock()

	task := args.Task
	task.Alive = false

	DPrintf("the task %d is report, and it state is %v", task.Seq, args.State)
	if args.State{
		m.taskStats[task.Seq].State = TaskStatusFinished
		//DPrintf("task state %+v", m.taskStats)
	} else{
		m.taskStats[task.Seq].State = TaskStatusErr
	}
	return nil
}
//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.isDone
}

func (m *Master) initMapTasks(){
	m.mu.Lock()
	defer m.mu.Unlock()

	m.nTask = len(m.files)
	m.finishedTask = 0
	m.taskSeq = 0
	// 因为后续要reduce任务放入chanel中，所以需要判断放多大
	m.taskStats = make([]TaskStat, len(m.files))
	m.taskPhase = Map

	DPrintf("init map task, there are %d tasks", len(m.files))
	// 为每个task开启一个timer，即一个线程, 即模拟分布式，假设一个线程就是一台机器
	for seq := range m.taskStats {
		go m.schedule(seq)
	}
}

func (m *Master) initReduceTask() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.nTask = m.nRuduce
	m.finishedTask = 0
	m.taskSeq = 0
	m.taskStats = make([]TaskStat, m.nRuduce)
	DPrintf("init reduce task, there are %d tasks", m.nRuduce)
	for id := 0 ; id < m.nRuduce ; id++{
		go m.schedule(id)
	}
}


// 根据taskSeq来创建一个新的task
func (m *Master) createOneTask(taskSeq int) Task{

	task := Task{
		FileName: "",
		Seq: taskSeq,
		NReduce: m.nRuduce,
		NMaps: len(m.files),
		Phase: m.taskPhase,
		Alive: true,
	}

	DPrintf("Get task, taskseq:%d, len files:%d, len tasks:%d", m, taskSeq, len(m.files), len(m.taskStats))
	if task.Phase == Map {
		task.FileName = m.files[taskSeq]
	}

	return task
}

func (m *Master) schedule(taskSeq int){
	for {
		// 这里我一开始将获取锁放在循环外面，但是这里是sleep前就要丢弃锁，
		// 然后下次进入循环后，就没获得锁了
		// 这里只是单纯地处理task，而不会去关心task的类型
		m.mu.Lock()
		switch m.taskStats[taskSeq].State{
			case TaskStatusReady:
				m.taskStats[taskSeq].State = TaskStatusWaiting
				m.taskCh <- m.createOneTask(taskSeq)
			case TaskStatusWaiting:
			case TaskStatusRunning:
				// 判断任务是否超时
				if time.Since(m.taskStats[taskSeq].StartTime) > MaxTaskRunTime{
					DPrintf("task %d is timeout", taskSeq)
					m.taskStats[taskSeq].State = TaskStatusWaiting
					m.taskCh <- m.createOneTask(taskSeq)
				}
			case TaskStatusFinished:
				// 假如这个线程知道这个任务已经结束了，那么直接结束这个监听
				m.finishedTask += 1
				DPrintf("finished task num %d", m.finishedTask)
				DPrintf("task %d is finished", taskSeq)
				m.mu.Unlock()
				return
			case TaskStatusErr:
				m.taskStats[taskSeq].State = TaskStatusWaiting
				m.taskCh <- m.createOneTask(taskSeq)
		}
		DPrintf("Schedule finished, taskseq: %d, taskState: %d", taskSeq, m.taskStats[taskSeq].State)
		m.mu.Unlock()
		time.Sleep(ScheduleInterval)
	}
}

//前面map线程在各自拥有的task完成后就会退出，而这个线程是用来判断所处阶段是否完成和以及是否Map和Reduce全done
func (m *Master) tickSchedule() {
	// m.isDone是来用来标识整个mapreduce是否完成的，所以这里直接用finishedTask来判断map阶段是否完成
	//
	for !m.Done(){
		m.mu.Lock()
		if m.finishedTask == m.nTask {
			switch m.taskPhase {
				case Map:
					m.mu.Unlock()
					// 开启ReducePhase
					m.taskPhase = Reduce
					DPrintf("all the Map Task are finished")
					m.initReduceTask()
					time.Sleep(ScheduleInterval)
					continue //要不然会解锁两次，出错
				case Reduce:
					// 假如Reduce阶段也全完成了，那么说明整个MR任务结束
					m.isDone = true
			}
		}
		m.mu.Unlock()
		time.Sleep(ScheduleInterval)
	}

	DPrintf("the whole application are finished")
	DPrintf("exit")
}


// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.files = files
	m.nRuduce = nReduce
	m.taskCh = make(chan Task, len(m.files))
	m.taskSeq = 0
	m.isDone = false
	
	m.initMapTasks()
	m.server()
	//go func() {
	//	if err := http.ListenAndServe("0.0.0.0:6060", nil); err != nil {
	//		fmt.Println("0.0.0.0:6060", err.Error())
	//	}
	//}()
	go m.tickSchedule()
	return &m
}
