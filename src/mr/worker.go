package mr

import (
	"fmt"
	"hash/fnv"
	_ "log"
	"net/rpc"
	"sort"
	"time"

	//"strconv"
	"sync"
	"os"
	"io/ioutil"
	"encoding/json"
)

//
// Map functions return a slice of KeyValue.
//

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type KeyValue struct {
	Key   string
	Value string
}

// 因为这里是worker主动请求任务的，所以这里worker的状态不需要保存
type worker struct {
	mu sync.Mutex
	id int
	status WorkerType
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
}

func fileName(MapSeq int, reduceSeq int) string {
	//fmt.Printf("fileName concat error 1, --%d--, --%d--\n", MapSeq,reduceSeq)
	return fmt.Sprintf("mr-%d-%d", MapSeq, reduceSeq)
}

func mergeName(reduceIdx int) string {
	//fmt.Printf("fileName concat error 2, --%d--\n", reduceIdx)
	return fmt.Sprintf("mr-out-%d", reduceIdx)
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

// Worker
// main/mrworker.go calls this function.
// 需要向master来注册本身
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	// 用来标识该worker线程的信息
	w := worker{}
	w.status = Idle
	w.mapf = mapf
	w.reducef = reducef

	w.callRegister()
	//fmt.Println(w.id)

	//task := w.callRegTask()
	//w.processMapTask(task)
	w.run()
	// uncomment to send the Example RPC to the master.
	// CallExample()
}

func (w *worker) run(){
	for {
		if !w.Ping() {
			return
		}
		task := w.callRegTask()
		if task == (Task{}) {
			DPrintf("worker %v exit", w.id)
			return
		}
		if !task.Alive{
			DPrintf("worker %v get task not alive, exit", w.id)
			return
		}
		DPrintf("worker %v get task alive", w.id)
		w.doTask(task)
		time.Sleep(ScheduleInterval)
	}
}

func (w *worker) doTask(task Task) {
	switch task.Phase {
	case Map:
		w.doMapTask(task)
	case Reduce:
		w.doReduceTask(task)
	default:
		DPrintf("task type is error")
		w.reportTask(task, false, nil)
	}
}

// 向master注册自身，获取id
func (w *worker) callRegister(){
	args := RegisterArgs{}
	reply := RegisterReply{}
	call("Master.Register", &args, &reply)
	w.id = reply.WorkId
}

func (w *worker) callRegTask() Task{
	args := RegTaskArgs{}
	reply := RegTaskReply{}
	args.WorkerId = w.id

	state := call("Master.HandleGetTask", &args, &reply)
	if !state {
		return Task{}
	}
	DPrintf("worker %d get a new task %+v", w.id, reply.Task)
	return *reply.Task
}

// 发送心跳包，用来判断master是否已经死亡
func (w *worker) Ping() bool {
	args := Args{}
	reply := Reply{}
	state := call("Master.Pong", &args, &reply)
	return state
}

func (w *worker) doMapTask(task Task) {

	/***************************** Map阶段 **********************************/
	filename := task.FileName
	//fmt.Println("filename is ", filename)

	content, err := ioutil.ReadFile(filename)
	if err != nil {
		DPrintf("cannot read %v", filename)
	}
	kvarr := w.mapf(filename, string(content))
	// crash
	//if kvarr[0].Key == "a" && kvarr[0].Value == filename{
	//	w.reportTask(task, false, nil)
	//	return
	//}
	// slice... 是添加一整个数组，在尾部追加


	/***************************** 生成中间键值对 **********************************/


	// 这里为单个task创建NReduce个文件，用来存放中间KV，哪个键值对存放在哪个文件中取决于key的hash
	reduceFiles := make([][]KeyValue, task.NReduce)
	for _, kv := range kvarr {
		idx := ihash(kv.Key) % task.NReduce
		reduceFiles[idx] = append(reduceFiles[idx], kv)
	}

	for idx, kva := range reduceFiles{
		//fmt.Printf("%s", fileName(task.Seq, idx))
		filename := fileName(task.Seq, idx)
		tmpFile, err := os.Create(filename)
		if err != nil {
			DPrintf("cannot create %v", tmpFile)
			w.reportTask(task, false, err)
		}
		enc := json.NewEncoder(tmpFile)
		for _, kv := range kva {
			if err := enc.Encode(&kv); err != nil {
				DPrintf("cannot write file %v", tmpFile)
				w.reportTask(task, false, err)
			}
		}
		if err := tmpFile.Close(); err != nil {
			DPrintf("cannot close file %v", tmpFile)
			w.reportTask(task, false, err)
		}
		DPrintf("worker %v created intermedia file %v", w.id, filename)
	}
	w.reportTask(task, true, nil)
}

func (w *worker) doReduceTask(task Task) {
	//从mr-M-R中获取数据，其中M是Map任务的长度，即bucket大小，R是当前reduce task序号
	kva := []KeyValue{}
	for i := 0 ; i < task.NMaps ; i++ {
		fileName_ := fileName(i, task.Seq)
		file, err := os.Open(fileName_)
		if err != nil {
			DPrintf("cannot create %v", file)
			w.reportTask(task, false, err)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				DPrintf("cannot reade file %v", fileName_)
				w.reportTask(task, false, err)
				break
			}
			kva = append(kva, kv)
		}
		if err = os.Remove(fileName_) ; err != nil {
			DPrintf("worker %v failed to delete intermedia file %v", w.id, fileName_)
			w.reportTask(task, false, err)
		} else {
			DPrintf("worker %v deleted intermedia file %v", w.id, fileName_)
		}
		file.Close()
	}

	sort.Sort(ByKey(kva))
	oname := mergeName(task.Seq)
	ofile, _ := os.Create(oname)

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := w.reducef(kva[i].Key, values)

		//crash
		//if len(output)> 1000{
		//	w.reportTask(task, false, nil)
		//	return
		//}

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}
	DPrintf("worker %v created the final file %v", w.id, oname)
	w.reportTask(task, true, nil)
	ofile.Close()
}


func (w *worker) reportTask(task Task, state bool, err error) {
	if err != nil {
		DPrintf("error is %v", err.Error())
	}
	args := ReportTaskArgs{
		Task : task,
		State : state,
		Err : err,
		WorkId: w.id,
	}
	reply := ReportTaskReply{}

	if state{
		DPrintf("task %d is finished by worker %d", task.Seq, w.id)
	} else{
		DPrintf("task %d is error by worker %d", task.Seq, w.id)

	}
	call("Master.HandleReportTask", &args, &reply)
}




// RPC call 的例子，如何使用RPC到master中，规范参数需要定义在在rpc.go中
// example function to show how to make an RPC call to the master.
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
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		DPrintf("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
