package kvraft

import (
	"../labgob"
	"../labrpc"
	"fmt"
	"log"
	"../raft"
	"sync"
	"sync/atomic"
	"time"
	"net/http"
	_ "net/http/pprof"
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


//type Op struct {
//	// Your definitions here.
//	// Field names must start with capital letters,
//	// otherwise RPC will break.
//	Key string
//	Value string
//	Operation int
//	ClientId int
//	SequenceNum int
//}
type Session struct {
	requestID int
	commandReply *CommandReply
}

type KVServer struct {
	mu      sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big


	waitApplyCh map[int] chan *CommandReply
	lastSession map[int64] Session
	stateMachine KVStateMachine

	lastAppliedIndex int
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {

}

/**
 * 当kv-server成功apply后，会向相应index的waitChan中发送小心，这样在command方法中阻塞的chan就会被重新唤醒，然后返回client 此次command的响应结果
 */
func (kv *KVServer) getWaitChan(index int) chan *CommandReply{
	ch, ok := kv.waitApplyCh[index]
	if !ok {
		ch := make(chan *CommandReply, 1)
		kv.waitApplyCh[index] = ch
	}
	return ch
}

func (kv *KVServer) removeWaitChan(index int){
	ch, ok := kv.waitApplyCh[index]
	if ok {
		delete(kv.waitApplyCh, index)
		close(ch)
	}
}


func (kv *KVServer) Command(args *CommandArgs, reply *CommandReply) {

	raftIndex, _, isLeader := kv.rf.Start(*args)
	DPrintf("[server %d] Leader is %v", kv.me, isLeader)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	waitCh := kv.getWaitChan(raftIndex)
	kv.mu.Unlock()

	select {
	case result := <- waitCh:
		reply.Err = result.Err
		reply.Value = result.Value

	case <- time.NewTimer(500 * time.Millisecond).C:
		reply.Err = ErrTimeOut
	}

	kv.mu.Lock()
	kv.removeWaitChan(raftIndex)
	kv.mu.Unlock()

	return
}

func (kv *KVServer) isDuplicatedRequest(clientID int64, requestID int) bool {
	session, ok := kv.lastSession[clientID]
	return ok && session.requestID == requestID
}

func (kv *KVServer) applyCommand(command CommandArgs) *CommandReply{
	reply := &CommandReply{}
	switch command.Op {
	case OpPut:
		reply.Value, reply.Err = "", kv.stateMachine.Put(command.Key, command.Value)
	case OpAppend:
		reply.Value, reply.Err = "", kv.stateMachine.Append(command.Key, command.Value)
	case OpGet:
		reply.Value, reply.Err = kv.stateMachine.Get(command.Key)
	}
	return reply
}

func (kv *KVServer) applier(){
	for !kv.killed(){
		select {
		case applyMsg := <- kv.applyCh:
			if applyMsg.CommandValid {
				command := applyMsg.Command.(CommandArgs)

				kv.mu.Lock()
				if kv.lastAppliedIndex >= applyMsg.CommandIndex {
					DPrintf("[KVServer %d] discard out-of-date apply Msg in [index %d]", kv.me, applyMsg.CommandIndex)
					kv.mu.Unlock()
					continue
				}

				kv.lastAppliedIndex = applyMsg.CommandIndex
				var reply *CommandReply
				// 对于写请求，由于其会改变系统状态，因此在执行到状态机之前需要去重，仅对不重复的日志进行 apply 并记录执行结果，保证其仅被执行一次。
				// 对于读请求，由于其不影响系统状态，所以直接去状态机执行即可，当然，其结果也不需要再记录到去重的数据结构中。
				// 由于读操作也在log中而apply后才执行，所以仍然是集群中有majority回应后才会读取，以免读到网络分区中旧leader的值
				if command.Op != OpGet && kv.isDuplicatedRequest(command.ClientID, command.RequestID){
					DPrintf("[KVServer %d] receive a duplicated command", kv.me)
					reply = kv.lastSession[command.ClientID].commandReply
				} else {
					reply := kv.applyCommand(command)
					if command.Op != OpGet{
						kv.lastSession[command.ClientID] = Session{
							requestID: command.RequestID,
							commandReply: reply,
						}
					}
				}
				DPrintf("apply success! state machine is %v", kv.stateMachine)
				if currentTerm, isLeader := kv.rf.GetState(); currentTerm == applyMsg.CommandTerm && isLeader {
					ch := kv.getWaitChan(applyMsg.CommandIndex)
					ch <- reply
				}
				kv.mu.Unlock()
			}
		}
	}
}

func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(CommandArgs{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)


	kv.waitApplyCh = make(map[int]chan *CommandReply)
	kv.lastSession = make(map[int64]Session)
	kv.stateMachine = createNormalDB()
	kv.lastAppliedIndex = -1
	go func() {
		fmt.Println(http.ListenAndServe(":6060", nil))
	}()

	go kv.applier()
	return kv
}
