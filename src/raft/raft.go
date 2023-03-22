package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	_ "fmt"
	_"fmt"
	"math/rand"
	_ "net/http"
	_ "net/http/pprof"
	"sync"
	"time"

)
import "sync/atomic"
import "../labrpc"

import "bytes"
import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm int
}

type LogEntry struct {
	Command interface{}
	Term int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan ApplyMsg

	state        NodeState
	//lastRec      time.Time
	//electTimeout time.Duration    // will update when reset the timeout

	electionTimer  *time.Timer
	heartbeatTimer *time.Timer

	currentTerm  int
	voteFor      int
	log          []LogEntry

	commitIndex int
	lastApplied int

	nextIndex    []int
	matchIndex   []int
	replicatorCond []sync.Cond
	applierCond sync.Cond
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	term = rf.currentTerm
	isleader = rf.state == Leader
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//

func (rf *Raft) EncodePersitedData() (data []byte){
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := e.Encode(rf.currentTerm); err != nil {
		DPrintf("encode currentTerm error!!")
	}
	if err := e.Encode(rf.voteFor); err != nil {
		DPrintf("encode voteFor error!!")
	}
	if err := e.Encode(rf.log); err != nil {
		DPrintf("encode log error!!")
	}
	data = w.Bytes()
	return
}
func (rf *Raft) persist() {
	DPrintf("start persisting...")
	data := rf.EncodePersitedData()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var term int
	var voteFor int
	var log []LogEntry
	if d.Decode(&term) != nil ||
			d.Decode(&voteFor) != nil ||
			d.Decode(&log) != nil{
		DPrintf("read persist data error!!")
	} else{
		rf.currentTerm = term
		rf.voteFor = voteFor
		rf.log = log
	}
}

func getRandomElectTimeout() time.Duration {
	return time.Duration(250 + rand.Int31n(150)) * time.Millisecond
}


//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	lastIndex, _  := rf.getLastLogIndexTerm()
	index := lastIndex + 1
	term := rf.currentTerm
	isLeader := rf.state == Leader
	if isLeader {
		rf.log = append(rf.log, LogEntry{
			Term:    rf.currentTerm,
			Command: command,
		})

		//rf.matchIndex[rf.me] = index
		//rf.nextIndex[rf.me] = index + 1
		rf.persist()
		DPrintf("new command %+v come in, and now leader [%d] log is %+v", command, rf.me, rf.log)
		rf.startHeartBeats(false)
	}

	return index, term, isLeader
}


//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		mu: sync.RWMutex{},
		peers: peers,
		persister: persister,
		me: me,
		dead: -1,
		state: Follwer,
		electionTimer: time.NewTimer(getRandomElectTimeout()),
		heartbeatTimer: time.NewTimer(HeartBeatsInterval),

		currentTerm: 1,
		voteFor: -1,
		log: make([]LogEntry, 1),
		commitIndex: 0,
		lastApplied: 0,
		applyCh: applyCh,

		nextIndex: make([]int, len(peers)),
		matchIndex: make([]int, len(peers)),
		replicatorCond: make([]sync.Cond, len(peers)),
	}
	rf.applierCond = *sync.NewCond(&rf.mu)
	rf.log[0] = LogEntry{nil, 0}
	for i := 0; i < len(rf.peers); i++{
		rf.nextIndex[i] = 1
		rf.matchIndex[i] = 0
		if i == me{
			continue
		}
		rf.replicatorCond[i] = *sync.NewCond(&sync.Mutex{})
		go rf.replicator(i)
	}

	go rf.Ticker()

	go rf.applier(applyCh)

	// pprof check the goroutine state
	//go func() {
	//	fmt.Println(http.ListenAndServe(":6060", nil))
	//}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func (rf *Raft) Ticker()  {
	DPrintf("ticker di di di")
	for !rf.killed(){
		select {
		case <- rf.electionTimer.C:
			rf.mu.Lock()
			if rf.state != Leader{
				DPrintf("[%d] election timeout", rf.me)
				go rf.startElection()
				//rf.resetElectionTimer()
			}
			rf.mu.Unlock()
			// maybe need to use lock
		case <- rf.heartbeatTimer.C:
			rf.mu.Lock()
			if rf.state == Leader{
				resetTimer(rf.heartbeatTimer, HeartBeatsInterval)
				rf.startHeartBeats(true)
			}
			rf.mu.Unlock()
		}
	}
}
func (rf *Raft) needApply() bool{
	return rf.lastApplied < rf.commitIndex
}
func (rf *Raft) applier(applyCh chan ApplyMsg){
	for !rf.killed(){
		rf.mu.Lock()
		for !rf.needApply(){
			rf.applierCond.Wait()
		}
		DPrintf("[%d] start to apply, it commitIndex is %d, lastApplied is %d, \n -log is %+v", rf.me, rf.commitIndex, rf.lastApplied, rf.log)
		//rf.mu.RUnlock()
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++{
			msg := ApplyMsg{
				CommandValid: true,
				Command: rf.log[i].Command,
				CommandIndex: i,
				CommandTerm: rf.currentTerm,
			}
			applyCh <- msg
		}
		//rf.mu.RLock()
		if rf.commitIndex > rf.lastApplied{
			rf.lastApplied = rf.commitIndex
		}
		rf.mu.Unlock()
	}
}
