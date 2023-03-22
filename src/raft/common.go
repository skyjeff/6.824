package raft

import "time"

type NodeState int

const (
	HeartBeatsInterval = time.Millisecond * 100
)

const (
	HeartBeatsFlag = 0
	AppendEntriesFlag = 1
)


const(
	Follwer   NodeState = iota
	Candidate
	Leader
)


// utils
// 这里只关注换角色，以及换角色之后的动作(赋值)，锁在外面使用
// e.g. election time out, follwer -> candidate
// vote fail, candidate -> follwer
// vote success, candiate -> leader
func (rf *Raft) changeState(state NodeState){
	rf.state = state
	switch rf.state {
	case Follwer:
		DPrintf("[%d] change to follwer at term %d", rf.me, rf.currentTerm)
		rf.voteFor = -1
	case Candidate:
		rf.currentTerm += 1
		rf.voteFor = rf.me
		rf.resetElectionTimer()
		rf.persist()
		DPrintf("[%d] change to candiate at term %d", rf.me, rf.currentTerm)
	case Leader:
		lastLogIndex, _ := rf.getLastLogIndexTerm()
		for i := 0; i < len(rf.peers); i++{
			rf.nextIndex[i] = lastLogIndex + 1
			rf.matchIndex[i] = 0
		}
		rf.matchIndex[rf.me] = lastLogIndex
		//rf.electionTimer.Stop()
		DPrintf("[%d] change to leader at term %d", rf.me, rf.currentTerm)
	}
}

func (rf *Raft) getLastLogIndexTerm() (lastLogIndex int, lastLogTerm int) {
	// outer to decide to use lock
	//rf.mu.RLock()
	//defer rf.mu.RUnlock()

	lastLogIndex = len(rf.log) - 1
	lastLogTerm = rf.log[lastLogIndex].Term
	return
}

func resetTimer(timer *time.Timer, timeout time.Duration){
	if !timer.Stop() && len(timer.C) > 0{
		<- timer.C
	}
	timer.Reset(timeout)
}

func stopTimer(timer *time.Timer){
	if !timer.Stop() && len(timer.C) > 0{
		<- timer.C
	}
}

func minValue(v1 int, v2 int) int {
	if v1 < v2{
		return v1
	}
	return v2
}


