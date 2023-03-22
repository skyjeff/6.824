package raft

import (
	_"fmt"
	_"math/rand"
	"sync"
	_"time"
)


//
// utils
//

// must stop the old timer and empty the chanel before reset
func (rf *Raft) resetElectionTimer() {
	if !rf.electionTimer.Stop() && len(rf.electionTimer.C) > 0{
		<- rf.electionTimer.C
	}
	rf.electionTimer.Reset(getRandomElectTimeout())
}

//
// RPC handler.
//
func (rf *Raft) HandleRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Granted = false
	reply.From = args.To
	reply.To = args.From
	reply.Term = rf.currentTerm

	DPrintf("[%d] receive requestvote by [%d]",rf.me, args.CandidateId)
	DPrintf("[%d] current log is %+v", rf.me, rf.log)

	lastLogIndex, lastLogTerm := rf.getLastLogIndexTerm()

	// candidate term must bigger
	if rf.currentTerm >= args.Term{
		DPrintf("[%d] follwer term [%d] is more bigger than candidate [%d] term [%d], term fail",
			rf.me, rf.currentTerm, args.CandidateId, args.Term)
		return
	}
	if rf.currentTerm < args.Term{
		reply.Term = args.Term
		rf.currentTerm = args.Term
		if rf.state == Leader{
			rf.changeState(Follwer)
			rf.resetElectionTimer()
		}
		rf.persist()
		DPrintf("----election, [%d] change term to %d", rf.me, rf.currentTerm)
	}

	// if votedFor is null or candidateId
	//if rf.voteFor != -1 && rf.voteFor != args.CandidateId && rf.state == Follwer {
	//	DPrintf("has voted")
	//	return
	//}
	// had voted to candidate
	if rf.voteFor == args.CandidateId{
		reply.Granted = true
		return
	}
	// at least up-to-date as follwer's log (5.4.1)
	if args.LastLogTerm < lastLogTerm ||
		(args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIndex){
		DPrintf("log too old")
		return
	}
	rf.changeState(Follwer)
	// 只有在赞同投票才会重置election timeout
	rf.resetElectionTimer()
	rf.voteFor = args.CandidateId
	rf.persist()
	reply.Granted = true
}



//cause Timer only execute ont time, so must reset timer after time out
// 这里修改为不使用NewCond，因为假如某次选举中，RPC一直丢失，并没有重新发送，那么此时的startElection会一直wait。
// 直到这次timeout，重新生成一个startElection的goroutine，但是旧的startElection会一直wait
func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.changeState(Candidate)
	vote := 1
	finished := 0
	//done := false
	term := rf.currentTerm
	me := rf.me
	lastLogIndex, LastLogTerm := rf.getLastLogIndexTerm()
	DPrintf("[%d] attempting an election at term %d", rf.me, rf.currentTerm)
	DPrintf("[%d] candidate log is %+v", rf.me, rf.log)
	rf.mu.Unlock()
	//等待vote数量到达一半，或者完全收到
	cond := sync.NewCond(&rf.mu)
	for server, _ := range rf.peers {
		if server == rf.me{
			continue
		}
		go func(server int) {
			reply := rf.sendRequestVote(me, server, term, lastLogIndex, LastLogTerm)
			rf.mu.Lock()
			finished += 1
			if !reply.Granted {
				if reply.Term > rf.currentTerm{
					rf.changeState(Follwer)
					rf.currentTerm = reply.Term
					rf.persist()
					// 需要，因为我自身成为leader的时候，我已经关闭了timer，所以需要重启一下
					//rf.resetElectionTimer() // only granted false by Term will reset timer, not the log up-to-date
				}
				DPrintf("[%d] got false vote from [%d] at term %d", me, server, term)
				// reply.term == currentTerm && follwer more up-to-date than candidate
				rf.mu.Unlock()
				return
			}
			if reply.Term == rf.currentTerm{
				vote++
				DPrintf("[%d] got a vote from [%d] at term %d", me, server, term)
			}

			cond.Broadcast()
			rf.mu.Unlock()
		}(server)
	}

	rf.mu.Lock()
	// finish用来判断投票都给出回答了
	for vote <= len(rf.peers)/2 && finished < len(rf.peers){
		cond.Wait()
		DPrintf("vote is %d, finished is %d", vote, finished)
	}
	//if finished == len(rf.peers) && vote <= len(rf.peers)/2{
	//	DPrintf("[%d] fail the election (currentTerm=%d)!", rf.me, rf.currentTerm)
	//}
	// be a leader
	if vote > len(rf.peers)/2 {
		DPrintf("[%d] got enough vote, we are now the leader (currentTerm=%d)!", rf.me, rf.currentTerm)
		rf.changeState(Leader)
		resetTimer(rf.heartbeatTimer, HeartBeatsInterval)
		//rf.heartbeatTimer.Reset(HeartBeatsInterval)
		rf.mu.Unlock()
		// 开启心跳timer，来定时发送
		//go rf.heartBeatsSchedule()
		return
	}
	DPrintf("%d finish the electiion, but fail to be leader", rf.me)
	rf.mu.Unlock()

}

// 需要复制term，要不然有可能再call这个函数的过程中，term已经被改变了，但是我们的目的是这个发送的原来的term的RPC
// 并且，在这个实际调用RPC的地方最好不要去获取锁，
func (rf *Raft) sendRequestVote(from int, to int, term int, lastLogIndex int, LastLogTerm int) RequestVoteReply {
	args := RequestVoteArgs{
		From : from,
		To : to,
		Term : term,
		LastLogIndex : lastLogIndex,
		LastLogTerm : LastLogTerm,
		CandidateId: rf.me,
	}
	reply := RequestVoteReply{}
	DPrintf("[%d] send RequestVote to [%d] at term %d", from, to, term)

	if ok := rf.peers[to].Call("Raft.HandleRequestVote", &args, &reply); !ok{
		DPrintf("error, [%d]send RequestVote to [%d] at term %d!", args.From, args.To, args.Term)
	}

	return reply
}
