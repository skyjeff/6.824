package raft


//
// Handler
//
func (rf *Raft) HandleAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply)  {
	// 1. 如果自己是candidate或follwer，然后term >= currentTerm就，转为follwer，重置eleciton timeout
	// 2. 要判断last的值匹不匹配，处理reply的match处理
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("*****[%d] follwer, term is [%d], commitIndex is [%d], lastApplied is [%d]", rf.me, rf.currentTerm, rf.commitIndex, rf.lastApplied)
	DPrintf("*****[%d] receive args is %+v", rf.me, args)

	reply.From = args.To
	reply.To = args.LeaderId
	reply.MatchIndex = 0
	reply.XTerm = -1
	reply.XIndex = -1
	reply.XLen = -1
	lastLogIndex, _ := rf.getLastLogIndexTerm()

	// if follwer term bigger than leader
	if rf.currentTerm > args.Term{
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm{
		rf.changeState(Follwer)
		rf.currentTerm = args.Term
	}
	reply.Term = args.Term
	rf.resetElectionTimer()

	// 这里需要添加，because can it tell the kv-server who is the leader in cluster
	if rf.voteFor != args.LeaderId{
		rf.voteFor = args.LeaderId
	}
	// follwer do not have prev or prev do not match, should use fast backup in this case
	if lastLogIndex < args.PrevLogIndex{
		reply.Success = false
		reply.XLen = len(rf.log)
		DPrintf("-----[%d] XTerm is %d, XIndex is %d, XLen is %d", rf.me, reply.XTerm, reply.XIndex, reply.XLen)
		return
	}
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		conflictTerm := rf.log[args.PrevLogIndex].Term
		reply.XTerm = conflictTerm
		// find the conflict term, and the leftmost position of conflict term at follwer log
		for i := args.PrevLogIndex ; i >= 0; i-- {
			if rf.log[i].Term != conflictTerm{
				reply.XIndex = i + 1
				break
			}
		}
		DPrintf("-----[%d] XTerm is %d, XIndex is %d, XLen is %d", rf.me, reply.XTerm, reply.XIndex, reply.XLen)
		return
	}

	// conflict at the same index(same index but diff term), delete the existing entry and all that follow it
	if rf.log[args.PrevLogIndex].Term == args.PrevLogTerm{
		reply.Success = true
		if len(args.Entries) > 0{
			if rf.outOfOrderAppendEntries(args){
				DPrintf("----has discard the old order Append Entries")
				DPrintf("----which is %+v", args)
				DPrintf("----[%d] current log is %+v", rf.me, rf.log)
				// 这里一定要返回true，因为当收到了符合条件的旧AE的时候，假如返回false，那么leader会认为这里不匹配，那么会next--，再重新发一个AE过来
				reply.Success = true
				return
			} else {
				DPrintf("[%d] follwer replicate log", rf.me)
				//= lastLogIndex + len(args.Entries)
				rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
				reply.MatchIndex = args.PrevLogIndex + len(args.Entries)
				rf.persist()
			}
		} else{
			reply.MatchIndex = args.PrevLogIndex
		}
	}
	if reply.Success {
		//if len(args.Entries) > 0{

			if rf.commitIndex < args.LeaderCommit {
				rf.commitIndex = minValue(args.LeaderCommit, reply.MatchIndex)
				rf.applierCond.Signal()
			}
		//}
	}
	//todo leader commit

	DPrintf("[%d] receive heartbeats from [%d] at term %d", reply.From, reply.To, reply.Term)
	DPrintf("-----[%d] current log is %+v", rf.me, rf.log)
	DPrintf("-----[%d] matchIndex is %d", rf.me, reply.MatchIndex)
	DPrintf("-----[%d] commitIndex is %+v", rf.me, rf.commitIndex)
	DPrintf("-----finish one replication")
}

func (rf *Raft) outOfOrderAppendEntries(args *AppendEntriesArgs) bool{
	argsLastLogIndex := args.PrevLogIndex + len(args.Entries)
	lastLogIndex, lastLogTerm := rf.getLastLogIndexTerm()
	// 假如prevIndex+需要添加的log长度比当前log长度短的话，说明这是一个合法，但是是很旧的AE,yin
	// 不能去用这个来修改日志，因为会把这个AE之后的日志都删除了
	if argsLastLogIndex < lastLogIndex && lastLogTerm == args.Term {
		return true
	}
	return false
}

func (rf *Raft) startHeartBeats(isHeartBeats bool){
	for to, _ := range rf.peers {
		if to == rf.me{
			continue
		}
		// 正常情况的时候，HB timeout了就立刻发送heartsBeats给peers
		// 而根据start的上层指令引起的复制的时候，就唤醒一个replicator线程去给他慢慢复制
		// 这里唤醒之后 也是会执行replicateOne
		if isHeartBeats{
			DPrintf("[%d] start a heartbeats to [%d]", rf.me, to)
			go rf.replicateOne(to)
		} else{
			DPrintf("[%d] start a log replicatoin to [%d]", rf.me, to)
			rf.replicatorCond[to].Signal()
			//stopTimer(rf.heartbeatTimer)
		}
	}
	//DPrintf("[%d] do heartbeats schedule at term %d", from, term)
}

func (rf *Raft) genAppendEntriesArgs(to int) AppendEntriesArgs{
	prevLogIndex := rf.nextIndex[to] - 1
	lastLogIndex, _ := rf.getLastLogIndexTerm()
	DPrintf("[%d] follwer is preLogIndex = %d, and [%d] leader lastLogIndex = %d", to, prevLogIndex, rf.me, lastLogIndex)
	args := AppendEntriesArgs{
		LeaderId:     rf.me,
		To:           to,
		Term:         rf.currentTerm,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  rf.log[prevLogIndex].Term,
		LeaderCommit: rf.commitIndex,
	}
	if lastLogIndex < rf.nextIndex[to]{
		return args
	}

	// if peer match the log at matchIndex, then send all log[matchIndex[to]:] to peer
	if rf.matchIndex[to] == prevLogIndex {
		args.Entries = append([]LogEntry{}, rf.log[prevLogIndex+1:]...)
	}

	//todo use snapshot
	return args
}

func (rf *Raft) handleAppendEntriesReply(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply)  {
	// if is not leader or leader term change when handle the reply
	DPrintf("[%d] leader handle [%d] reply", rf.me, peer)

	// 这里需要比下面一个判断提前，因为leader term小的话是需要转变为follwer，而不是直接return
	if args.Term < reply.Term {
		DPrintf("follwer term is bigger, leader [%d] change to follwer at term [%d]", rf.me, rf.currentTerm)
		rf.changeState(Follwer)
		rf.currentTerm = reply.Term
		rf.resetElectionTimer()
		rf.persist()
		return
	}

	if rf.state != Leader || rf.currentTerm != args.Term{
		return
	}

	// 1. 第一次尝试减少next 匹配到match
	// 2. 匹配到match，发送match为起点的所有log，
	// 3. 心跳成功
	lastLogIndex, _ := rf.getLastLogIndexTerm()
	//rf.matchIndex[peer] = reply.MatchIndex
	if !reply.Success{
		if reply.XTerm != -1 && reply.XIndex != -1{
			flag := false
			conflictTerm := reply.XTerm
			for i := len(rf.log) - 1; i >= 0; i-- {
				if rf.log[i].Term == conflictTerm{
					flag = true
					break
				}
			}
			// if leader has conflict term or not
			if flag{
				rf.nextIndex[peer] = reply.XIndex + 1
			} else{
				rf.nextIndex[peer] = reply.XIndex
			}
		} else if reply.XLen != -1{
			rf.nextIndex[peer] = reply.XLen
			//rf.matchIndex[peer] = reply.XLen - 1
		} else if reply.MatchIndex < lastLogIndex{
			// can fast find the conflict index
			DPrintf("log match fail, decrease the nextIndex")
			rf.nextIndex[peer]--
			//stopTimer(rf.heartbeatTimer)
		}
		rf.replicatorCond[peer].Signal()
		return
	}
	if reply.Success{
		//DPrintf("[%d] reply is %+v", peer, reply)
		//newMatchIndex := args.PrevLogIndex + len(args.Entries)
		//if newMatchIndex > rf.matchIndex[peer] {
		//	rf.matchIndex[peer] = newMatchIndex
		//}
		//rf.nextIndex[peer] = rf.matchIndex[peer] + 1
		//rf.updateCommitIndex()

		DPrintf("[%d] reply is %+v", peer, reply)
		rf.matchIndex[peer] = reply.MatchIndex
		rf.nextIndex[peer] = reply.MatchIndex + 1
		if len(args.Entries) > 0{
			rf.updateCommitIndex()
		}
	}
	DPrintf("handleAppendEntriesReply Function")
	DPrintf("leader [%d] log is %+v", rf.me, rf.log)
	DPrintf("leader [%d] nextIndex is %+v", rf.me, rf.nextIndex)
	DPrintf("leader [%d] matchIndex is %+v", rf.me, rf.matchIndex)
}

func (rf *Raft) updateCommitIndex()  {
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	DPrintf("try to update commit index, matchIndex is %+v", rf.matchIndex)

	lastLogIndex, _ := rf.getLastLogIndexTerm()
	for N := lastLogIndex; N > rf.commitIndex; N-- {
		majority := 1
		if rf.log[N].Term == rf.currentTerm{
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me && rf.matchIndex[i] >= N{
					majority++
				}
			}
		}
		if majority > len(rf.peers) / 2 {
			rf.commitIndex = N
			rf.applierCond.Signal()
			break
		}
	}
	//for i := 0 ; i < len(rf.peers) ; i++ {
	//	if i == rf.me{
	//		continue
	//	}
	//
	//	if rf.matchIndex[i] > rf.commitIndex{
	//r		majority++
	//	}
	//	if majority > len(rf.peers) / 2 {
	//		rf.commitIndex = rf.matchIndex[i]
	//		rf.applierCond.Signal()
	//		break
	//	}
	//}
	DPrintf("leader [%d] commitIndex is %d", rf.me, rf.commitIndex)
}

// only send RPC for one round
func (rf *Raft) replicateOne(peer int){
	rf.mu.Lock()
	// very important, prevent the old RPC sent by old leader
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}

	args := rf.genAppendEntriesArgs(peer)
	reply := AppendEntriesReply{}
	rf.mu.Unlock()

	if ok := rf.peers[peer].Call("Raft.HandleAppendEntries", &args, &reply); ok {
		rf.mu.Lock()
		rf.handleAppendEntriesReply(peer, &args, &reply)
		rf.mu.Unlock()
	}
}

// judge need to replicate or not
func (rf *Raft) needReplicate(peer int) bool{
	rf.mu.Lock()
	defer rf.mu.Unlock()
	lastLogIndex, _ := rf.getLastLogIndexTerm()
	// 重启的时候同步给follwer日志的时候，可能next是大于last的，但是会通过心跳来减少next
	// reboost can use heartbeats to checck
	return rf.state == Leader && lastLogIndex >= rf.nextIndex[peer]
}

// each peer have potential replicator to replicate log
func (rf *Raft) replicator(peer int){
	rf.replicatorCond[peer].L.Lock()
	defer rf.replicatorCond[peer].L.Unlock()

	for !rf.killed(){
		for !rf.needReplicate(peer){
			//resetTimer(rf.heartbeatTimer, HeartBeatsInterval)
			rf.replicatorCond[peer].Wait()
		}
		DPrintf("[%d] need to replication, start a log replication", peer)
		rf.replicateOne(peer)
	}
}


