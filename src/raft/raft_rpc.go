package raft


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	From int
	To int
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

//
// example callRequestVode RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	From int
	To int
	Term int
	Granted bool
}

type AppendEntriesArgs struct {
	Term int
	LeaderId int
	To int
	PrevLogIndex int
	PrevLogTerm int
	Entries []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	From int
	To int
	Term int
	Success bool
	MatchIndex int
	XTerm int
	XIndex int
	XLen int
}