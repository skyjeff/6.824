package raft

import "time"

type NodeState string

const (
	MaxTaskRunTime   = time.Second * 5
	ElectionInterval = time.Millisecond * 5
	HeartBeatsInterval = time.Millisecond * 100
)

const (
	HeartBeatsFlag = 0
	AppendEntriesFlag = 1
)


const(
	Follwer   NodeState = "follwer"
	Candidate NodeState = "candidate"
	Leader    NodeState = "leader"
)


