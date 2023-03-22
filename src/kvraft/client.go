package kvraft

import "../labrpc"
import "crypto/rand"
import "math/big"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clientID int64
	requestID int
	leaderID int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers

	ck.clientID = nrand()
	ck.requestID = 0
	ck.leaderID = 0

	return ck
}

func (ck *Clerk) Command(Key string, Value string, opType Operation) string{
	commandArgs := getCommandArgs(Key, Value, opType, ck.clientID, ck.requestID)
	for {
		reply := CommandReply{}
		if !ck.servers[ck.leaderID].Call("KVServer.Command", &commandArgs, &reply) || reply.Err == ErrWrongLeader || reply.Err == ErrTimeOut {
			ck.leaderID = (ck.leaderID + 1) % len(ck.servers)
			DPrintf("resend to [KVServer %d], because %s", ck.leaderID, reply.Err)
			continue
		}
		DPrintf("finish command, key %s, value %s, type is %s", Key, Value, OpName[opType])
		ck.requestID += 1
		return reply.Value
	}
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	return ck.Command(key, "", OpGet)
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op Operation) {
	ck.Command(key, value, op)
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, OpPut)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, OpAppend)
}
