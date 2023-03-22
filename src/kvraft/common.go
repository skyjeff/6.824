package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeOut     = "ErrTimeOut"
)

const (
	OpPut = 0
	OpAppend = 1
	OpGet = 2
)

type Err string

type Operation int

var OpName = map[Operation]string{
	OpGet:    "Get",
	OpPut:    "Put",
	OpAppend: "Append",
}

var OpNum = map[string]Operation{
	"Get":    OpGet,
	"Put":    OpPut,
	"Append": OpAppend,
}

type CommandArgs struct {
	Key string
	Value string
	Op Operation
	ClientID int64
	RequestID int
}

type CommandReply struct {
	Err Err
	Value string // Value is '' if op is not 'GET'
}

func getCommandArgs(key string, value string, op Operation, clientID int64, requestID int) CommandArgs {
	return CommandArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		ClientID:  clientID,
		RequestID: requestID,
	}
}


type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

type KVStateMachine interface {
	Get(Key string) (string, Err)
	Put(Key, Value string) Err
	Append(Key, Value string) Err
}

type NormalDB struct {
	DB map[string] string
}
func createNormalDB() *NormalDB {
	return &NormalDB{make(map[string] string)}
}

func (db *NormalDB) Get(Key string) (string, Err) {
	if value, ok := db.DB[Key]; ok {
		return value, OK
	}
	return "", ErrNoKey
}

func (db *NormalDB) Put(Key, Value string) Err {
	db.DB[Key] = Value
	return OK
}

func (db *NormalDB) Append(Key, Value string) Err {
	db.DB[Key] += Value
	return OK
}