package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout = "ErrTimeout"
	ErrOldRequest = "ErrOldRequest"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	ClientID int64
	Sequence int

	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

const (
	APPEND string = "Append"
	GET string = "Get"
	PUT string = "Put"
)

type PutAppendReply struct {
	Err Err
	LeaderId int
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
	LeaderId int
}
