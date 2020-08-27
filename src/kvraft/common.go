package kvraft

import "log"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	ClientId   int64
	RequestSeq int
	Key        string
	Value      string
	Op         string // "Put" or "Append"
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

func (arg *GetArgs) copy() GetArgs {
	return GetArgs{arg.Key}
}

func (arg *PutAppendArgs) copy() PutAppendArgs {
	return PutAppendArgs{arg.ClientId, arg.RequestSeq, arg.Key, arg.Value, arg.Op}
}
