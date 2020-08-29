package shardmaster

import "log"

//
// Master shard server: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10
const (
	OK          = "OK"
	WrongLeader = true // used in RPC reply
	Ok          = false
)
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Err string

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

func (config Config) Copy() Config {
	newConfig := Config{Num: config.Num, Shards: config.Shards, Groups: make(map[int][]string)}
	for gid, servers := range config.Groups {
		newConfig.Groups[gid] = append([]string{}, servers...)
	}
	return newConfig
}

type JoinArgs struct {
	Servers    map[int][]string // new GID -> servers mappings
	ClientId   int64            // de-duplicate
	RequestSeq int              // de-duplicate
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	GIDs       []int
	ClientId   int64 // de-duplicate
	RequestSeq int   // de-duplicate
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	Shard      int
	GID        int
	ClientId   int64 // de-duplicate
	RequestSeq int   // de-duplicate
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	Num int // desired config number
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}

func (arg *JoinArgs) copy() JoinArgs {
	result := JoinArgs{ClientId: arg.ClientId, RequestSeq: arg.RequestSeq, Servers: make(map[int][]string)}
	for gid, server := range arg.Servers {
		result.Servers[gid] = append([]string{}, server...)
	}
	return result
}

func (arg *LeaveArgs) copy() LeaveArgs {
	return LeaveArgs{ClientId: arg.ClientId, RequestSeq: arg.RequestSeq, GIDs: append([]int{}, arg.GIDs...)}
}

func (arg *MoveArgs) copy() MoveArgs {
	return MoveArgs{ClientId: arg.ClientId, RequestSeq: arg.RequestSeq, Shard: arg.Shard, GID: arg.GID}
}

func (arg *QueryArgs) copy() QueryArgs {
	return QueryArgs{arg.Num}
}
