package shardmaster

//
// Shardmaster clerk.
//

// This has similar structure as ../kvraft

import "../labrpc"
import "time"
import "crypto/rand"
import "math/big"
import "sync"

const RetryInterval = time.Duration(100 * time.Millisecond)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	clientId   int64 // duplicate client request detection for RPCs
	requestSeq int
	leaderId   int

	mu sync.Mutex // For atomic change to requestSeq field
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

// Helper, retry until RPC succeeds
func (ck *Clerk) Call(rpcname string, args interface{}, reply interface{}) bool {
	return ck.servers[ck.leaderId].Call(rpcname, args, reply)
}

func (ck *Clerk) nextRequestSeq() int {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	ck.requestSeq++
	return ck.requestSeq
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.clientId = nrand()
	ck.requestSeq = 0
	ck.leaderId = 0
	return ck
}

// Get the config for input number.
func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	args.Num = num
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardMaster.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				return reply.Config
			}
			// Try another server.
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		}
		time.Sleep(RetryInterval)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	// servers arg: mapping from GID to server-address
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers
	args.ClientId = ck.clientId
	args.RequestSeq = ck.nextRequestSeq()

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardMaster.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
			// Try another server.
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		}
		time.Sleep(RetryInterval)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids
	args.ClientId = ck.clientId
	args.RequestSeq = ck.nextRequestSeq()
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardMaster.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
			// Try another server.
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		}
		time.Sleep(RetryInterval)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	args.ClientId = ck.clientId
	args.RequestSeq = ck.nextRequestSeq()
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardMaster.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
			// Try another server.
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		}
		time.Sleep(RetryInterval)
	}
}
