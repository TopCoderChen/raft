package shardkv

// Lab 4B code is similar to Lab 3.

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardmaster to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import "../labrpc"
import "crypto/rand"
import "math/big"
import "../shardmaster"
import "time"

const RetryInterval = time.Duration(100 * time.Millisecond)

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardmaster.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

// client -> master (for config) => client -> replica-group(set of shardkv-servers)
type Clerk struct {
	sm       *shardmaster.Clerk
	config   shardmaster.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.

	// Your storage system must provide a linearizable interface to applications that use its client interface.
	clientId      int64
	lastRequestId int64
}

//
// the tester calls MakeClerk.
//
// masters[] is needed to call shardmaster.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
func MakeClerk(masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardmaster.MakeClerk(masters)
	ck.make_end = make_end
	ck.clientId = nrand()
	ck.lastRequestId = 0
	ck.config = ck.sm.Query(-1)
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
//
func (ck *Clerk) Get(key string) string {
	args := GetArgs{ConfigNum: ck.config.Num, Key: key}
	DPrintf("[GET] %s shard %d", args.Key, key2shard(key))
	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		args.ConfigNum = ck.config.Num
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ { // try each server for the shard.
				srv := ck.make_end(servers[si])
				var reply GetReply
				if srv.Call("ShardKV.Get", &args, &reply) {
					if reply.Err == OK || reply.Err == ErrNoKey {
						DPrintf("[GET SUCCESS %s from %d-%d] shard %d config %d reply %v", args.Key, gid, si, shard, ck.config.Num, reply)
						return reply.Value
					} else if reply.Err == ErrWrongGroup {
						break
					}
				}
			}
		}
		time.Sleep(RetryInterval)
		ck.config = ck.sm.Query(-1)
		// ck.config = ck.sm.Query(ck.config.Num + 1)
	}
	return ""
}

//
// shared by Put and Append.
// You will have to modify this function.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	requestId := time.Now().UnixNano() - ck.clientId
	args := PutAppendArgs{RequestId: requestId, ExpireRequestId: ck.lastRequestId, ConfigNum: ck.config.Num, Key: key, Value: value, Op: op}
	ck.lastRequestId = requestId
	if op == "Put" {
		DPrintf("[PUT] %s %s id %d shard %d", args.Key, args.Value, args.RequestId, key2shard(key))
	} else {
		DPrintf("[APPEND] %s %s id %d shard %d", args.Key, args.Value, args.RequestId, key2shard(key))
	}
	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		args.ConfigNum = ck.config.Num
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply PutAppendReply
				if srv.Call("ShardKV.PutAppend", &args, &reply) {
					if reply.Err == OK {
						// if op == "Put" {
						// 	DPrintf("[PUT SUCCESS] %s value %s id %d from %d-%d shard %d config %d reply %v", runtime.NumGoroutine(), args.Key, args.Value, args.RequestId, gid, si, shard, ck.config.Num, reply)
						// } else {
						// 	DPrintf("[APPEND SUCCESS] %s value %s id %d from %d-%d shard %d config %d reply %v", runtime.NumGoroutine(), args.Key, args.Value, args.RequestId, gid, si, shard, ck.config.Num, reply)
						// }
						return
					} else if reply.Err == ErrWrongGroup {
						break
					}
				}
			}
		}
		time.Sleep(RetryInterval)
		ck.config = ck.sm.Query(-1)
		// ck.config = ck.sm.Query(ck.config.Num + 1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
