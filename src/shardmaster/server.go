package shardmaster

// This has similar structure as ../kvraft
import "../raft"
import "../labrpc"
import "sync"

import "../labgob"
import "time"

const RaftTimeout = time.Duration(3 * time.Second)

// Without this init(), Lab 4A won't pass, labgob is like Go's gob.
func init() {
	labgob.Register(JoinArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})
	labgob.Register(QueryArgs{})
	// log.SetFlags(log.LstdFlags | log.Lmicroseconds)
}

// This is the master server for storing configs, contacted by kvraft servers.
type ShardMaster struct {
	mu       sync.Mutex
	me       int
	rf       *raft.Raft
	applyCh  chan raft.ApplyMsg
	shutdown chan struct{}
	// Your data here.

	configs []Config // indexed by config num
	// Cache maps client-id to request-seq
	cache         map[int64]int           // cache processed request, detect duplicate request
	notifyChanMap map[int]chan notifyArgs // map log-index to channel
}

type notifyArgs struct {
	Term int
	Args interface{}
}

func (sm *ShardMaster) getConfig(i int) Config {
	var srcConfig Config
	if i < 0 || i >= len(sm.configs) {
		srcConfig = sm.configs[len(sm.configs)-1]
	} else {
		srcConfig = sm.configs[i]
	}
	// Deep copy
	dstConfig := Config{Num: srcConfig.Num, Shards: srcConfig.Shards, Groups: make(map[int][]string)}
	for gid, servers := range srcConfig.Groups {
		dstConfig.Groups[gid] = append([]string{}, servers...)
	}
	return dstConfig
}

// This will ultimately notify the RPC and send reply back.
func (sm *ShardMaster) notifyIfPresent(index int, reply notifyArgs) {
	if ch, ok := sm.notifyChanMap[index]; ok {
		ch <- reply
		delete(sm.notifyChanMap, index)
	}
}

// Send the command to Raft Start().
// Flow: Join/Leave -> Raft -> applyCh -> apply() -> notifyIfPresent -> notifyChanMap
func (sm *ShardMaster) start(arg interface{}) (bool, interface{}) {
	index, term, isleader := sm.rf.Start(arg)
	if !isleader {
		return WrongLeader, struct{}{}
	}
	sm.mu.Lock()
	notifyCh := make(chan notifyArgs, 1)
	sm.notifyChanMap[index] = notifyCh
	sm.mu.Unlock()
	select {
	case <-time.After(RaftTimeout): // timed out from Raft
		sm.mu.Lock()
		delete(sm.notifyChanMap, index)
		sm.mu.Unlock()
		return WrongLeader, struct{}{}
	case result := <-notifyCh:
		if result.Term != term {
			return WrongLeader, struct{}{}
		} else {
			return Ok, result.Args
		}
	}
	return Ok, struct{}{}
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	reply.WrongLeader, _ = sm.start(args.copy())
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	reply.WrongLeader, _ = sm.start(args.copy())
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	reply.WrongLeader, _ = sm.start(args.copy())
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	if _, isLeader := sm.rf.GetState(); !isLeader {
		reply.WrongLeader = WrongLeader
		return
	}
	sm.mu.Lock()
	if args.Num > 0 && args.Num < len(sm.configs) {
		reply.WrongLeader, reply.Config = Ok, sm.getConfig(args.Num)
		sm.mu.Unlock()
		return
	} else { // args.Num <= 0
		sm.mu.Unlock()
		err, config := sm.start(args.copy())
		reply.WrongLeader = err
		if !reply.WrongLeader {
			reply.Config = config.(Config)
		}
	}
}

func (sm *ShardMaster) appendNewConfig(newConfig Config) {
	newConfig.Num = len(sm.configs)
	sm.configs = append(sm.configs, newConfig)
}

func (sm *ShardMaster) apply(msg raft.ApplyMsg) {
	reply := notifyArgs{Term: msg.CommandTerm, Args: ""}
	if arg, ok := msg.Command.(JoinArgs); ok {
		sm.applyJoin(arg)
	} else if arg, ok := msg.Command.(LeaveArgs); ok {
		sm.applyLeave(arg)
	} else if arg, ok := msg.Command.(MoveArgs); ok {
		sm.applyMove(arg)
	} else if arg, ok := msg.Command.(QueryArgs); ok {
		reply.Args = sm.getConfig(arg.Num)
	}
	sm.notifyIfPresent(msg.CommandIndex, reply)
}

// Interact with underlying Raft.
func (sm *ShardMaster) run() {
	go sm.rf.Replay(1)

	for {
		select {
		case msg := <-sm.applyCh:
			sm.mu.Lock()
			if msg.CommandValid {
				// Raft successfully applied the command, the master now applies it.
				sm.apply(msg)
			} else if cmd, ok := msg.Command.(string); ok && cmd == "NewLeader" {
				sm.rf.Start("")
			}
			sm.mu.Unlock()
		case <-sm.shutdown:
			return
		}
	}
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
	close(sm.shutdown)
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.cache = make(map[int64]int)
	sm.notifyChanMap = make(map[int]chan notifyArgs)
	sm.shutdown = make(chan struct{})

	go sm.run()

	return sm
}
