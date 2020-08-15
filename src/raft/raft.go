package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//
import (
	// "bytes"
	// crand "crypto/rand"
	"bytes"
	"log"
	"math/rand"

	"../labgob"

	"../labrpc"
	// "log"
	// "math/big"
	// "math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// import "bytes"
// import "../labgob"

const electionTimeout = time.Duration(500 * time.Millisecond)
const AppendEntriesInterval = time.Duration(100 * time.Millisecond)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//

func init() {
	labgob.Register(LogEntry{})
	// max := big.NewInt(int64(1) << 62)
	// bigx, _ := crand.Int(crand.Reader, max)
	// seed := bigx.Int64()
	// rand.Seed(seed)
	// log.SetFlags(log.LstdFlags | log.Lmicroseconds)
}

func newRandDuration(min time.Duration) time.Duration {
	extra := time.Duration(rand.Int63()) % min
	return time.Duration(extra + min)
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// server states
	leaderId int
	state    serverState

	// Persistent state on all servers.
	currentTerm int
	votedFor    int
	log         []LogEntry

	// Volatile state on all servers.
	commitIndex int // index of highest log entry known to be committed, initialized to 0
	lastApplied int // index of highest log entry applied to state machine, initialized to 0  (for snapshot ??)
	logIndex    int // index of log to be stored next

	lastIncludedIndex int // snapshot

	// Volatile state on leaders.
	// nextIndex: 下一个 还未send 的 log 的 index, for each peer raft server.
	nextIndex  []int
	matchIndex []int

	// More states
	applyCh       chan ApplyMsg
	shutdown      chan struct{} // for close()
	notifyApplyCh chan struct{} // for send msg to applyCh to the client
	electionTimer *time.Timer   // for leader election
}

func (rf *Raft) resetElectionTimer(duration time.Duration) {
	rf.electionTimer.Stop()
	rf.electionTimer.Reset(duration)
}

// After a leader comes to power, it calls this function to initialize nextIndex and matchIndex
func (rf *Raft) initIndex() {
	peersNum := len(rf.peers)
	rf.nextIndex, rf.matchIndex = make([]int, peersNum), make([]int, peersNum)
	for i := 0; i < peersNum; i++ {
		rf.nextIndex[i] = rf.logIndex
		rf.matchIndex[i] = 0
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

func (rf *Raft) getPersistState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	// e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.logIndex)
	e.Encode(rf.commitIndex)
	e.Encode(rf.lastApplied)
	e.Encode(rf.log)
	return w.Bytes()
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	data := rf.getPersistState()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	currentTerm, votedFor, logIndex, commitIndex, lastApplied := 0, 0, 0, 0, 0
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		// d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&logIndex) != nil ||
		d.Decode(&commitIndex) != nil ||
		d.Decode(&lastApplied) != nil ||
		d.Decode(&rf.log) != nil {
		log.Fatal("!!! \n\n Error unmarshaling raft state \n\n")
	}
	rf.currentTerm, rf.votedFor, rf.logIndex, rf.commitIndex, rf.lastApplied = currentTerm, votedFor, logIndex, commitIndex, lastApplied
}

// because snapshot will replace committed log entries in log
// thus the length of rf.log is different from(less than or equal) rf.logIndex
func (rf *Raft) getOffsetIndex(i int) int {
	return i - rf.lastIncludedIndex
}

func (rf *Raft) getEntry(i int) LogEntry {
	offsetIndex := rf.getOffsetIndex(i)
	return rf.log[offsetIndex]
}

func (rf *Raft) getRangeEntry(fromInclusive, toExclusive int) []LogEntry {
	from := rf.getOffsetIndex(fromInclusive)
	to := rf.getOffsetIndex(toExclusive)
	return append([]LogEntry{}, rf.log[from:to]...)
}

// Check raft can commit log entry at index (if majority agrees on the match)
func (rf *Raft) canCommit(index int) bool {
	if index < rf.logIndex && rf.commitIndex < index && rf.getEntry(index).LogTerm == rf.currentTerm {
		majority, count := len(rf.peers)/2+1, 0
		for j := 0; j < len(rf.peers); j++ {
			if rf.matchIndex[j] >= index {
				count++
			}
		}
		return count >= majority
	}
	return false
}

func (rf *Raft) stepDown(term int) {
	rf.currentTerm = term
	rf.state = Follower
	rf.votedFor, rf.leaderId = -1, -1
	rf.persist()
	rf.resetElectionTimer(newRandDuration(electionTimeout))
}

func (rf *Raft) requestVoteAndGetReply(server int, args RequestVoteArgs, replyCh chan<- RequestVoteReply) {
	var reply RequestVoteReply
	if !rf.peers[server].Call("Raft.RequestVote", &args, &reply) {
		reply.Err, reply.Server = ErrRPCFail, server
	}
	replyCh <- reply
}

// Send AppendEntries RPC call to the follower and handle reply.
// sendLogEntry(follower int)
func (rf *Raft) sendLogEntry(follower int) {
	DPrintf("[%d] is send AppendEntry to [%d]", rf.me, follower)
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	// Prepare the AppendEntries request args
	prevLogIndex := rf.nextIndex[follower] - 1
	prevLogTerm := rf.getEntry(prevLogIndex).LogTerm
	args := AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me, PrevLogIndex: prevLogIndex, PrevLogTerm: prevLogTerm, CommitIndex: rf.commitIndex, Len: 0}
	if rf.nextIndex[follower] < rf.logIndex {
		// Fill the log data gap for this follower
		entries := rf.getRangeEntry(rf.nextIndex[follower], rf.logIndex)
		args.Entries = entries
		args.Len = len(entries)
	}
	rf.mu.Unlock()

	// Send the RPC AppendEntries
	var reply AppendEntriesReply
	if rf.peers[follower].Call("Raft.AppendEntries", &args, &reply) {
		DPrintf("[%d] get AppendEntries reply from [%d]", rf.me, follower)
		// Lock after the RPC finished.
		rf.mu.Lock()
		defer rf.mu.Unlock()

		// Reply is not successful, early return.
		if !reply.Success {
			if reply.Term > rf.currentTerm { // the leader is obsolete
				rf.stepDown(reply.Term)
			} else {
				// follower is inconsistent with leader
				// force follower's data to be overwritten by resetting index.
				rf.nextIndex[follower] = Max(1, Min(reply.ConflictIndex, rf.logIndex))
				// TODO
			}

			return
		}

		// Reply is successful

		prevLogIndex, logEntriesLen := args.PrevLogIndex, args.Len
		if prevLogIndex+logEntriesLen+1 > rf.nextIndex[follower] {
			// Update the our local record (for index) for this follower.
			rf.nextIndex[follower] = prevLogIndex + logEntriesLen + 1
			rf.matchIndex[follower] = prevLogIndex + logEntriesLen
		}

		// Update Commit Index if logs have been replicated to majority of followers.
		toCommitIndex := prevLogIndex + logEntriesLen
		if rf.canCommit(toCommitIndex) {
			rf.commitIndex = toCommitIndex
			rf.persist()
			rf.notifyApplyCh <- struct{}{}
		}

	}
}

func (rf *Raft) replicate() {
	DPrintf("[%d] is replicate()", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for follower := 0; follower < len(rf.peers); follower++ {
		if follower != rf.me {
			go rf.sendLogEntry(follower)
		}
	}
}

// Data Sync from leader to followers, keep trying until die or succeed.
// tick()
func (rf *Raft) tick() {
	DPrintf("[%d] is tick()", rf.me)
	timer := time.NewTimer(AppendEntriesInterval)
	// Notice the while loop here, keep trying until die or succeed !!!
	for {
		select {
		case <-rf.shutdown:
			return
		case <-timer.C:
			if _, isLeader := rf.GetState(); !isLeader {
				DPrintf("[%d] lost leadership", rf.me)
				return
			}
			// DPrintf("[%d] is replicate()", rf.me)
			go rf.replicate()
			timer.Reset(AppendEntriesInterval)
		}
	}
}

func (rf *Raft) campaign() {
	rf.mu.Lock()
	if rf.state == Leader {
		rf.mu.Unlock()
		return
	}
	// Convert to candidate if necessary for leader election
	// • Increment currentTerm
	// • Vote for self
	// • Reset election timer
	// • Send RequestVote RPCs to all other servers
	rf.state = Candidate
	rf.leaderId = -1
	rf.currentTerm++
	rf.votedFor = rf.me

	term := rf.currentTerm
	candidateId := rf.me
	lastLogIndex := rf.logIndex - 1
	lastLogTerm := rf.getEntry(lastLogIndex).LogTerm

	rf.persist()

	args := RequestVoteArgs{Term: term, CandidateId: candidateId, LastLogIndex: lastLogIndex, LastLogTerm: lastLogTerm}

	// reset election Timer
	newElectionTimeout := newRandDuration(electionTimeout)
	rf.resetElectionTimer(newElectionTimeout)
	currentElectionTimer := time.After(newElectionTimeout)
	rf.mu.Unlock()

	// Send actual vote request and get reply
	voteReplyCh := make(chan RequestVoteReply)
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(server int) {
				DPrintf("[%d] requestVoteAndGetReply from [%d]", rf.me, server)
				rf.requestVoteAndGetReply(server, args, voteReplyCh)
			}(i)
		}
	}

	voteCount, threshold := 0, len(rf.peers)/2
	// until we reach majority of votes
	for voteCount < threshold {
		select {
		case <-currentElectionTimer: // this election times out, terminate.
			DPrintf("[%d] currentElectionTimer ", rf.me)
			return
		case <-rf.shutdown:
			return
		case reply := <-voteReplyCh:
			if reply.Err != OK {
				// RPC failed, retry
				DPrintf("[%d] RPC failed, retry send to [%d]", rf.me, reply.Server)
				go rf.requestVoteAndGetReply(reply.Server, args, voteReplyCh)
			} else if reply.VoteGranted {
				DPrintf("[%d] voteCount += 1 from [%d]", rf.me, reply.Server)
				voteCount += 1
			} else {
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					DPrintf("[%d] stepDown from [%d]", rf.me, reply.Server)
					rf.stepDown(reply.Term)
				}
				rf.mu.Unlock()
			}
		}
	}

	rf.mu.Lock()
	if rf.state == Candidate { // check state again
		DPrintf("[%d] got majority of vote, becoming new leader ", rf.me)
		rf.state = Leader
		rf.initIndex()
		go rf.tick()
		go rf.applyChNewLeader()
	}
	rf.mu.Unlock()
}

func (rf *Raft) applyChNewLeader() {
	rf.applyCh <- ApplyMsg{CommandValid: false, CommandIndex: -1, Command: "NewLeader"}
}

// Wait for msg from notifyApplyCh and notify the rf.applyCh
func (rf *Raft) apply() {
	for {
		select {
		case <-rf.shutdown:
			return
		case <-rf.notifyApplyCh:
			rf.mu.Lock()
			var commandValid bool
			var entries []LogEntry
			if rf.lastApplied < rf.logIndex && rf.lastApplied < rf.commitIndex {
				commandValid = true
				entries = rf.getRangeEntry(rf.lastApplied+1, rf.commitIndex+1)
				rf.lastApplied = rf.commitIndex
			}

			rf.persist()

			rf.mu.Unlock()

			// Notify rf.applyCh
			for _, entry := range entries {
				rf.applyCh <- ApplyMsg{CommandValid: commandValid, CommandIndex: entry.LogIndex, CommandTerm: entry.LogTerm, Command: entry.Command}
			}
		}
	}
}

// If timeout, start to compete for leader.
func (rf *Raft) electionMonitor() {
	for {
		select {
		case <-rf.electionTimer.C:
			DPrintf("[%d] campaign()", rf.me)
			rf.campaign()

		case <-rf.shutdown:
			return
		}
	}
}

// Return: next index, term, isleader
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("[%d] Start() in term %d", rf.me, rf.currentTerm)

	if rf.state != Leader {
		return -1, -1, false
	}
	index := rf.logIndex
	newEntry := LogEntry{LogIndex: index, LogTerm: rf.currentTerm, Command: command}
	rf.log = append(rf.log, newEntry)

	rf.matchIndex[rf.me] = index

	// Advance the index !!!
	rf.logIndex++

	rf.persist()

	go rf.replicate()

	return index, rf.currentTerm, true
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	close(rf.shutdown)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.leaderId = -1
	// start off as a follower
	rf.state = Follower

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = []LogEntry{{LogTerm: 0, LogIndex: 0, Command: nil}}

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.logIndex = 1

	rf.lastIncludedIndex = 0

	rf.applyCh = applyCh
	rf.shutdown = make(chan struct{})
	rf.notifyApplyCh = make(chan struct{})
	rf.electionTimer = time.NewTimer(newRandDuration(electionTimeout))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// kick off apply msg & leader election
	go rf.apply()
	go rf.electionMonitor()

	return rf
}
