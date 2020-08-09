package raft

// Put related RPC handlers in one place.

// Reset timer inside each handler.

// Servers retry RPCs if they do not receive a response in a timely manner.

type AppendEntriesArgs struct {
	Term,
	LeaderId,
	PrevLogIndex,
	PrevLogTerm,
	CommitIndex int
	Len     int        // number of logs sends to follower
	Entries []LogEntry // logs that send to follower
}

type AppendEntriesReply struct {
	// true if follower contained entry matching prevLogIndex and prevLogTerm
	Success bool
	// max(request.term, rf Local Term)
	Term int
	// in case of conflicting, follower include the first index it store for conflict term
	// This is for keeping the log consistency between follower and leader
	ConflictIndex int
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
	Err         Err // Err is string defined by "type Err string"
	Server      int // the server who sent this reply msg
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Err = OK
	reply.Server = rf.me

	// invalid/outdated candidate
	if rf.currentTerm > args.Term || (rf.currentTerm == args.Term && rf.votedFor != -1) {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
	// already voted for this candidateId
	if rf.currentTerm == args.Term && rf.votedFor == args.CandidateId {
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		if rf.state != Follower {
			rf.state = Follower
			rf.resetElectionTimer(newRandDuration(electionTimeout))
		}
	}

	// invalidate old leaderId because other wants to be new leader
	rf.leaderId = -1

	reply.Term = args.Term

	lastLogIndex := rf.logIndex - 1
	lastLogTerm := rf.getEntry(lastLogIndex).LogTerm
	// Election restriction: prevent unless its log contains all committed entries
	if lastLogTerm > args.LastLogTerm || (lastLogTerm == args.LastLogTerm && lastLogIndex > args.LastLogIndex) {
		// Don't grant vote
		reply.VoteGranted = false
		return
	}
	reply.VoteGranted = true
	rf.votedFor = args.CandidateId

	rf.resetElectionTimer(newRandDuration(electionTimeout))
}

// AppendEntries RPC handler
// Consistency check performed by AppendEntries:
// When sending an AppendEntries RPC, the leader includes the index
// and term of the entry in its log that immediately precedes
// the new entries. If the follower does not find an entry in
// its log with the same index and term, then it refuses the
// new entries.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm > args.Term {
		// RPC call comes from an illegitimate leader
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}
	reply.Term = args.Term

	// Transition to follower
	rf.leaderId = args.LeaderId
	rf.resetElectionTimer(newRandDuration(electionTimeout)) // reset electionTimer
	if args.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = args.Term, -1
	}
	rf.state = Follower

	logIndex := rf.logIndex
	prevLogIndex := args.PrevLogIndex
	// follower don't agree with leader on last log entry (index or term mismatch)
	if logIndex <= prevLogIndex || rf.getEntry(prevLogIndex).LogTerm != args.PrevLogTerm {
		conflictIndex := Min(rf.logIndex-1, prevLogIndex)
		conflictTerm := rf.getEntry(conflictIndex).LogTerm
		// find the smallest conflictIndex
		for conflictIndex > rf.commitIndex && rf.getEntry(conflictIndex - 1).LogTerm == conflictTerm {
			conflictIndex -= 1
		}
		reply.Success, reply.ConflictIndex = false, conflictIndex
		return
	}

	reply.Success, reply.ConflictIndex = true, -1

	// Delete any conflicting log entries
	i := 0
	for ; i < args.Len; i++ {
		if prevLogIndex+1+i >= rf.logIndex {
			break
		}
		if rf.getEntry(prevLogIndex+1+i).LogTerm != args.Entries[i].LogTerm {
			rf.logIndex = prevLogIndex + 1 + i
			// delete any conflicting log entries
			rf.log = append(rf.log[:prevLogIndex+1+i])
			break
		}
	}

	// After deletion, append fresh logs
	for ; i < args.Len; i++ {
		rf.log = append(rf.log, args.Entries[i])
		rf.logIndex += 1
	}

	oldCommitIndex := rf.commitIndex
	rf.commitIndex = Max(rf.commitIndex, Min(args.CommitIndex, args.PrevLogIndex+args.Len))
	if rf.commitIndex > oldCommitIndex {
		rf.notifyApplyCh <- struct{}{}
	}

	// reset electionTimer
	rf.resetElectionTimer(newRandDuration(electionTimeout)) 
}
