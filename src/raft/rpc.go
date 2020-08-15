package raft

// Put related RPC handlers in one place.

// Reset timer inside each handler.

// Servers retry RPCs if they do not receive a response in a timely manner.

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

	rf.persist()

	rf.resetElectionTimer(newRandDuration(electionTimeout))
}

// AppendEntries RPC handler.
// Consistency check performed by AppendEntries:
// When sending an AppendEntries RPC, the leader includes the index
// and term of the entry in its log that immediately precedes
// the new entries. If the follower does not find an entry in
// its log with the same index and term, then it refuses the
// new entries.

// When it gets a successful response from the majority of nodes, the command is committed and the client gets a confirmation;
// In the next AppendEntries RPC sent to the follower (that can be a new entry or just a heartbeat), the follower also commits the message;
// The AppendEntries RPC implements a consistency check, to guarantee its local log is consistent with the leader’s

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
	// Reset electionTimer
	rf.resetElectionTimer(newRandDuration(electionTimeout))
	if args.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = args.Term, -1
	}
	rf.state = Follower

	logIndex := rf.logIndex
	prevLogIndex := args.PrevLogIndex
	// Server has wrong information about this follower.
	// The index could be either bigger or smaller, or term doesn't match.
	// Follower don't agree with leader on last log entry.
	if logIndex <= prevLogIndex || rf.getEntry(prevLogIndex).LogTerm != args.PrevLogTerm {
		conflictIndex := Min(rf.logIndex-1, prevLogIndex)
		conflictTerm := rf.getEntry(conflictIndex).LogTerm
		// Find the smallest conflictIndex
		for conflictIndex > rf.commitIndex && rf.getEntry(conflictIndex-1).LogTerm == conflictTerm {
			conflictIndex--
		}
		reply.Success, reply.ConflictIndex = false, conflictIndex
		return
	}

	reply.Success, reply.ConflictIndex = true, -1

	// Find the first starting diff point & Delete any conflicting log entries.
	// Notice this i is global here !
	i := 0
	for ; i < args.Len; i++ {
		if prevLogIndex+1+i >= rf.logIndex {
			break
		}
		if rf.getEntry(prevLogIndex+1+i).LogTerm != args.Entries[i].LogTerm {
			// Find the turning point of diff !
			rf.logIndex = prevLogIndex + 1 + i
			// Delete any conflicting log entries
			rf.log = append(rf.log[:prevLogIndex+1+i])
			break
		}
	}

	// After deletion, append the remaining logs from master.
	for ; i < args.Len; i++ {
		rf.log = append(rf.log, args.Entries[i])
		rf.logIndex++
	}

	// Sync the commit-index if necessary.
	oldCommitIndex := rf.commitIndex
	// Min(server commit index, local replicated index)
	if potentialNewCommit := Min(args.CommitIndex, args.PrevLogIndex+args.Len); potentialNewCommit > rf.commitIndex {
		rf.commitIndex = potentialNewCommit
	}
	if rf.commitIndex > oldCommitIndex {
		rf.notifyApplyCh <- struct{}{}
	}

	// reset electionTimer
	rf.resetElectionTimer(newRandDuration(electionTimeout))

	rf.persist()
}
