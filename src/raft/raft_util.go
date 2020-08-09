package raft

// Put utils for the raft labs, for cleaner code.

import( 
	"time"
	"math/rand"
)
func newRandDuration(min time.Duration) time.Duration {
	extra := time.Duration(rand.Int63()) % min
	return time.Duration(extra + min)
}

func (rf *Raft) getEntry(i int) LogEntry {
	return rf.log[i]
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

func (rf *Raft) getRangeEntry(fromInclusive, toExclusive int) []LogEntry {
	return append([]LogEntry{}, rf.log[fromInclusive:toExclusive]...)
}

// Check raft can commit log entry at index (if majority agrees on the match)
func (rf *Raft) canCommit(index int) bool {
	if index < rf.logIndex && rf.commitIndex < index && rf.getEntry(index).LogTerm == rf.currentTerm {
		majority, count := len(rf.peers)/2+1, 0
		for j := 0; j < len(rf.peers); j++ {
			if rf.matchIndex[j] >= index {
				count += 1
			}
		}
		return count >= majority
	} else {
		return false
	}
}
