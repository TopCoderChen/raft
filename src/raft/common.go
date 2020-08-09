package raft

type serverState int32

const (
	Leader serverState = iota
	Follower
	Candidate
)

type Err string

const (
	OK         = "OK"
	ErrRPCFail = "ErrRPCFail"
)

func Min(a, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

func Max(a, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}
