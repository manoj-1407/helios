package raft

// AppendEntriesRequest both replicates log entries AND serves as the
// heartbeat (an empty Entries slice with no new data still resets the
// follower's election timer and confirms the leader is alive).
type AppendEntriesRequest struct {
	Term         uint64
	LeaderID     string
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []LogEntry
	LeaderCommit uint64
}

type AppendEntriesResponse struct {
	Term    uint64
	Success bool
	// Fast conflict resolution (Raft §5.3 optimization): instead of
	// stepping back one entry at a time on conflict, the follower tells
	// the leader the conflicting term and the first index it stores for
	// that term, so the leader can jump directly there instead of
	// retrying index-by-index.
	ConflictTerm  uint64
	ConflictIndex uint64
}

type RequestVoteRequest struct {
	Term         uint64
	CandidateID  string
	LastLogIndex uint64
	LastLogTerm  uint64
}

type RequestVoteResponse struct {
	Term        uint64
	VoteGranted bool
}
