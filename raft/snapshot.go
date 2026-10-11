package raft

import "fmt"

// Snapshot, InstallSnapshotRequest/Response, and the actual snapshot-taking
// and snapshot-installing logic are Week 3 scope per the spec's strict
// implementation order (membership/snapshot/bridge don't get touched until
// a 3-node cluster replicates correctly and survives single-node failure).
// The structs live here now because Transport's interface needs them to
// compile -- HandleInstallSnapshot is a real, callable method that returns
// an explicit "not implemented yet" error rather than silently pretending
// to succeed, which matters: a follower that's fallen far enough behind to
// need a snapshot should know loudly that this node can't help it yet,
// not get a response that looks like success.

type Snapshot struct {
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	Data              []byte // serialized state machine state
}

type InstallSnapshotRequest struct {
	Term              uint64
	LeaderID          string
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	Data              []byte
}

type InstallSnapshotResponse struct {
	Term    uint64
	Success bool
}

// TakeSnapshot will, once implemented: flush the state machine (for
// helios-kv, that means flushing the active MemTable to an SSTable),
// record LastIncludedIndex/Term, and truncate the Raft log up to that
// point so it doesn't grow forever. Not implemented yet -- see class doc.
func (r *RaftNode) TakeSnapshot() error {
	return fmt.Errorf("raft: TakeSnapshot not implemented yet (Week 3 scope)")
}

// HandleInstallSnapshot will, once implemented, let a follower that's too
// far behind for normal AppendEntries to catch up receive a full snapshot
// from the leader instead. For now it returns Success: false with the
// current term, which is the correct, honest response for "I can't do
// this" rather than corrupting state by half-implementing it.
func (r *RaftNode) HandleInstallSnapshot(req InstallSnapshotRequest) InstallSnapshotResponse {
	r.mu.Lock()
	defer r.mu.Unlock()
	return InstallSnapshotResponse{Term: r.persisted.CurrentTerm, Success: false}
}
