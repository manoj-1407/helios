package raft

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

// Snapshot, InstallSnapshot, TakeSnapshot — Raft log compaction.
//
// How snapshot install works in Raft:
//  1. Leader notices a follower's nextIndex is at or before snapshotIndex
//     (i.e., the entries the follower needs have already been compacted away).
//  2. Leader calls its state machine's Snapshot() to serialize current state.
//  3. Leader sends InstallSnapshot RPC to the lagging follower.
//  4. Follower calls its state machine's Restore(data) to replace its state,
//     then sets lastApplied = lastIncludedIndex so Apply() won't re-apply
//     those entries, and truncates its log to remove everything ≤ lastIncludedIndex.
//
// For helios-kv, Snapshot() calls the bridge which POSTs to /snapshot on the
// local JVM, getting back a tar.gz of the SSTable data directory. Restore()
// POSTs that tar to /restore on the follower's JVM, which unpacks it and
// reopens the storage engine.

type Snapshot struct {
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	Data              []byte // opaque; bridge hands this to helios-kv
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
	NodeID  string
}

// snapshotMeta is written to disk alongside the snapshot data so we
// survive a crash between "snapshot taken" and "log truncated".
type snapshotMeta struct {
	LastIncludedIndex uint64 `json:"last_included_index"`
	LastIncludedTerm  uint64 `json:"last_included_term"`
}

// TakeSnapshot flushes the state machine and compacts the Raft log.
// After this call, all entries ≤ lastApplied are removed from the log
// and the snapshot is written to dataDir/snapshot.bin + snapshot.meta.
// TakeSnapshot is safe to call concurrently with Apply — it takes the
// node lock to read lastApplied, then drops it while serializing state.
func (r *RaftNode) TakeSnapshot() error {
	r.mu.Lock()
	lastApplied := r.volatile.LastApplied
	lastTerm, err := r.log.TermAt(lastApplied)
	r.mu.Unlock()
	if err != nil {
		return fmt.Errorf("raft: TakeSnapshot: log.TermAt(%d): %w", lastApplied, err)
	}
	if lastApplied == 0 {
		return nil // nothing applied yet, no point snapshotting
	}

	// Serialize state machine state outside the lock — this may be slow
	// (flushing a MemTable, creating a tar.gz of SSTable files).
	data, err := r.sm.Snapshot()
	if err != nil {
		return fmt.Errorf("raft: TakeSnapshot: state machine: %w", err)
	}

	meta := snapshotMeta{LastIncludedIndex: lastApplied, LastIncludedTerm: lastTerm}
	metaBytes, _ := json.Marshal(meta)

	// Persist snapshot data then meta; recovery checks for meta presence.
	if err := snapAtomicWrite(r.dataDir+"/snapshot.bin", data); err != nil {
		return fmt.Errorf("raft: TakeSnapshot: write snapshot: %w", err)
	}
	if err := snapAtomicWrite(r.dataDir+"/snapshot.meta", metaBytes); err != nil {
		return fmt.Errorf("raft: TakeSnapshot: write meta: %w", err)
	}

	// Compact the log: entries up to and including lastApplied are now
	// covered by the snapshot and can be removed.
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.log.CompactTo(lastApplied); err != nil {
		return fmt.Errorf("raft: TakeSnapshot: compact log to %d: %w", lastApplied, err)
	}
	return nil
}

// HandleInstallSnapshot applies a snapshot sent by the leader.
// On success, the follower's state machine is replaced by the snapshot,
// its log is truncated to remove everything the snapshot covers, and
// lastApplied / commitIndex are advanced to lastIncludedIndex.
func (r *RaftNode) HandleInstallSnapshot(req InstallSnapshotRequest) InstallSnapshotResponse {
	r.mu.Lock()

	if req.Term < r.persisted.CurrentTerm {
		term := r.persisted.CurrentTerm
		r.mu.Unlock()
		return InstallSnapshotResponse{Term: term, Success: false, NodeID: r.id}
	}

	// Higher term from leader — convert to follower.
	if req.Term > r.persisted.CurrentTerm {
		r.becomeFollowerLocked(req.Term)
	}

	r.lastHeartbeatSeen = time.Now()

	// Snapshot is stale: we already have this index applied.
	if req.LastIncludedIndex <= r.volatile.LastApplied {
		term := r.persisted.CurrentTerm
		r.mu.Unlock()
		return InstallSnapshotResponse{Term: term, Success: true, NodeID: r.id}
	}

	// Restore state machine — drop the lock so a slow restore
	// (unpacking tar.gz) doesn't block heartbeat handling.
	r.mu.Unlock()
	err := r.sm.Restore(req.Data)
	r.mu.Lock()

	if err != nil {
		term := r.persisted.CurrentTerm
		r.mu.Unlock()
		return InstallSnapshotResponse{Term: term, Success: false, NodeID: r.id}
	}

	// Persist snapshot so we survive a crash before log compaction.
	meta := snapshotMeta{LastIncludedIndex: req.LastIncludedIndex, LastIncludedTerm: req.LastIncludedTerm}
	metaBytes, _ := json.Marshal(meta)
	_ = snapAtomicWrite(r.dataDir+"/snapshot.bin", req.Data)
	_ = snapAtomicWrite(r.dataDir+"/snapshot.meta", metaBytes)

	// Compact log and advance volatile state.
	_ = r.log.CompactTo(req.LastIncludedIndex)
	r.volatile.LastApplied = req.LastIncludedIndex
	if req.LastIncludedIndex > r.volatile.CommitIndex {
		r.volatile.CommitIndex = req.LastIncludedIndex
	}

	term := r.persisted.CurrentTerm
	r.mu.Unlock()
	return InstallSnapshotResponse{Term: term, Success: true, NodeID: r.id}
}

// LoadSnapshot reads the last persisted snapshot from disk.
// Returns nil if no snapshot exists (fresh node).
func (r *RaftNode) LoadSnapshot() (*Snapshot, error) {
	metaBytes, err := snapReadIfExists(r.dataDir + "/snapshot.meta")
	if err != nil || metaBytes == nil {
		return nil, nil // no snapshot yet
	}
	var meta snapshotMeta
	if err := json.Unmarshal(metaBytes, &meta); err != nil {
		return nil, fmt.Errorf("raft: corrupt snapshot meta: %w", err)
	}
	data, err := snapReadIfExists(r.dataDir + "/snapshot.bin")
	if err != nil || data == nil {
		return nil, nil // meta without data — incomplete, treat as absent
	}
	return &Snapshot{
		LastIncludedIndex: meta.LastIncludedIndex,
		LastIncludedTerm:  meta.LastIncludedTerm,
		Data:              data,
	}, nil
}

// ─── file helpers ────────────────────────────────────────────────────────────

func snapAtomicWrite(path string, data []byte) error {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	if err := func() error {
		f, err := os.Open(tmp)
		if err != nil {
			return err
		}
		defer f.Close()
		return f.Sync()
	}(); err != nil {
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, path)
}

func snapReadIfExists(path string) ([]byte, error) {
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	return data, err
}
