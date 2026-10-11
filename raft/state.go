package raft

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
)

type NodeState int

const (
	Follower NodeState = iota
	Candidate
	Leader
)

func (s NodeState) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// PersistentState is everything that MUST survive a crash. Written to disk
// atomically (write-tmp, fsync, rename) before any RPC response is sent.
// Skipping this is how you get split-brain: a node that forgets it already
// voted this term, or forgets a term it already saw, can vote twice or
// accept a stale leader.
type PersistentState struct {
	CurrentTerm uint64
	VotedFor    string // candidateId that received this node's vote this term, "" if none
}

// VolatileState is safe to reconstruct after a crash -- CommitIndex and
// LastApplied get rebuilt from the log and the state machine's own
// recovery, never read back from a file.
type VolatileState struct {
	CommitIndex uint64
	LastApplied uint64
}

// LeaderVolatileState exists only while this node is the leader, and is
// discarded and rebuilt from scratch on every new election -- there is no
// reason to persist it, and persisting it would be actively wrong (a new
// leader's NextIndex/MatchIndex have nothing to do with whatever the
// previous leader believed).
type LeaderVolatileState struct {
	NextIndex  map[string]uint64 // next log index to send to each follower
	MatchIndex map[string]uint64 // highest log index known replicated on each follower
}

// persistentStateFile is the on-disk encoding for PersistentState:
//
//	[magic uint32][currentTerm uint64][votedForLen uint32][votedFor bytes]
//
// Small and fixed-shape enough that "write to a temp file, fsync, rename
// over the real path" is cheap to do unconditionally on every single
// term/vote change -- there's no reason to batch or skip this, the whole
// point of PersistentState is that it's small and must never be stale.
const stateMagic uint32 = 0x48455453 // "HETS"

func statePath(dataDir string) string {
	return filepath.Join(dataDir, "state.bin")
}

// LoadPersistentState reads the on-disk state, or returns the zero value
// (term 0, no vote) if this node has never persisted anything -- a brand
// new node with no state file is exactly equivalent to one that crashed
// before its first term increment.
func LoadPersistentState(dataDir string) (PersistentState, error) {
	path := statePath(dataDir)
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return PersistentState{}, nil
	}
	if err != nil {
		return PersistentState{}, fmt.Errorf("reading persistent state: %w", err)
	}
	if len(data) < 4+8+4 {
		return PersistentState{}, fmt.Errorf("persistent state file %s is truncated (%d bytes)", path, len(data))
	}
	magic := binary.BigEndian.Uint32(data[0:4])
	if magic != stateMagic {
		return PersistentState{}, fmt.Errorf("persistent state file %s has bad magic %x", path, magic)
	}
	term := binary.BigEndian.Uint64(data[4:12])
	votedForLen := binary.BigEndian.Uint32(data[12:16])
	if len(data) < int(16+votedForLen) {
		return PersistentState{}, fmt.Errorf("persistent state file %s is truncated mid-votedFor", path)
	}
	votedFor := string(data[16 : 16+votedForLen])
	return PersistentState{CurrentTerm: term, VotedFor: votedFor}, nil
}

// SavePersistentState writes state durably: write to a sibling temp file,
// fsync the temp file's contents, rename over the real path (POSIX
// rename is atomic -- a crash mid-rename never leaves a half-written
// state.bin), then fsync the *directory* too. That last fsync is the part
// every "just write-tmp-then-rename" writeup skips and it matters: without
// it, a power loss can lose the rename itself even though the temp file's
// bytes were durable, because the directory entry update was still sitting
// in volatile cache.
func SavePersistentState(dataDir string, s PersistentState) error {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return fmt.Errorf("creating data dir: %w", err)
	}
	path := statePath(dataDir)
	tmpPath := path + ".tmp"

	buf := make([]byte, 16+len(s.VotedFor))
	binary.BigEndian.PutUint32(buf[0:4], stateMagic)
	binary.BigEndian.PutUint64(buf[4:12], s.CurrentTerm)
	binary.BigEndian.PutUint32(buf[12:16], uint32(len(s.VotedFor)))
	copy(buf[16:], s.VotedFor)

	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("opening temp state file: %w", err)
	}
	if _, err := f.Write(buf); err != nil {
		f.Close()
		return fmt.Errorf("writing temp state file: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("fsyncing temp state file: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("closing temp state file: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("renaming temp state file into place: %w", err)
	}
	dir, err := os.Open(dataDir)
	if err != nil {
		return fmt.Errorf("opening data dir to fsync rename: %w", err)
	}
	defer dir.Close()
	if err := dir.Sync(); err != nil {
		return fmt.Errorf("fsyncing data dir after rename: %w", err)
	}
	return nil
}
