package raft

import (
	"os"
	"testing"
)

func TestPersistentStateRoundTrip(t *testing.T) {
	dir := t.TempDir()

	s := PersistentState{CurrentTerm: 7, VotedFor: "node2"}
	if err := SavePersistentState(dir, s); err != nil {
		t.Fatalf("SavePersistentState: %v", err)
	}

	loaded, err := LoadPersistentState(dir)
	if err != nil {
		t.Fatalf("LoadPersistentState: %v", err)
	}
	if loaded != s {
		t.Fatalf("round trip mismatch: saved %+v, loaded %+v", s, loaded)
	}
}

func TestPersistentStateFreshNodeHasZeroValue(t *testing.T) {
	dir := t.TempDir()
	loaded, err := LoadPersistentState(dir)
	if err != nil {
		t.Fatalf("LoadPersistentState on a directory with no state file: %v", err)
	}
	if loaded.CurrentTerm != 0 || loaded.VotedFor != "" {
		t.Fatalf("expected zero value for a brand new node, got %+v", loaded)
	}
}

func TestPersistentStateOverwriteIsAtomic(t *testing.T) {
	dir := t.TempDir()

	if err := SavePersistentState(dir, PersistentState{CurrentTerm: 1, VotedFor: "a"}); err != nil {
		t.Fatalf("first save: %v", err)
	}
	if err := SavePersistentState(dir, PersistentState{CurrentTerm: 5, VotedFor: "b"}); err != nil {
		t.Fatalf("second save: %v", err)
	}

	loaded, err := LoadPersistentState(dir)
	if err != nil {
		t.Fatalf("LoadPersistentState: %v", err)
	}
	if loaded.CurrentTerm != 5 || loaded.VotedFor != "b" {
		t.Fatalf("expected the second save to win cleanly, got %+v", loaded)
	}

	// The write-tmp-then-rename pattern should never leave a .tmp file
	// lying around after a successful save.
	if _, err := os.Stat(statePath(dir) + ".tmp"); !os.IsNotExist(err) {
		t.Fatalf("expected the temp file to be gone after rename, stat returned: %v", err)
	}
}
