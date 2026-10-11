package raft

import (
	"path/filepath"
	"testing"
)

func TestRaftLogAppendAndGet(t *testing.T) {
	dir := t.TempDir()
	log, err := NewRaftLog(filepath.Join(dir, "log.bin"))
	if err != nil {
		t.Fatalf("NewRaftLog: %v", err)
	}
	defer log.Close()

	entries := []LogEntry{
		{Term: 1, Index: 1, Command: []byte("put:a=1")},
		{Term: 1, Index: 2, Command: []byte("put:b=2")},
		{Term: 2, Index: 3, Command: []byte("put:c=3")},
	}
	if err := log.Append(entries...); err != nil {
		t.Fatalf("Append: %v", err)
	}

	for _, want := range entries {
		got, ok := log.Get(want.Index)
		if !ok {
			t.Fatalf("Get(%d): not found", want.Index)
		}
		if got.Term != want.Term || string(got.Command) != string(want.Command) {
			t.Fatalf("Get(%d) = %+v, want %+v", want.Index, got, want)
		}
	}

	lastIndex, lastTerm := log.LastIndexAndTerm()
	if lastIndex != 3 || lastTerm != 2 {
		t.Fatalf("LastIndexAndTerm() = (%d, %d), want (3, 2)", lastIndex, lastTerm)
	}

	// Index 0 is always the sentinel, even on a log with real entries.
	sentinel, ok := log.Get(0)
	if !ok || sentinel.Term != 0 || sentinel.Index != 0 {
		t.Fatalf("Get(0) should be the sentinel, got ok=%v entry=%+v", ok, sentinel)
	}
}

func TestRaftLogRecoversAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "log.bin")

	log1, err := NewRaftLog(path)
	if err != nil {
		t.Fatalf("NewRaftLog: %v", err)
	}
	if err := log1.Append(
		LogEntry{Term: 1, Index: 1, Command: []byte("a")},
		LogEntry{Term: 1, Index: 2, Command: []byte("b")},
	); err != nil {
		t.Fatalf("Append: %v", err)
	}
	log1.Close()

	log2, err := NewRaftLog(path)
	if err != nil {
		t.Fatalf("reopening NewRaftLog: %v", err)
	}
	defer log2.Close()

	lastIndex, lastTerm := log2.LastIndexAndTerm()
	if lastIndex != 2 || lastTerm != 1 {
		t.Fatalf("after reopen: LastIndexAndTerm() = (%d, %d), want (2, 1)", lastIndex, lastTerm)
	}
	entry, ok := log2.Get(2)
	if !ok || string(entry.Command) != "b" {
		t.Fatalf("after reopen: Get(2) = %+v, ok=%v, want Command=b", entry, ok)
	}
}

func TestRaftLogTruncateFromIsPhysicalAndPersists(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "log.bin")

	log1, err := NewRaftLog(path)
	if err != nil {
		t.Fatalf("NewRaftLog: %v", err)
	}
	if err := log1.Append(
		LogEntry{Term: 1, Index: 1, Command: []byte("a")},
		LogEntry{Term: 1, Index: 2, Command: []byte("b")},
		LogEntry{Term: 2, Index: 3, Command: []byte("c")}, // a follower would truncate from here on conflict
	); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := log1.TruncateFrom(3); err != nil {
		t.Fatalf("TruncateFrom: %v", err)
	}
	lastIndex, _ := log1.LastIndexAndTerm()
	if lastIndex != 2 {
		t.Fatalf("after TruncateFrom(3): LastIndexAndTerm index = %d, want 2", lastIndex)
	}
	log1.Close()

	// The whole point of TruncateFrom fsyncing is that this survives a
	// reopen -- an in-memory-only truncate would resurrect entry 3 here.
	log2, err := NewRaftLog(path)
	if err != nil {
		t.Fatalf("reopening: %v", err)
	}
	defer log2.Close()
	lastIndex, _ = log2.LastIndexAndTerm()
	if lastIndex != 2 {
		t.Fatalf("after reopen following TruncateFrom: index = %d, want 2 (truncate must be physical, not just in-memory)", lastIndex)
	}
	if _, ok := log2.Get(3); ok {
		t.Fatalf("entry 3 should be gone after TruncateFrom + reopen, but Get(3) still found it")
	}

	// And new entries appended after a truncate must land where the
	// truncated data used to be, not get corrupted by leftover bytes.
	if err := log2.Append(LogEntry{Term: 3, Index: 3, Command: []byte("replacement")}); err != nil {
		t.Fatalf("Append after truncate: %v", err)
	}
	entry, ok := log2.Get(3)
	if !ok || string(entry.Command) != "replacement" || entry.Term != 3 {
		t.Fatalf("Get(3) after truncate+reappend = %+v, ok=%v, want Term=3 Command=replacement", entry, ok)
	}
}

func TestRaftLogSlice(t *testing.T) {
	dir := t.TempDir()
	log, err := NewRaftLog(filepath.Join(dir, "log.bin"))
	if err != nil {
		t.Fatalf("NewRaftLog: %v", err)
	}
	defer log.Close()

	for i := uint64(1); i <= 5; i++ {
		if err := log.Append(LogEntry{Term: 1, Index: i, Command: []byte{byte(i)}}); err != nil {
			t.Fatalf("Append(%d): %v", i, err)
		}
	}

	got := log.Slice(2, 5)
	if len(got) != 3 {
		t.Fatalf("Slice(2,5) returned %d entries, want 3", len(got))
	}
	for i, e := range got {
		wantIndex := uint64(2 + i)
		if e.Index != wantIndex {
			t.Fatalf("Slice(2,5)[%d].Index = %d, want %d", i, e.Index, wantIndex)
		}
	}

	if got := log.Slice(10, 20); got != nil {
		t.Fatalf("Slice past the end of the log should return nil/empty, got %v", got)
	}
}

func TestRaftLogTornTrailingRecordRecoversCleanly(t *testing.T) {
	// Mirrors helios-kv's WALTest: a torn trailing record at the tail of
	// the file is the expected shape of an interrupted write, not a bug --
	// recovery should silently drop only that one record, not throw and
	// not lose anything that was fully written before it.
	dir := t.TempDir()
	path := filepath.Join(dir, "log.bin")

	log1, err := NewRaftLog(path)
	if err != nil {
		t.Fatalf("NewRaftLog: %v", err)
	}
	for i := uint64(1); i <= 5; i++ {
		if err := log1.Append(LogEntry{Term: 1, Index: i, Command: []byte("entry")}); err != nil {
			t.Fatalf("Append(%d): %v", i, err)
		}
	}
	log1.Close()

	fullSize := fileSize(t, path)
	truncateFile(t, path, fullSize-3) // simulate a crash mid-write on the last record

	log2, err := NewRaftLog(path)
	if err != nil {
		t.Fatalf("NewRaftLog on a torn file should still succeed: %v", err)
	}
	defer log2.Close()

	lastIndex, _ := log2.LastIndexAndTerm()
	if lastIndex != 4 {
		t.Fatalf("after torn trailing record: last index = %d, want 4 (entry 5 was torn and should be dropped)", lastIndex)
	}

	// And the log must still be writable afterward -- replay should have
	// physically truncated the torn bytes away, not just ignored them.
	if err := log2.Append(LogEntry{Term: 1, Index: 5, Command: []byte("rewritten")}); err != nil {
		t.Fatalf("Append after torn-record recovery: %v", err)
	}
	entry, ok := log2.Get(5)
	if !ok || string(entry.Command) != "rewritten" {
		t.Fatalf("Get(5) after recovery+append = %+v, ok=%v", entry, ok)
	}
}
