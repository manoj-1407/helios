package raft

import (
	"testing"
	"time"
)

func TestFsyncDelayIsInjectedAndCounted(t *testing.T) {
	l, err := NewRaftLog(t.TempDir() + "/log.bin")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	// baseline: no injected delay
	if err := l.Append(LogEntry{Term: 1, Index: 1, Command: []byte("a")}); err != nil {
		t.Fatal(err)
	}
	n, _, inj := l.FsyncStats()
	if n != 1 || inj != 0 {
		t.Fatalf("baseline: count=%d injected=%d, want 1 and 0", n, inj)
	}

	const d = 20 * time.Millisecond
	l.SetFsyncDelay(d)
	for i := uint64(2); i <= 6; i++ {
		start := time.Now()
		if err := l.Append(LogEntry{Term: 1, Index: i, Command: []byte("b")}); err != nil {
			t.Fatal(err)
		}
		if el := time.Since(start); el < d {
			t.Fatalf("append %d took %v, shorter than injected %v", i, el, d)
		}
	}
	n, total, inj := l.FsyncStats()
	if n != 6 || inj != 5*int64(d) {
		t.Fatalf("count=%d injected=%v, want 6 and %v", n, time.Duration(inj), 5*d)
	}
	if total < inj {
		t.Fatalf("total sync time %v is less than injected %v", time.Duration(total), time.Duration(inj))
	}
}
