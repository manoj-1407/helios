package raft

import (
	"context"
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

// with deadline enforcement on, a follower handler that outlasts the caller's
// context yields ctx.Err() at the deadline; with it off (the default) the call
// blocks until the handler returns and the deadline is ignored.
func TestTransportDeadlineEnforcement(t *testing.T) {
	for _, enforce := range []bool{false, true} {
		transport := NewChannelTransport()
		transport.SetEnforceDeadlines(enforce)
		ids := []string{"a", "b"}
		nodes := map[string]*RaftNode{}
		for _, id := range ids {
			other := "a"
			if id == "a" {
				other = "b"
			}
			n, err := NewRaftNode(id, []string{other}, t.TempDir(), transport, NewMockStateMachine(), testConfig())
			if err != nil {
				t.Fatal(err)
			}
			transport.Register(n)
			nodes[id] = n
		}
		// "b" has a 150 ms fsync; send it an entry with a 50 ms deadline
		nodes["b"].SetFsyncDelay(150 * time.Millisecond)
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		start := time.Now()
		_, err := transport.SendAppendEntries(ctx, "b", AppendEntriesRequest{
			Term: 1, LeaderID: "a", PrevLogIndex: 0, PrevLogTerm: 0,
			Entries: []LogEntry{{Term: 1, Index: 1, Command: []byte("x")}},
		})
		cancel()
		el := time.Since(start)
		switch {
		case enforce && (err == nil || el > 120*time.Millisecond):
			t.Fatalf("enforce=true: err=%v after %v, want a deadline error near 50ms", err, el)
		case !enforce && (err != nil || el < 150*time.Millisecond):
			t.Fatalf("enforce=false: err=%v after %v, want success after the full 150ms", err, el)
		}
		nodes["a"].Stop()
		nodes["b"].Stop()
	}
}
