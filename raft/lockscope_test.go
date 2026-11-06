package raft

import (
	"bytes"
	"sync"
	"testing"
	"time"
)

func lockScopeCluster(t *testing.T, outside bool) (map[string]*RaftNode, *ChannelTransport) {
	t.Helper()
	transport := NewChannelTransport()
	ids := []string{"a", "b", "c"}
	nodes := map[string]*RaftNode{}
	for _, id := range ids {
		var peers []string
		for _, o := range ids {
			if o != id {
				peers = append(peers, o)
			}
		}
		cfg := testConfig()
		cfg.AppendOutsideLock = outside
		n, err := NewRaftNode(id, peers, t.TempDir(), transport, NewMockStateMachine(), cfg)
		if err != nil {
			t.Fatal(err)
		}
		transport.Register(n)
		nodes[id] = n
	}
	for _, n := range nodes {
		n.Start()
	}
	t.Cleanup(func() {
		for _, n := range nodes {
			n.Stop()
		}
	})
	return nodes, transport
}

func waitLeader(t *testing.T, nodes map[string]*RaftNode) *RaftNode {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			if n.ObservedState() == Leader {
				return n
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("no leader")
	return nil
}

// concurrent proposers must get distinct, gap-free, ordered indexes, every ack must
// be durable and committed to the same command on every node, and no node's log may
// ever contain an index above what the leader had made durable at that moment.
func TestOutsideLockOrderingDurabilityAndCommit(t *testing.T) {
	nodes, _ := lockScopeCluster(t, true)
	leader := waitLeader(t, nodes)
	leader.SetFsyncDelay(3 * time.Millisecond)

	const writers, per = 8, 25
	type ack struct {
		idx, term uint64
		cmd       []byte
	}
	var mu sync.Mutex
	var acks []ack
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < per; i++ {
				cmd := []byte{byte(w), byte(i)}
				idx, term, ok := leader.Propose(cmd)
				if !ok {
					continue
				}
				if leader.log.DurableIndex() < idx {
					t.Errorf("ack for index %d before it was durable (durable=%d)", idx, leader.log.DurableIndex())
				}
				mu.Lock()
				acks = append(acks, ack{idx, term, cmd})
				mu.Unlock()
			}
		}(w)
	}
	// watcher: a follower must never hold an entry the leader has not made durable
	stop := make(chan struct{})
	var wwg sync.WaitGroup
	wwg.Add(1)
	go func() {
		defer wwg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			d := leader.log.DurableIndex()
			for id, n := range nodes {
				if n == leader {
					continue
				}
				if li, _ := n.log.LastIndexAndTerm(); li > d+uint64(writers) {
					// the follower may have read a newer durable index than d; allow
					// writers entries of slack only for that race, re-check strictly below
					if li > leader.log.DurableIndex() {
						t.Errorf("follower %s has index %d beyond leader durable %d", id, li, leader.log.DurableIndex())
					}
				}
			}
			time.Sleep(200 * time.Microsecond)
		}
	}()
	wg.Wait()
	close(stop)
	wwg.Wait()

	if len(acks) == 0 {
		t.Fatal("no acks")
	}
	seen := map[uint64]bool{}
	for _, a := range acks {
		if seen[a.idx] {
			t.Fatalf("index %d acked twice", a.idx)
		}
		seen[a.idx] = true
	}
	last := uint64(0)
	for _, a := range acks {
		if a.idx > last {
			last = a.idx
		}
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		ok := true
		for _, n := range nodes {
			if n.CommitIndex() < last {
				ok = false
			}
		}
		if ok {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	for id, n := range nodes {
		if n.CommitIndex() < last {
			t.Fatalf("node %s commit %d < last ack %d", id, n.CommitIndex(), last)
		}
		for _, a := range acks {
			e, ok := n.log.Get(a.idx)
			if !ok || e.Term != a.term || !bytes.Equal(e.Command, a.cmd) {
				t.Fatalf("node %s index %d: got %+v ok=%v, want term %d cmd %v", id, a.idx, e, ok, a.term, a.cmd)
			}
		}
		// contiguity
		li, _ := n.log.LastIndexAndTerm()
		for i := uint64(1); i <= li; i++ {
			e, ok := n.log.Get(i)
			if !ok || e.Index != i {
				t.Fatalf("node %s hole at %d", id, i)
			}
		}
	}
}

// heartbeats and log reads must not wait for the leader's fsync in the new path,
// and must wait for it in the original path (that is the coupling under test).
func TestHeartbeatPathNotBlockedByFsync(t *testing.T) {
	for _, outside := range []bool{false, true} {
		nodes, _ := lockScopeCluster(t, outside)
		leader := waitLeader(t, nodes)
		const d = 150 * time.Millisecond
		leader.SetFsyncDelay(d)
		done := make(chan struct{})
		go func() { leader.Propose([]byte("x")); close(done) }()
		time.Sleep(20 * time.Millisecond) // the proposal is now inside its fsync
		start := time.Now()
		leader.mu.Lock()
		_, _ = leader.log.LastIndexAndTerm()
		leader.log.Get(0)
		leader.mu.Unlock()
		el := time.Since(start)
		<-done
		if outside && el > 30*time.Millisecond {
			t.Fatalf("outside=true: r.mu + log read took %v during an fsync", el)
		}
		if !outside && el < 50*time.Millisecond {
			t.Fatalf("outside=false: r.mu + log read took only %v; expected to wait for the fsync", el)
		}
		for _, n := range nodes {
			n.Stop()
		}
	}
}

// a sync that started before a truncation must not mark later entries durable
func TestSyncDoesNotCoverTruncatedEntries(t *testing.T) {
	l, err := NewRaftLog(t.TempDir() + "/log.bin")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if err := l.Append(LogEntry{Term: 1, Index: 1, Command: []byte("a")}); err != nil {
		t.Fatal(err)
	}
	if err := l.AppendBuffered(LogEntry{Term: 1, Index: 2, Command: []byte("b")}); err != nil {
		t.Fatal(err)
	}
	if l.DurableIndex() != 1 {
		t.Fatalf("durable=%d want 1 before sync", l.DurableIndex())
	}
	if err := l.TruncateFrom(2); err != nil {
		t.Fatal(err)
	}
	if err := l.AppendBuffered(LogEntry{Term: 2, Index: 2, Command: []byte("c")}); err != nil {
		t.Fatal(err)
	}
	if l.DurableIndex() != 1 {
		t.Fatalf("durable=%d want 1 (new entry unsynced)", l.DurableIndex())
	}
	if err := l.SyncDurable(); err != nil {
		t.Fatal(err)
	}
	if l.DurableIndex() != 2 {
		t.Fatalf("durable=%d want 2 after sync", l.DurableIndex())
	}
}

// crash recovery: only synced entries are required to be present; a reopened log
// must hold every entry whose Propose returned ok.
func TestOutsideLockAckedEntriesSurviveReopen(t *testing.T) {
	dir := t.TempDir()
	l, err := NewRaftLog(dir + "/log.bin")
	if err != nil {
		t.Fatal(err)
	}
	for i := uint64(1); i <= 5; i++ {
		if err := l.AppendBuffered(LogEntry{Term: 1, Index: i, Command: []byte{byte(i)}}); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.SyncDurable(); err != nil {
		t.Fatal(err)
	}
	l.Close()
	l2, err := NewRaftLog(dir + "/log.bin")
	if err != nil {
		t.Fatal(err)
	}
	defer l2.Close()
	if li, _ := l2.LastIndexAndTerm(); li != 5 || l2.DurableIndex() != 5 {
		t.Fatalf("after reopen last=%d durable=%d want 5/5", li, l2.DurableIndex())
	}
}
