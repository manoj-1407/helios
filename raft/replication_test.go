package raft

import (
	"strconv"
	"testing"
	"time"
)

func TestLogReplication(t *testing.T) {
	transport := NewChannelTransport()
	ids := []string{"n0", "n1", "n2"}
	nodes := make(map[string]*RaftNode)
	sms := make(map[string]*MockStateMachine)
	for _, id := range ids {
		var peers []string
		for _, other := range ids {
			if other != id {
				peers = append(peers, other)
			}
		}
		dir := t.TempDir()
		sm := NewMockStateMachine()
		node, err := NewRaftNode(id, peers, dir, transport, sm, testConfig())
		if err != nil {
			t.Fatalf("NewRaftNode(%s): %v", id, err)
		}
		transport.Register(node)
		nodes[id] = node
		sms[id] = sm
	}
	for _, n := range nodes {
		n.Start()
	}
	defer func() {
		for _, n := range nodes {
			n.Stop()
		}
	}()

	leader, _ := waitForConvergence(t, nodes, 2*time.Second)
	if leader == nil {
		t.Fatalf("cluster never converged on a leader")
	}

	const numCommands = 5
	var indices []uint64
	for i := 0; i < numCommands; i++ {
		cmd := EncodeMockCommand(Command{Type: CmdPut, Key: keyFor(i), Value: valueFor(i)})
		idx, _, ok := leader.Propose(cmd)
		if !ok {
			t.Fatalf("Propose(%d) failed -- leader stepped down mid-test?", i)
		}
		indices = append(indices, idx)
	}

	// Every node -- not just the leader -- should eventually apply all 5
	// commands, in the same order, to its own state machine.
	for id, sm := range sms {
		ok := waitFor(t, 2*time.Second, func() bool { return sm.AppliedCount() >= numCommands })
		if !ok {
			t.Fatalf("node %s only applied %d/%d commands", id, sm.AppliedCount(), numCommands)
		}
	}

	for i := 0; i < numCommands; i++ {
		wantKey, wantValue := keyFor(i), valueFor(i)
		for id, sm := range sms {
			got, ok := sm.Get(wantKey)
			if !ok || got != wantValue {
				t.Errorf("node %s: Get(%s) = %q, ok=%v, want %q", id, wantKey, got, ok, wantValue)
			}
		}
	}

	// CommitIndex on every node should have caught up to the last proposed index.
	lastIndex := indices[len(indices)-1]
	for id, n := range nodes {
		ok := waitFor(t, 1*time.Second, func() bool { return n.CommitIndex() >= lastIndex })
		if !ok {
			t.Errorf("node %s CommitIndex = %d, want >= %d", id, n.CommitIndex(), lastIndex)
		}
	}
}

// TestFastConflictResolution forces a follower's log to diverge from the
// leader's (by manually appending a bogus entry directly to its log,
// simulating what an old, now-deposed leader could have left behind),
// then verifies it catches up via AppendEntries' conflict response rather
// than getting permanently stuck.
func TestFastConflictResolution(t *testing.T) {
	transport := NewChannelTransport()
	ids := []string{"n0", "n1", "n2"}
	nodes := make(map[string]*RaftNode)
	for _, id := range ids {
		var peers []string
		for _, other := range ids {
			if other != id {
				peers = append(peers, other)
			}
		}
		dir := t.TempDir()
		node, err := NewRaftNode(id, peers, dir, transport, NewMockStateMachine(), testConfig())
		if err != nil {
			t.Fatalf("NewRaftNode(%s): %v", id, err)
		}
		transport.Register(node)
		nodes[id] = node
	}
	for _, n := range nodes {
		n.Start()
	}
	defer func() {
		for _, n := range nodes {
			n.Stop()
		}
	}()

	leader, _ := waitForConvergence(t, nodes, 2*time.Second)
	if leader == nil {
		t.Fatalf("cluster never converged on a leader")
	}

	// Find a follower and inject a handful of bogus entries directly into
	// its log -- entries the leader never sent and doesn't know about.
	var follower *RaftNode
	for _, n := range nodes {
		if n != leader {
			follower = n
			break
		}
	}
	lastIndex, _ := follower.log.LastIndexAndTerm()
	for i := uint64(1); i <= 5; i++ {
		bogusTerm := follower.Term() + 10 // a term high enough to be obviously bogus/from a fictional future leader
		if err := follower.log.Append(LogEntry{
			Term: bogusTerm, Index: lastIndex + i, Command: []byte("bogus"),
		}); err != nil {
			t.Fatalf("injecting bogus entry: %v", err)
		}
	}
	divergedIndex, divergedTerm := follower.log.LastIndexAndTerm()
	if divergedTerm != follower.Term()+10 {
		t.Fatalf("setup failed: follower log doesn't actually show the injected divergence")
	}

	// The injection above only touched the log file directly -- it left
	// the follower's own (possibly nearly-expired) election timer
	// untouched. Without this reset, there's a real, if rare, race: if
	// that pre-existing timer fires before the leader's next heartbeat
	// arrives, the follower would call a real election presenting its
	// artificially-inflated log term as if it were legitimate, and could
	// actually win a vote on the strength of a term nobody really voted
	// it into. That's not a protocol bug -- a log entry with a given term
	// IS supposed to look authoritative to the rest of the cluster, and
	// the "fakeness" here only exists because this test fabricated the
	// entries directly instead of simulating a full prior election. Giving
	// the follower a fresh full-length timeout here means the leader's
	// heartbeats (every few ms) get an overwhelming head start over the
	// follower's own timeout (100+ms), making the test deterministic for
	// practical purposes without changing anything about the algorithm.
	follower.mu.Lock()
	follower.resetElectionTimerLocked()
	follower.mu.Unlock()

	// Now have the leader propose real commands -- this should force
	// replicateToPeer's conflict-detection path against the follower,
	// which should report the conflict, and the leader should jump
	// NextIndex back via fast conflict resolution rather than needing 5
	// separate round trips to step back one index at a time.
	for i := 0; i < 3; i++ {
		cmd := EncodeMockCommand(Command{Type: CmdPut, Key: keyFor(100 + i), Value: valueFor(100 + i)})
		if _, _, ok := leader.Propose(cmd); !ok {
			t.Fatalf("Propose failed")
		}
	}

	ok := waitFor(t, 2*time.Second, func() bool {
		idx, term := follower.log.LastIndexAndTerm()
		return term != divergedTerm || idx != divergedIndex // log has been rewritten away from the bogus state
	})
	if !ok {
		t.Fatalf("follower's diverged log was never corrected -- conflict resolution did not run")
	}

	// And the follower's corrected log should actually match the leader's
	// real entries, not just be "different from the bogus state" by accident.
	leaderLastIndex, leaderLastTerm := leader.log.LastIndexAndTerm()
	ok = waitFor(t, 2*time.Second, func() bool {
		idx, term := follower.log.LastIndexAndTerm()
		return idx == leaderLastIndex && term == leaderLastTerm
	})
	if !ok {
		followerIdx, followerTerm := follower.log.LastIndexAndTerm()
		t.Fatalf("follower log = (index=%d, term=%d), want to match leader's (index=%d, term=%d)",
			followerIdx, followerTerm, leaderLastIndex, leaderLastTerm)
	}
}

func keyFor(i int) string   { return "key" + strconv.Itoa(i) }
func valueFor(i int) string { return "value" + strconv.Itoa(i) }
