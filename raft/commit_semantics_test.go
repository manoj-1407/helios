package raft

import (
	"testing"
	"time"
)

// the bench treats "leader.CommitIndex() >= idx" as the moment a write is
// committed (research/RESEARCH_AUDIT.md, round 2). this checks that reading
// that condition is safe: at that instant a majority of nodes must already hold
// the entry at idx, with the same term and command, in their own logs.
func TestCommitIndexImpliesMajorityHasEntry(t *testing.T) {
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
		node, err := NewRaftNode(id, peers, t.TempDir(), transport, NewMockStateMachine(), testConfig())
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

	for i := 0; i < 200; i++ {
		cmd := EncodeMockCommand(Command{Type: CmdPut, Key: keyFor(i), Value: valueFor(i)})
		idx, term, ok := leader.Propose(cmd)
		if !ok {
			t.Fatalf("Propose(%d) failed", i)
		}
		deadline := time.Now().Add(2 * time.Second)
		for leader.CommitIndex() < idx {
			if time.Now().After(deadline) {
				t.Fatalf("index %d never committed", idx)
			}
			time.Sleep(100 * time.Microsecond)
		}
		have := 0
		for _, n := range nodes {
			if e, found := n.log.Get(idx); found && e.Term == term && string(e.Command) == string(cmd) {
				have++
			}
		}
		if have < 2 {
			t.Fatalf("index %d reported committed but only %d of 3 nodes hold it", idx, have)
		}
	}
}
