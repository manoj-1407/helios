package chaos

import (
	"testing"
	"time"

	"github.com/manoj-1407/helios/raft"
)

// documents what an "ok" from Propose (and therefore from KVServer's /kv/put)
// guarantees: the entry is fsynced in the proposing leader's own log, nothing
// more. if that leader is cut off before replicating, the write is acknowledged
// and then lost once the cluster elects a new leader and the old one rejoins.
// see research/RESEARCH_AUDIT.md, "acknowledgement semantics".
func TestProposeAckDoesNotImplyCommit(t *testing.T) {
	c, err := NewCluster(3)
	if err != nil {
		t.Fatalf("NewCluster: %v", err)
	}
	defer c.Close()

	old := c.WaitForLeader(2 * time.Second)
	if old == nil {
		t.Fatal("no initial leader")
	}

	// cut the leader off, then write through it
	c.Partition(old.ID())
	lostCmd := raft.EncodeMockCommand(raft.Command{Type: raft.CmdPut, Key: "acked", Value: "lost"})
	idx, _, ok := old.Propose(lostCmd)
	if !ok {
		t.Fatal("isolated leader refused the proposal; expected it to acknowledge locally")
	}
	// it acknowledged. commit must not advance: nobody else has the entry.
	time.Sleep(300 * time.Millisecond)
	if old.CommitIndex() >= idx {
		t.Fatalf("entry %d committed with the leader isolated; test premise broken", idx)
	}

	// the other two elect a new leader and commit a different write
	var fresh *raft.RaftNode
	if !waitForCond(3*time.Second, func() bool {
		for id, n := range c.Nodes() {
			if id != old.ID() && n.State() == raft.Leader {
				fresh = n
				return true
			}
		}
		return false
	}) {
		t.Fatal("remaining nodes never elected a new leader")
	}
	keepCmd := raft.EncodeMockCommand(raft.Command{Type: raft.CmdPut, Key: "kept", Value: "ok"})
	kidx, _, ok := fresh.Propose(keepCmd)
	if !ok {
		t.Fatal("new leader refused proposal")
	}
	if !waitForCond(2*time.Second, func() bool { return fresh.CommitIndex() >= kidx }) {
		t.Fatal("new leader never committed")
	}

	// heal: the old leader steps down and its unreplicated entry is overwritten
	c.Heal()
	if !waitForCond(3*time.Second, func() bool { return old.State() != raft.Leader }) {
		t.Fatal("old leader never stepped down")
	}
	if !waitForCond(3*time.Second, func() bool {
		_, has := c.StateMachine(old.ID()).Get("kept")
		return has
	}) {
		t.Fatal("old leader never caught up")
	}
	for id := range c.Nodes() {
		if v, has := c.StateMachine(id).Get("acked"); has {
			t.Fatalf("node %s applied the acknowledged-but-uncommitted write (%q); expected it to be lost", id, v)
		}
	}
	t.Logf("write acknowledged at index %d by the isolated leader was lost on every node after rejoin", idx)
}
