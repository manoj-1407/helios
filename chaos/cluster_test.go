package chaos

import (
	"testing"
	"time"

	"github.com/manoj-1407/helios/raft"
)

func waitForCond(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(2 * time.Millisecond)
	}
	return cond()
}

// TestNetworkPartition verifies the actual safety property partitioning a
// cluster is supposed to test: a minority partition (1 of 3 nodes) can
// never elect a leader, because it can never reach quorum -- no matter
// how many times its election timer fires and it tries.
func TestNetworkPartition(t *testing.T) {
	c, err := NewCluster(3)
	if err != nil {
		t.Fatalf("NewCluster: %v", err)
	}
	defer c.Close()

	leader := c.WaitForLeader(2 * time.Second)
	if leader == nil {
		t.Fatalf("cluster never elected an initial leader")
	}

	// Isolate a FOLLOWER specifically, not whichever node a map iteration
	// happens to land on first. Isolating the already-elected leader
	// itself wouldn't violate anything -- it already won fairly, with a
	// real quorum, before being cut off, so it correctly keeps believing
	// it's leader (it just can't get anything new committed without
	// contact with anyone, which IS the property worth testing, just not
	// via State() on that particular node). The actual claim under test
	// here -- "a lone, cut-off node can never elect itself" -- needs a
	// follower as the isolated node.
	var isolated string
	for id := range c.Nodes() {
		if id != leader.ID() {
			isolated = id
			break
		}
	}
	c.Partition(isolated)

	// Give the isolated node plenty of time to repeatedly time out and
	// retry elections -- it should never succeed, because it can never
	// get a second vote from anyone.
	time.Sleep(500 * time.Millisecond)

	if c.Node(isolated).State() == raft.Leader {
		t.Fatalf("isolated minority node %s became leader -- this should be impossible without quorum", isolated)
	}

	// And the OTHER two nodes (still a majority) should have a working
	// leader among themselves the whole time, possibly a new one if the
	// isolated node happened to be the original leader.
	majorityHasLeader := waitForCond(1*time.Second, func() bool {
		for id, n := range c.Nodes() {
			if id != isolated && n.State() == raft.Leader {
				return true
			}
		}
		return false
	})
	if !majorityHasLeader {
		t.Fatalf("the majority partition (2 of 3 nodes) never had a leader")
	}
}

// TestPartitionHeal verifies that once a partition heals, a stale leader
// (if the isolated node happened to have been the leader) correctly steps
// down on learning about a higher term, and the cluster converges back to
// a single agreed leader.
func TestPartitionHeal(t *testing.T) {
	c, err := NewCluster(3)
	if err != nil {
		t.Fatalf("NewCluster: %v", err)
	}
	defer c.Close()

	originalLeader := c.WaitForLeader(2 * time.Second)
	if originalLeader == nil {
		t.Fatalf("cluster never elected an initial leader")
	}
	leaderID := originalLeader.ID()

	c.Partition(leaderID)
	time.Sleep(400 * time.Millisecond) // let the majority elect a new leader without the old one

	newLeader := c.WaitForLeader(1 * time.Second)
	if newLeader == nil {
		t.Fatalf("majority partition never elected a replacement leader")
	}

	c.Heal()

	// After healing, the cluster should converge back to exactly one
	// leader and everyone agreeing on the term -- the old leader (now
	// reachable again) must recognize the new leader's higher term and
	// step down rather than continuing to believe it's still in charge.
	ok := waitForCond(1*time.Second, func() bool {
		leaderCount, term := 0, uint64(0)
		for _, n := range c.Nodes() {
			if n.State() == raft.Leader {
				leaderCount++
				term = n.Term()
			}
		}
		if leaderCount != 1 {
			return false
		}
		for _, n := range c.Nodes() {
			if n.Term() != term {
				return false
			}
		}
		return true
	})
	if !ok {
		t.Fatalf("cluster never re-converged on a single leader after the partition healed")
	}

	if c.Node(leaderID).State() == raft.Leader && leaderID != newLeader.ID() {
		t.Fatalf("the old, partitioned-away leader %s still believes it's leader after healing -- it should have stepped down", leaderID)
	}
}

// TestNodeCrashAndRecover kills a follower, lets the cluster keep making
// progress without it, then restarts it from the same data directory and
// verifies it catches up to everything it missed.
func TestNodeCrashAndRecover(t *testing.T) {
	c, err := NewCluster(3)
	if err != nil {
		t.Fatalf("NewCluster: %v", err)
	}
	defer c.Close()

	leader := c.WaitForLeader(2 * time.Second)
	if leader == nil {
		t.Fatalf("cluster never elected an initial leader")
	}

	var victim string
	for id := range c.Nodes() {
		if id != leader.ID() {
			victim = id
			break
		}
	}
	c.KillNode(victim)

	// Cluster should still make progress with 2 of 3 nodes (still a majority).
	for i := 0; i < 5; i++ {
		cmd := raft.EncodeMockCommand(raft.Command{Type: raft.CmdPut, Key: keyForTest(i), Value: valueForTest(i)})
		if _, _, ok := leader.Propose(cmd); !ok {
			// leader might have changed if the killed node happened to be
			// re-elected as leader in some other test run's residual state
			// -- re-fetch and retry once.
			leader = c.WaitForLeader(1 * time.Second)
			if leader == nil {
				t.Fatalf("lost the leader entirely with only one node down out of three")
			}
			_, _, ok = leader.Propose(cmd)
			if !ok {
				t.Fatalf("Propose failed even after re-fetching the leader")
			}
		}
	}

	if err := c.RestartNode(victim); err != nil {
		t.Fatalf("RestartNode(%s): %v", victim, err)
	}

	restarted := c.Node(victim)
	ok := waitForCond(2*time.Second, func() bool {
		return restarted.CommitIndex() >= leader.CommitIndex()
	})
	if !ok {
		t.Fatalf("restarted node %s never caught up: its commitIndex=%d, leader's=%d",
			victim, restarted.CommitIndex(), leader.CommitIndex())
	}
}

func keyForTest(i int) string   { return "ck" + string(rune('a'+i)) }
func valueForTest(i int) string { return "cv" + string(rune('a'+i)) }
