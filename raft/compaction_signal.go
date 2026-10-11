package raft

import (
	"sync"
	"time"
)

// CompactionCoordinator prevents I/O storms from simultaneous compaction
// across all nodes. Without coordination, all three nodes in a cluster can
// independently decide to compact at the same time -- they all have similar
// write patterns and similar SSTable accumulation rates. When all three
// compact simultaneously, disk I/O triples, replication lag spikes, and
// the very problem compaction is supposed to solve gets amplified.
//
// The solution: the leader registers its compaction intent before starting.
// Other nodes check whether a peer recently compacted before starting their
// own. If yes, they back off for a stagger window. This is a soft protocol
// (no consensus required, no log entries) -- a node that doesn't receive
// the signal still compacts, just potentially at the same time as another.
// That's acceptable: the coordination is best-effort, not a hard guarantee.
//
// The stagger window (2s default) is intentionally short -- long enough to
// separate most concurrent compactions, short enough that a node doesn't
// defer indefinitely if the signal is lost.
type CompactionCoordinator struct {
	mu              sync.RWMutex
	lastCompactions map[string]time.Time // nodeID -> when it last started compacting
	staggerWindow   time.Duration
}

func NewCompactionCoordinator() *CompactionCoordinator {
	return &CompactionCoordinator{
		lastCompactions: make(map[string]time.Time),
		staggerWindow:   2 * time.Second,
	}
}

// RecordCompactionStart registers that nodeID is beginning a compaction.
// Called by the bridge before forwarding a compaction command to helios-kv.
func (c *CompactionCoordinator) RecordCompactionStart(nodeID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lastCompactions[nodeID] = time.Now()
}

// ShouldDefer returns true if another node compacted recently enough that
// this node should wait before starting its own compaction.
// thisNodeID is excluded from the check -- we don't defer based on our own
// previous compaction, only based on peers.
func (c *CompactionCoordinator) ShouldDefer(thisNodeID string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for nodeID, t := range c.lastCompactions {
		if nodeID == thisNodeID {
			continue
		}
		if time.Since(t) < c.staggerWindow {
			return true
		}
	}
	return false
}

// DeferDuration returns how long this node should wait before compacting,
// based on when the most recent peer compaction started.
func (c *CompactionCoordinator) DeferDuration(thisNodeID string) time.Duration {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var mostRecent time.Time
	for nodeID, t := range c.lastCompactions {
		if nodeID == thisNodeID {
			continue
		}
		if t.After(mostRecent) {
			mostRecent = t
		}
	}
	if mostRecent.IsZero() {
		return 0
	}
	remaining := c.staggerWindow - time.Since(mostRecent)
	if remaining < 0 {
		return 0
	}
	return remaining
}
