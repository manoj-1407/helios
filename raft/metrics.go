package raft

import (
	"sync"
	"time"
)

// ReplicationMetrics exposes per-follower lag data so external components
// (specifically the helios-kv compaction manager via the bridge) can make
// decisions based on actual replication health -- not just local write/read
// ratios. This is the feedback loop that doesn't exist in RocksDB, TiKV,
// or Cassandra: the storage layer knowing about replication state and
// adapting its I/O behavior accordingly.
type ReplicationMetrics struct {
	mu sync.RWMutex

	// lag[peerID] = how many log entries behind this follower currently is
	lag map[string]uint64

	// lastAck[peerID] = when we last heard a successful AppendEntries from this peer
	lastAck map[string]time.Time

	// applyLatency[peerID] = rolling P99 apply latency in nanoseconds,
	// reported back from the bridge layer (the bridge knows because it
	// measures how long each HTTP POST to helios-kv takes)
	applyLatencyP99 map[string]int64
}

func NewReplicationMetrics() *ReplicationMetrics {
	return &ReplicationMetrics{
		lag:             make(map[string]uint64),
		lastAck:         make(map[string]time.Time),
		applyLatencyP99: make(map[string]int64),
	}
}

func (m *ReplicationMetrics) UpdateLag(peer string, leaderIndex, matchIndex uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if leaderIndex >= matchIndex {
		m.lag[peer] = leaderIndex - matchIndex
	} else {
		m.lag[peer] = 0
	}
	m.lastAck[peer] = time.Now()
}

func (m *ReplicationMetrics) UpdateApplyLatency(peer string, p99Ns int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.applyLatencyP99[peer] = p99Ns
}

// PressureLevel returns a 0.0-1.0 score representing how much replication
// pressure the cluster is under right now. 0.0 = perfectly healthy,
// 1.0 = severely degraded. This single number is what the compaction
// classifier uses as a third input alongside write/read ratio.
//
// The formula: weighted sum of (lag / threshold) + (latency / threshold),
// clamped to 1.0. The thresholds are tunable but default to values that
// reflect "a follower more than 100 entries behind or experiencing
// >500ms apply latency is meaningfully stressed."
func (m *ReplicationMetrics) PressureLevel() float64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.lag) == 0 {
		return 0.0
	}

	const (
		lagThreshold     = 100.0  // entries
		latencyThreshold = 5e8    // 500ms in nanoseconds
	)

	var totalPressure float64
	count := 0

	for peer, lag := range m.lag {
		lagScore := float64(lag) / lagThreshold
		if lagScore > 1.0 {
			lagScore = 1.0
		}

		latencyScore := 0.0
		if p99, ok := m.applyLatencyP99[peer]; ok && p99 > 0 {
			latencyScore = float64(p99) / latencyThreshold
			if latencyScore > 1.0 {
				latencyScore = 1.0
			}
		}

		// Check staleness: if we haven't heard from this peer in 2x
		// election timeout, treat it as maximum pressure
		staleness := 0.0
		if last, ok := m.lastAck[peer]; ok {
			if time.Since(last) > 2*time.Second {
				staleness = 1.0
			}
		}

		peerPressure := 0.4*lagScore + 0.4*latencyScore + 0.2*staleness
		totalPressure += peerPressure
		count++
		_ = peer
	}

	avg := totalPressure / float64(count)
	if avg > 1.0 {
		avg = 1.0
	}
	return avg
}

// ShouldReduceIOPressure returns true when the cluster is under enough
// replication pressure that helios-kv should back off on compaction
// aggressiveness. This is the actual decision boundary -- above 0.6
// pressure, we want less disk I/O, not more.
func (m *ReplicationMetrics) ShouldReduceIOPressure() bool {
	return m.PressureLevel() > 0.6
}

// MaxFollowerLag returns the highest lag among all followers.
// Used by the leader to decide whether to delay a compaction round.
func (m *ReplicationMetrics) MaxFollowerLag() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var max uint64
	for _, lag := range m.lag {
		if lag > max {
			max = lag
		}
	}
	return max
}
