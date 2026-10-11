package store

import (
	"fmt"
	"math"
	"time"

	"github.com/manoj-1407/helios/raft"
)

// CompactionPressureLoop is the feedback loop between Raft replication health
// and helios-kv compaction behavior. It runs as a background goroutine on
// every node (leader and follower alike -- followers also need to know the
// cluster's pressure level since they do their own compaction).
//
// The loop:
//   1. Reads RepMetrics.PressureLevel() from the local RaftNode
//   2. If pressure changed by more than the threshold since the last push,
//      calls PushPressure() on the local HeliosKVBridge
//   3. The JVM receives it via POST /pressure, updates
//      ReplicationPressureReceiver, which CompactionManager reads on every
//      tick to scale its trigger thresholds
//
// Why 0.1 as the change threshold: pushing on every poll (every 500ms) would
// mean ~2 HTTP calls/second of constant overhead to the JVM even when nothing
// is changing. Only pushing on meaningful change (>10% shift in pressure)
// keeps the overhead near zero during stable operation while still reacting
// within one poll interval of a real pressure event.
type CompactionPressureLoop struct {
	node    *raft.RaftNode
	bridge  *HeliosKVBridge
	stopC   chan struct{}
	lastPressure float64
}

const (
	pressurePollInterval = 500 * time.Millisecond
	pressureChangeDelta  = 0.1 // only push if pressure changed by more than this
)

func NewCompactionPressureLoop(node *raft.RaftNode, bridge *HeliosKVBridge) *CompactionPressureLoop {
	return &CompactionPressureLoop{
		node:   node,
		bridge: bridge,
		stopC:  make(chan struct{}),
		lastPressure: -1.0, // force push on first tick
	}
}

func (l *CompactionPressureLoop) Start() {
	go l.run()
}

func (l *CompactionPressureLoop) Stop() {
	close(l.stopC)
}

func (l *CompactionPressureLoop) run() {
	ticker := time.NewTicker(pressurePollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-l.stopC:
			// Push 0.0 on shutdown so the JVM doesn't stay in a
			// high-pressure state after we disconnect
			l.bridge.PushPressure(0.0)
			return
		case <-ticker.C:
			pressure := l.node.RepMetrics.PressureLevel()
			if math.Abs(pressure-l.lastPressure) > pressureChangeDelta {
				if err := l.bridge.PushPressure(pressure); err != nil {
					fmt.Printf("[pressure-loop] push failed (will retry): %v\n", err)
					continue
				}
				l.lastPressure = pressure
			}
		}
	}
}
