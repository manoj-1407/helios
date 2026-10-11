package ebpf

import (
	"sort"
	"sync"
	"time"
)

const (
	postCompactionWindowNs = 500 * time.Millisecond
	spikeThresholdMultiple = 3.0
	rollingWindowSize      = 100 // last N events per PID for baseline P50
)

// Correlator receives raw TraceEvents from the ring buffer and
// CompactionEvents from the bridge layer, and produces CorrelatedEvents
// that are the actual thesis measurement: "did this fsync/write spike
// happen in the post-compaction window, and by how much?"
//
// This is the layer that separates "we measured syscall latency" from
// "we proved that compaction events cause replication lag spikes." Without
// this join, you have two independent time series. With it, you have a
// causal claim with kernel-level timestamps on both sides.
type Correlator struct {
	mu              sync.Mutex
	compactionEvents []CompactionEvent // sorted by timestamp, newest last
	latencyHistory   map[uint32][]uint64 // pid -> rolling window of durations
	outputC         chan<- CorrelatedEvent
	pidNames        map[uint32]string
}

func NewCorrelator(outputC chan<- CorrelatedEvent) *Correlator {
	return &Correlator{
		latencyHistory: make(map[uint32][]uint64),
		outputC:        outputC,
		pidNames:       make(map[uint32]string),
	}
}

// RegisterProcess names a PID so output events carry a human-readable
// process name instead of a bare number.
func (c *Correlator) RegisterProcess(pid uint32, name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pidNames[pid] = name
}

// RecordCompaction tells the correlator that a compaction event happened
// at this timestamp on this PID. Called by the bridge's BridgeMetrics
// when it detects a strategy change via /stats.
func (c *Correlator) RecordCompaction(ev CompactionEvent) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.compactionEvents = append(c.compactionEvents, ev)
}

// Process takes a raw TraceEvent from the ring buffer, correlates it
// against the most recent compaction event for the same PID (or any PID,
// since on a single machine a compaction on the JVM process affects the
// same disk as the Raft Go process), computes baseline anomaly detection,
// and emits a CorrelatedEvent if the event is interesting.
//
// "Interesting" means: either it's in the post-compaction window, or its
// duration is more than 3x the rolling P50 for this PID. The 3x threshold
// is intentionally generous — this is exploratory measurement, not alerting.
func (c *Correlator) Process(e TraceEvent) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Update rolling latency history for baseline computation.
	hist := c.latencyHistory[e.Pid]
	hist = append(hist, e.DurationNs)
	if len(hist) > rollingWindowSize {
		hist = hist[len(hist)-rollingWindowSize:]
	}
	c.latencyHistory[e.Pid] = hist

	baseline := rollingP50(hist)
	isSpike := baseline > 0 && float64(e.DurationNs) > spikeThresholdMultiple*float64(baseline)

	// Find the most recent compaction event before this trace event.
	msSince := float64(-1)
	isPostCmp := false
	for i := len(c.compactionEvents) - 1; i >= 0; i-- {
		cmp := c.compactionEvents[i]
		if cmp.TimestampNs <= e.TimestampNs {
			delta := time.Duration(e.TimestampNs - cmp.TimestampNs)
			msSince = float64(delta.Milliseconds())
			isPostCmp = delta <= postCompactionWindowNs
			break
		}
	}

	// Only emit events that are actually interesting — otherwise the
	// output channel fills with noise from routine fast syscalls.
	if !isPostCmp && !isSpike {
		return
	}

	name := c.pidNames[e.Pid]
	if name == "" {
		name = "unknown"
	}

	out := CorrelatedEvent{
		TraceEvent:        e,
		ProcessName:       name,
		MsSinceCompaction: msSince,
		IsPostCompaction:  isPostCmp,
		BaselineP50Ns:     baseline,
		IsLatencySpike:    isSpike,
	}

	select {
	case c.outputC <- out:
	default:
		// output channel full — drop rather than block
	}
}

func rollingP50(durations []uint64) uint64 {
	if len(durations) == 0 {
		return 0
	}
	sorted := make([]uint64, len(durations))
	copy(sorted, durations)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	return sorted[len(sorted)/2]
}
