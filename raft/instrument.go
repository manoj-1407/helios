package raft

import (
	"sync/atomic"
	"time"
)

// instrumentation for research/RESEARCH_AUDIT.md (r.mu coupling, election
// stability). none of it changes behaviour; it only records timings.

// bucket upper bounds in milliseconds; the last bucket is open-ended.
var waitBucketsMs = [...]int64{1, 5, 20, 50, 100, 200}

// WaitSnapshot is a copy of a waitStats at one moment.
type WaitSnapshot struct {
	Count, SumNs, MaxNs int64
	Buckets             [7]int64 // <1ms, <5, <20, <50, <100, <200, >=200
}

type waitStats struct {
	count, sumNs, maxNs atomic.Int64
	buckets             [7]atomic.Int64
}

func (w *waitStats) record(d time.Duration) {
	ns := d.Nanoseconds()
	w.count.Add(1)
	w.sumNs.Add(ns)
	for {
		m := w.maxNs.Load()
		if ns <= m || w.maxNs.CompareAndSwap(m, ns) {
			break
		}
	}
	ms := ns / 1e6
	i := 0
	for i < len(waitBucketsMs) && ms >= waitBucketsMs[i] {
		i++
	}
	w.buckets[i].Add(1)
}

func (w *waitStats) snapshot() WaitSnapshot {
	s := WaitSnapshot{Count: w.count.Load(), SumNs: w.sumNs.Load(), MaxNs: w.maxNs.Load()}
	for i := range s.Buckets {
		s.Buckets[i] = w.buckets[i].Load()
	}
	return s
}

// lockTimed acquires r.mu and records how long the acquisition waited.
func (r *RaftNode) lockTimed(w *waitStats) {
	t := time.Now()
	r.mu.Lock()
	w.record(time.Since(t))
}

// Instr groups the per-node counters. All fields are read-only snapshots.
type Instr struct {
	ProposeLockWait   WaitSnapshot // time Propose waited for r.mu
	ReplicateLockWait WaitSnapshot // time replicateToPeer waited for r.mu (start and response handling)
	HandleAELockWait  WaitSnapshot // time HandleAppendEntries waited for r.mu
	ProposeLockHold   WaitSnapshot // time Propose held r.mu (whole fsync in the original path)
	SendGap           WaitSnapshot // gap between successive AppendEntries sends to the same peer (leader view)
	AEGap             WaitSnapshot // gap between consecutive AppendEntries arrivals at this node (follower view)
	PreVotesStarted   int64
	ElectionsStarted  int64
	LeaderChanges     int64 // times this node became leader
	Term              uint64
}

func (r *RaftNode) Instrumentation() Instr {
	return Instr{
		ProposeLockWait:   r.inProposeWait.snapshot(),
		ReplicateLockWait: r.inReplicateWait.snapshot(),
		HandleAELockWait:  r.inHandleAEWait.snapshot(),
		ProposeLockHold:   r.inProposeHold.snapshot(),
		SendGap:           r.inSendGap.snapshot(),
		AEGap:             r.inAEGap.snapshot(),
		PreVotesStarted:   r.inPreVotes.Load(),
		ElectionsStarted:  r.inElections.Load(),
		LeaderChanges:     r.metrics.LeaderChanges.Load(),
		Term:              r.obsTerm.Load(),
	}
}

// noteAEArrival records the gap since the previous AppendEntries arrived here.
func (r *RaftNode) noteAEArrival() {
	now := time.Now().UnixNano()
	prev := r.inLastAE.Swap(now)
	if prev != 0 {
		r.inAEGap.record(time.Duration(now - prev))
	}
}

// ObservedState, ObservedTerm and ObservedCommit read lock-free mirrors that are
// updated next to every mutation under r.mu. They can lag the locked values by
// the few instructions between the mutation and the mirror store.
func (r *RaftNode) ObservedState() NodeState { return NodeState(r.obsState.Load()) }
func (r *RaftNode) ObservedTerm() uint64     { return r.obsTerm.Load() }
func (r *RaftNode) ObservedCommit() uint64   { return r.obsCommit.Load() }
