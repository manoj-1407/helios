package bench

import (
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/manoj-1407/helios/raft"
)

// nullSM is a state machine that does nothing, so a Raft-only experiment has no
// engine, no JVM and no apply backlog in it.
type nullSM struct{}

func (nullSM) Apply(raft.LogEntry) error { return nil }
func (nullSM) Snapshot() ([]byte, error) { return nil, nil }
func (nullSM) Restore([]byte) error      { return nil }

type StallSpec struct {
	P float64       // probability that one Raft-log fsync stalls
	D time.Duration // stall length
}

// LoadSpec describes one Raft-only open-loop run. Roles ("L", "F1", "F2") are
// resolved once a leader exists: L is the leader and F1/F2 the other two nodes in
// id order. An injection stays with the node if leadership later moves.
type LoadSpec struct {
	Label            string
	Rate             float64 // offered writes per second
	Duration, Warmup time.Duration
	Delay            map[string]time.Duration
	Stall            map[string]StallSpec
	EnforceDeadlines bool // network-like RPC deadlines in the in-process transport
	MaxInFlight      int  // arrivals beyond this many unfinished writes are dropped (default 20000)
	MemLimitMB       int  // abort the run if the Go heap exceeds this (default 3000); the result is flagged MemAbort
	Seed             int64
}

type RoleStats struct {
	raft.Instr
	Syncs, SyncTotalNs, InjectedNs, Stalls, SyncMaxNs int64
}

type LoadResult struct {
	Spec        LoadSpec
	LeaderStart string
	LeaderEnd   string

	Offered, Issued, Dropped int
	GenSlipMaxNs             int64
	Measured                 int // committed after warmup
	Failed                   int // Propose refused (no leader / not leader / append error)
	Lost                     int // acknowledged, then a different entry committed at that index
	Unfinished               int // measured, neither committed nor lost 5 s after the last arrival
	AchievedPerSec           float64

	AckP50Ns, AckP99Ns                    int64
	CommitP50Ns, CommitP95Ns, CommitP99Ns int64
	CommitMaxNs                           int64
	MaxInFlight                           int
	NoLeaderFrac                          float64 // fraction of 20 ms samples with no leader
	TermStart, TermEnd                    uint64
	LeaderChanges, Elections, PreVotes    int64 // summed over nodes, excluding the initial election
	Role                                  map[string]RoleStats

	PeakHeapMB, PeakGoroutines int
	MemAbort                   bool // heap limit hit; the run was cut short and its latencies are not comparable
}

type loadOp struct {
	idx, term           uint64
	intended, ack, done time.Time
	measured, lost      bool
}

func RunRaftLoad(spec LoadSpec) (LoadResult, error) {
	res := LoadResult{Spec: spec, Role: map[string]RoleStats{}}
	if spec.MaxInFlight == 0 {
		spec.MaxInFlight = 20000
	}
	if spec.MemLimitMB == 0 {
		spec.MemLimitMB = 3000
	}
	transport := raft.NewChannelTransport()
	transport.SetEnforceDeadlines(spec.EnforceDeadlines)
	ids := []string{"n0", "n1", "n2"}
	nodes := map[string]*raft.RaftNode{}
	var dirs []string
	defer func() {
		for _, n := range nodes {
			n.Stop()
		}
		for _, d := range dirs {
			os.RemoveAll(d)
		}
	}()
	for _, id := range ids {
		var peers []string
		for _, o := range ids {
			if o != id {
				peers = append(peers, o)
			}
		}
		dir, err := os.MkdirTemp("", "helios-raftload-"+id+"-*")
		if err != nil {
			return res, err
		}
		dirs = append(dirs, dir)
		n, err := raft.NewRaftNode(id, peers, dir, transport, nullSM{}, raft.DefaultConfig())
		if err != nil {
			return res, err
		}
		transport.Register(n)
		nodes[id] = n
	}
	for _, n := range nodes {
		n.Start()
	}
	findLeader := func() *raft.RaftNode {
		for _, n := range nodes {
			if n.ObservedState() == raft.Leader {
				return n
			}
		}
		return nil
	}
	var leader *raft.RaftNode
	for dl := time.Now().Add(10 * time.Second); time.Now().Before(dl); time.Sleep(20 * time.Millisecond) {
		if leader = findLeader(); leader != nil {
			break
		}
	}
	if leader == nil {
		return res, fmt.Errorf("no leader elected")
	}
	time.Sleep(500 * time.Millisecond)
	if l := findLeader(); l != nil {
		leader = l
	}
	res.LeaderStart = leader.ID()

	var followers []*raft.RaftNode
	for _, id := range ids {
		if id != leader.ID() {
			followers = append(followers, nodes[id])
		}
	}
	roles := map[string]*raft.RaftNode{"L": leader, "F1": followers[0], "F2": followers[1]}
	for r, d := range spec.Delay {
		roles[r].SetFsyncDelay(d)
	}
	for r, s := range spec.Stall {
		roles[r].SetFsyncStall(s.P, s.D, uint64(spec.Seed)*7919+uint64(len(r)))
	}
	// baseline for "changes during the run": counters already include the first election
	base := map[string]raft.Instr{}
	for id, n := range nodes {
		base[id] = n.Instrumentation()
	}
	res.TermStart = leader.Term()

	var curLeader atomic.Pointer[raft.RaftNode]
	curLeader.Store(leader)
	var noLeader, samples atomic.Int64
	stopSample := make(chan struct{})
	sampleDone := make(chan struct{})
	go func() {
		defer close(sampleDone)
		t := time.NewTicker(20 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-stopSample:
				return
			case <-t.C:
				samples.Add(1)
				if l := findLeader(); l != nil {
					curLeader.Store(l)
				} else {
					noLeader.Add(1)
				}
			}
		}
	}()

	// resource guard: sample heap and goroutines; past the limit, stop generating.
	var memAbort atomic.Bool
	var peakHeap, peakG atomic.Int64
	stopMem := make(chan struct{})
	memDone := make(chan struct{})
	go func() {
		defer close(memDone)
		t := time.NewTicker(100 * time.Millisecond)
		defer t.Stop()
		var ms runtime.MemStats
		for {
			select {
			case <-stopMem:
				return
			case <-t.C:
				runtime.ReadMemStats(&ms)
				if h := int64(ms.HeapAlloc >> 20); h > peakHeap.Load() {
					peakHeap.Store(h)
				}
				if g := int64(runtime.NumGoroutine()); g > peakG.Load() {
					peakG.Store(g)
				}
				if int(ms.HeapAlloc>>20) > spec.MemLimitMB {
					memAbort.Store(true)
				}
			}
		}
	}()

	var mu sync.Mutex
	var ops, pending []*loadOp
	maxInFlight := 0
	stopWatch := make(chan struct{})
	watchDone := make(chan struct{})
	go func() {
		defer close(watchDone)
		for {
			select {
			case <-stopWatch:
				return
			default:
			}
			var bestC uint64
			for _, n := range nodes {
				if c := n.ObservedCommit(); c > bestC {
					bestC = c
				}
			}
			now := time.Now()
			mu.Lock()
			if len(pending) > maxInFlight {
				maxInFlight = len(pending)
			}
			keep := pending[:0]
			for _, p := range pending {
				if p.idx != 0 && p.idx <= bestC {
					// commit index only; whether it is *this* write that committed
					// is checked after the run (reclassify below), so the hot path
					// never reads the log or r.mu on a possibly stalled node.
					p.done = now
					continue
				}
				keep = append(keep, p)
			}
			pending = keep
			mu.Unlock()
			time.Sleep(200 * time.Microsecond)
		}
	}()

	rng := rand.New(rand.NewSource(spec.Seed))
	interval := time.Duration(float64(time.Second) / spec.Rate)
	start := time.Now()
	var wg sync.WaitGroup
	var inflight atomic.Int64
	for i := 0; ; i++ {
		intended := start.Add(time.Duration(i) * interval)
		if intended.Sub(start) >= spec.Duration {
			break
		}
		if memAbort.Load() {
			break
		}
		if d := time.Until(intended); d > 0 {
			time.Sleep(d)
		}
		if slip := time.Since(intended).Nanoseconds(); slip > res.GenSlipMaxNs {
			res.GenSlipMaxNs = slip
		}
		res.Offered++
		if inflight.Load() >= int64(spec.MaxInFlight) {
			res.Dropped++
			continue
		}
		res.Issued++
		cmd := []byte(fmt.Sprintf("k%d=v%d", rng.Intn(200), i))
		p := &loadOp{intended: intended, measured: intended.Sub(start) >= spec.Warmup}
		mu.Lock()
		ops = append(ops, p)
		mu.Unlock()
		inflight.Add(1)
		wg.Add(1)
		go func(p *loadOp, cmd []byte) {
			defer wg.Done()
			defer inflight.Add(-1)
			l := curLeader.Load()
			idx, term, ok := l.Propose(cmd)
			now := time.Now()
			mu.Lock()
			p.ack = now
			if ok {
				p.idx, p.term = idx, term
				pending = append(pending, p)
			} else {
				res.Failed++
			}
			mu.Unlock()
		}(p, cmd)
	}
	if !memAbort.Load() {
		wg.Wait()
	}
	for dl := time.Now().Add(5 * time.Second); time.Now().Before(dl); time.Sleep(20 * time.Millisecond) {
		mu.Lock()
		n := len(pending)
		mu.Unlock()
		if n == 0 {
			break
		}
	}
	close(stopWatch)
	<-watchDone
	close(stopSample)
	<-sampleDone
	close(stopMem)
	<-memDone
	res.PeakHeapMB, res.PeakGoroutines, res.MemAbort = int(peakHeap.Load()), int(peakG.Load()), memAbort.Load()

	if l := findLeader(); l != nil {
		res.LeaderEnd = l.ID()
		res.TermEnd = l.Term()
	}
	res.MaxInFlight = maxInFlight
	// clear injections, let the cluster settle, then check that each write the
	// watcher counted as committed is the entry that actually committed at its
	// index; if a different term's entry sits there the write was lost.
	for _, n := range nodes {
		n.SetFsyncDelay(0)
		n.SetFsyncStall(0, 0, 1)
	}
	time.Sleep(300 * time.Millisecond)
	for _, p := range ops {
		if p.idx == 0 || p.done.IsZero() {
			continue
		}
		for _, n := range nodes {
			if t, ok := n.CommitTermAt(p.idx); ok {
				if t != p.term {
					p.lost = true
				}
				break
			}
		}
	}
	if s := samples.Load(); s > 0 {
		res.NoLeaderFrac = float64(noLeader.Load()) / float64(s)
	}
	var ack, commit []int64
	for _, p := range ops {
		if !p.measured || p.idx == 0 {
			continue
		}
		switch {
		case p.lost:
			res.Lost++
		case p.done.IsZero():
			res.Unfinished++
		default:
			ack = append(ack, p.ack.Sub(p.intended).Nanoseconds())
			commit = append(commit, p.done.Sub(p.intended).Nanoseconds())
		}
	}
	sort.Slice(ack, func(i, j int) bool { return ack[i] < ack[j] })
	sort.Slice(commit, func(i, j int) bool { return commit[i] < commit[j] })
	res.Measured = len(commit)
	if w := (spec.Duration - spec.Warmup).Seconds(); w > 0 {
		res.AchievedPerSec = float64(res.Measured) / w
	}
	if len(commit) > 0 {
		res.AckP50Ns, res.AckP99Ns = percentile(ack, 50), percentile(ack, 99)
		res.CommitP50Ns, res.CommitP95Ns, res.CommitP99Ns = percentile(commit, 50), percentile(commit, 95), percentile(commit, 99)
		res.CommitMaxNs = commit[len(commit)-1]
	}
	for id, n := range nodes {
		in := n.Instrumentation()
		b := base[id]
		res.LeaderChanges += in.LeaderChanges - b.LeaderChanges
		res.Elections += in.ElectionsStarted - b.ElectionsStarted
		res.PreVotes += in.PreVotesStarted - b.PreVotesStarted
	}
	for role, n := range roles {
		c, tot, inj := n.FsyncStats()
		st, mx := n.FsyncDetail()
		res.Role[role] = RoleStats{Instr: n.Instrumentation(), Syncs: c, SyncTotalNs: tot, InjectedNs: inj, Stalls: st, SyncMaxNs: mx}
	}
	return res, nil
}
