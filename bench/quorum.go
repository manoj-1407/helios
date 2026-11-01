package bench

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"sort"
	"sync"
	"time"

	"github.com/manoj-1407/helios/raft"
	"github.com/manoj-1407/helios/store"
)

// QuorumSpec describes one quorum-masking run: which roles get an injected
// Raft-log fsync delay, an open-loop write rate, and a duration.
// Roles are resolved after the cluster elects a leader: "L" is the leader,
// "F1" and "F2" are the other two nodes in id order.
type QuorumSpec struct {
	Label    string
	SlowRole []string // subset of "L","F1","F2"
	Delay    time.Duration
	Rate     float64 // offered writes per second, open loop
	Duration time.Duration
	Warmup   time.Duration // arrivals in this window are not measured
	Seed     int64
}

type QuorumResult struct {
	Spec        QuorumSpec
	LeaderStart string
	LeaderEnd   string
	Offered     int
	Measured    int // committed writes arriving after warmup
	Failed      int // proposals rejected (not leader / append error)
	Unfinished  int // measured writes not committed within 10s of the run end
	AckP50Ns    int64
	AckP99Ns    int64
	CommitP50Ns int64
	CommitP95Ns int64
	CommitP99Ns int64
	CommitMaxNs int64
	MaxInFlight int
	BacklogP99  int64 // pooled CommitIndex-LastApplied samples, all nodes
	BacklogMax  int64
	// per role: raft-log fsync count, total sync wall time, injected delay
	Fsync map[string][3]int64
}

type pendingOp struct {
	idx       uint64
	intended  time.Time
	ack       time.Time
	committed time.Time
	measured  bool
}

// RunQuorum runs one open-loop experiment on a 3-node in-process cluster with
// three helios-kv JVMs (engine in FORCE_LEVELED; the legacy small workload never
// compacts, so compaction is not a variable here). Latencies are measured from
// the intended send time of each arrival, so a slow system cannot slow the
// offered load or hide queueing (coordinated omission).
func RunQuorum(spec QuorumSpec) (QuorumResult, error) {
	res := QuorumResult{Spec: spec, Fsync: map[string][3]int64{}}
	ports := []int{18080, 18081, 18082}
	jvms := make([]*exec.Cmd, 3)
	bridges := make([]*store.HeliosKVBridge, 3)
	dirs := make([]string, 3)
	killAll := func() {
		for _, j := range jvms {
			if j != nil && j.Process != nil {
				j.Process.Kill()
				j.Wait()
			}
		}
		for _, d := range dirs {
			if d != "" {
				os.RemoveAll(d)
			}
		}
	}
	for i, port := range ports {
		dir, err := os.MkdirTemp("", fmt.Sprintf("helios-quorum-n%d-*", i))
		if err != nil {
			killAll()
			return res, err
		}
		dirs[i] = dir
		jvms[i] = startHeliosKV(dir, port, "FORCE_LEVELED")
		bridges[i] = store.NewHeliosKVBridge(fmt.Sprintf("127.0.0.1:%d", port))
	}
	defer killAll()
	if err := waitHealthy(bridges); err != nil {
		return res, err
	}
	cluster, err := NewBenchCluster(bridges)
	if err != nil {
		return res, err
	}
	defer cluster.Shutdown()

	var leader *raft.RaftNode
	for dl := time.Now().Add(10 * time.Second); time.Now().Before(dl); time.Sleep(50 * time.Millisecond) {
		if leader = cluster.Leader(); leader != nil {
			break
		}
	}
	if leader == nil {
		return res, fmt.Errorf("no leader elected")
	}
	time.Sleep(500 * time.Millisecond) // let the first heartbeats settle before injecting
	res.LeaderStart = leader.ID()

	roles := map[string]*raft.RaftNode{"L": leader}
	var followers []*raft.RaftNode
	for _, n := range cluster.AllNodes() {
		if n.ID() != leader.ID() {
			followers = append(followers, n)
		}
	}
	sort.Slice(followers, func(i, j int) bool { return followers[i].ID() < followers[j].ID() })
	roles["F1"], roles["F2"] = followers[0], followers[1]
	for _, r := range spec.SlowRole {
		roles[r].SetFsyncDelay(spec.Delay)
	}

	// commit watcher: one goroutine stamps committed ops, so thousands of
	// in-flight arrivals do not each poll the leader's mutex.
	var mu sync.Mutex
	var ops []*pendingOp
	var pending []*pendingOp
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
			c := leader.CommitIndex()
			now := time.Now()
			mu.Lock()
			if len(pending) > maxInFlight {
				maxInFlight = len(pending)
			}
			keep := pending[:0]
			for _, p := range pending {
				if p.idx != 0 && p.idx <= c {
					p.committed = now
				} else {
					keep = append(keep, p)
				}
			}
			pending = keep
			mu.Unlock()
			time.Sleep(100 * time.Microsecond)
		}
	}()

	// backlog sampler: CommitIndex-LastApplied per node every 100 ms
	var backlog []int64
	stopSample := make(chan struct{})
	sampleDone := make(chan struct{})
	go func() {
		defer close(sampleDone)
		t := time.NewTicker(100 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-stopSample:
				return
			case <-t.C:
				for _, n := range cluster.AllNodes() {
					c, a := n.CommitIndex(), n.LastApplied()
					if c >= a {
						backlog = append(backlog, int64(c-a))
					}
				}
			}
		}
	}()

	// open-loop arrivals at fixed intervals
	rng := rand.New(rand.NewSource(spec.Seed))
	interval := time.Duration(float64(time.Second) / spec.Rate)
	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; ; i++ {
		intended := start.Add(time.Duration(i) * interval)
		if intended.Sub(start) >= spec.Duration {
			break
		}
		if d := time.Until(intended); d > 0 {
			time.Sleep(d)
		}
		cmd := store.EncodeCommand(store.Command{Type: store.CmdPut, Key: fmt.Sprintf("key%d", rng.Intn(200)), Value: fmt.Sprintf("v-%d", i)})
		p := &pendingOp{intended: intended, measured: intended.Sub(start) >= spec.Warmup}
		mu.Lock()
		ops = append(ops, p)
		mu.Unlock()
		wg.Add(1)
		go func(p *pendingOp, cmd []byte) {
			defer wg.Done()
			idx, _, ok := leader.Propose(cmd)
			now := time.Now()
			mu.Lock()
			p.ack = now
			if ok {
				p.idx = idx
				pending = append(pending, p)
			} else {
				res.Failed++
			}
			mu.Unlock()
		}(p, cmd)
	}
	res.Offered = len(ops)
	wg.Wait()

	// let in-flight writes finish (up to 10 s) before stopping the watcher
	for dl := time.Now().Add(10 * time.Second); time.Now().Before(dl); time.Sleep(20 * time.Millisecond) {
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
	if l := cluster.Leader(); l != nil {
		res.LeaderEnd = l.ID()
	}
	res.MaxInFlight = maxInFlight

	var ack, commit []int64
	for _, p := range ops {
		if !p.measured || p.idx == 0 {
			continue
		}
		if p.committed.IsZero() {
			res.Unfinished++
			continue
		}
		ack = append(ack, p.ack.Sub(p.intended).Nanoseconds())
		commit = append(commit, p.committed.Sub(p.intended).Nanoseconds())
	}
	sort.Slice(ack, func(i, j int) bool { return ack[i] < ack[j] })
	sort.Slice(commit, func(i, j int) bool { return commit[i] < commit[j] })
	res.Measured = len(commit)
	if len(commit) > 0 {
		res.AckP50Ns, res.AckP99Ns = percentile(ack, 50), percentile(ack, 99)
		res.CommitP50Ns, res.CommitP95Ns, res.CommitP99Ns = percentile(commit, 50), percentile(commit, 95), percentile(commit, 99)
		res.CommitMaxNs = commit[len(commit)-1]
	}
	sort.Slice(backlog, func(i, j int) bool { return backlog[i] < backlog[j] })
	if len(backlog) > 0 {
		res.BacklogP99, res.BacklogMax = percentile(backlog, 99), backlog[len(backlog)-1]
	}
	for name, n := range roles {
		c, t, inj := n.FsyncStats()
		res.Fsync[name] = [3]int64{c, t, inj}
	}
	return res, nil
}

func waitHealthy(bridges []*store.HeliosKVBridge) error {
	for i, b := range bridges {
		var last error
		for dl := time.Now().Add(120 * time.Second); time.Now().Before(dl); time.Sleep(300 * time.Millisecond) {
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			last = b.Health(ctx)
			cancel()
			if last == nil {
				break
			}
		}
		if last != nil {
			return fmt.Errorf("node%d never became healthy: %w", i, last)
		}
	}
	return nil
}

