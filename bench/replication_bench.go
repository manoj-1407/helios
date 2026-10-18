package bench

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"sort"
	"sync"
	"time"

	"github.com/manoj-1407/helios/store"
)

type CompactionConfig struct {
	Name         string
	LeaderMode   string
	FollowerMode string
}

var ThesisConfigs = []CompactionConfig{
	{"adaptive_both",  "ADAPTIVE",      "ADAPTIVE"},
	{"mismatched",     "FORCE_LEVELED", "FORCE_TIERED"},
	{"static_leveled", "FORCE_LEVELED", "FORCE_LEVELED"},
}

type BenchResult struct {
	Config             CompactionConfig
	Ops                int
	DurationMs         int64
	OpsPerSec          float64
	P50ApplyLagNs      int64
	P99ApplyLagNs      int64
	MaxApplyLagNs      int64
	WriteAmplification float64
	CompactionSwitches int64
	LagAfterCompaction []int64
	ApplyErrors        int64
}

func (r BenchResult) Print() {
	fmt.Printf("\n=== %s ===\n", r.Config.Name)
	fmt.Printf("  leader=%-10s  follower=%s\n", r.Config.LeaderMode, r.Config.FollowerMode)
	fmt.Printf("  ops=%d  duration=%dms  ops/sec=%.0f\n", r.Ops, r.DurationMs, r.OpsPerSec)
	fmt.Printf("  apply_lag  p50=%dns  p99=%dns  max=%dns\n", r.P50ApplyLagNs, r.P99ApplyLagNs, r.MaxApplyLagNs)
	fmt.Printf("  write_amplification=%.4fx\n", r.WriteAmplification)
	fmt.Printf("  compaction_switches=%d  apply_errors=%d\n", r.CompactionSwitches, r.ApplyErrors)
	if len(r.LagAfterCompaction) > 0 {
		p99Post := percentile(r.LagAfterCompaction, 99)
		fmt.Printf("  lag_p99_post_compaction=%dns  (delta vs baseline=%+dns)\n",
			p99Post, p99Post-r.P99ApplyLagNs)
	}
}

func Run(cfg CompactionConfig, ops int, writeRatio float64) (BenchResult, error) {
	ports := []int{18080, 18081, 18082}
	jvms := make([]*exec.Cmd, 3)
	bridges := make([]*store.HeliosKVBridge, 3)
	dirs := make([]string, 3)

	// killAll stops all JVMs and waits for them to fully exit so the OS
	// reclaims their ports before the next config tries to bind them.
	// defer alone is not sufficient: deferred calls run after Run() returns,
	// but RunAll() calls Run() again immediately — the next config's JVMs
	// can start binding before the previous config's defers have fired.
	killAll := func() {
		for _, jvm := range jvms {
			if jvm != nil && jvm.Process != nil {
				jvm.Process.Kill()
				jvm.Wait()
			}
		}
		for _, dir := range dirs {
			if dir != "" {
				os.RemoveAll(dir)
			}
		}
	}

	for i, port := range ports {
		dir, err := os.MkdirTemp("", fmt.Sprintf("helios-bench-%s-n%d-*", cfg.Name, i))
		if err != nil {
			killAll()
			return BenchResult{}, err
		}
		dirs[i] = dir

		mode := cfg.FollowerMode
		if i == 0 {
			mode = cfg.LeaderMode
		}
		jvms[i] = startHeliosKV(dir, port, mode)
		bridges[i] = store.NewHeliosKVBridge(fmt.Sprintf("127.0.0.1:%d", port))
	}

	// Poll until all 3 JVMs are healthy.
	for i, b := range bridges {
		deadline := time.Now().Add(120 * time.Second)
		var lastErr error
		for time.Now().Before(deadline) {
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			lastErr = b.Health(ctx)
			cancel()
			if lastErr == nil {
				break
			}
			time.Sleep(300 * time.Millisecond)
		}
		if lastErr != nil {
			killAll()
			return BenchResult{}, fmt.Errorf("node%d never became healthy within 120s: %w", i, lastErr)
		}
	}

	cluster, err := NewBenchCluster(bridges)
	if err != nil {
		killAll()
		return BenchResult{}, fmt.Errorf("cluster init: %w", err)
	}
	defer cluster.Shutdown()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && cluster.Leader() == nil {
		time.Sleep(50 * time.Millisecond)
	}
	if cluster.Leader() == nil {
		killAll()
		return BenchResult{}, fmt.Errorf("no leader elected within 5s")
	}

	rng := rand.New(rand.NewSource(42))
	start := time.Now()

	var wg sync.WaitGroup
	var errCount int64
	var errMu sync.Mutex
	sem := make(chan struct{}, 32)

	for i := 0; i < ops; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(n int) {
			defer wg.Done()
			defer func() { <-sem }()

			key := fmt.Sprintf("key%d", rng.Intn(200))
			if rng.Float64() < writeRatio {
				cmd := store.EncodeCommand(store.Command{
					Type:  store.CmdPut,
					Key:   key,
					Value: fmt.Sprintf("v-%d-%d", n, time.Now().UnixNano()),
				})
				leader := cluster.Leader()
				if leader == nil {
					errMu.Lock()
					errCount++
					errMu.Unlock()
					return
				}
				_, _, ok := leader.Propose(cmd)
				if !ok {
					errMu.Lock()
					errCount++
					errMu.Unlock()
				}
			} else {
				ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
				defer cancel()
				bridges[0].Get(ctx, key)
			}
		}(i)
	}
	wg.Wait()
	duration := time.Since(start)

	var allLags []int64
	var lagAfterCompaction []int64
	var totalCompactionSwitches int64
	var followerWriteAmp float64

	for i, b := range bridges {
		totalCompactionSwitches += b.Metrics.CompactionEvents.Load()
		points := b.Metrics.Snapshot()
		var lastCompactionTs time.Time
		for _, p := range points {
			allLags = append(allLags, p.ReplicationLagNs)
			if p.CompactionEventSaw {
				lastCompactionTs = p.Timestamp
			}
			if !lastCompactionTs.IsZero() && p.Timestamp.Sub(lastCompactionTs) <= 500*time.Millisecond {
				lagAfterCompaction = append(lagAfterCompaction, p.ReplicationLagNs)
			}
		}
		if i == 1 {
			if stats, serr := b.FetchStats(); serr == nil {
				followerWriteAmp = stats.WriteAmplification
			}
		}
	}

	sort.Slice(allLags, func(i, j int) bool { return allLags[i] < allLags[j] })

	result := BenchResult{
		Config:             cfg,
		Ops:                ops,
		DurationMs:         duration.Milliseconds(),
		OpsPerSec:          float64(ops) / duration.Seconds(),
		WriteAmplification: followerWriteAmp,
		CompactionSwitches: totalCompactionSwitches,
		LagAfterCompaction: lagAfterCompaction,
		ApplyErrors:        errCount,
	}
	if len(allLags) > 0 {
		result.P50ApplyLagNs = percentile(allLags, 50)
		result.P99ApplyLagNs = percentile(allLags, 99)
		result.MaxApplyLagNs = allLags[len(allLags)-1]
	}
	killAll() // explicit kill+wait before returning so ports are free for the next config
	return result, nil
}

func RunAll(ops int) ([]BenchResult, error) {
	var results []BenchResult
	for _, cfg := range ThesisConfigs {
		fmt.Printf("[bench] %s (%d ops, 90%% writes)...\n", cfg.Name, ops)
		r, err := Run(cfg, ops, 0.9)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", cfg.Name, err)
		}
		results = append(results, r)
		r.Print()
		// 5s buffer after killAll has already waited for JVMs to fully exit
		if len(results) < len(ThesisConfigs) {
			fmt.Println("[bench] waiting 5s before next config...")
			time.Sleep(5 * time.Second)
		}
	}
	PrintSummary(results)
	return results, nil
}

// ThesisReport is the serialisable form of a full benchmark run. Write it to
// bench/results/thesis.json after collecting data so it can be committed and
// compared across runs. The Grafana dashboard can consume it via the
// SimpleJSON data source plugin if needed.
type ThesisReport struct {
	RunAt     string       `json:"run_at"`    // RFC3339
	GitCommit string       `json:"git_commit"` // HEAD at time of run, empty if unknown
	Ops       int          `json:"ops_per_config"`
	WriteRatio float64     `json:"write_ratio"`
	Results   []BenchResult `json:"results"`
}

// WriteJSON serialises results to a JSON file at the given path, creating
// parent directories as needed. The output is formatted for readability so
// it can be committed as-is without running through jq.
func WriteJSON(results []BenchResult, ops int, writeRatio float64, path string) error {
	if err := os.MkdirAll(filepath_dir(path), 0o755); err != nil {
		return fmt.Errorf("bench: mkdir %s: %w", filepath_dir(path), err)
	}

	// best-effort: grab the current git commit hash so the file is traceable
	gitHash := ""
	if out, err := os.ReadFile(".git/HEAD"); err == nil {
		ref := string(out)
		// HEAD may be a SHA (detached) or "ref: refs/heads/main\n"
		if len(ref) >= 40 {
			gitHash = ref[:40]
		} else if len(ref) > 5 && ref[:5] == "ref: " {
			refPath := ".git/" + trimNL(ref[5:])
			if sha, err2 := os.ReadFile(refPath); err2 == nil && len(sha) >= 40 {
				gitHash = string(sha[:40])
			}
		}
	}

	report := ThesisReport{
		RunAt:      time.Now().UTC().Format(time.RFC3339),
		GitCommit:  gitHash,
		Ops:        ops,
		WriteRatio: writeRatio,
		Results:    results,
	}
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return fmt.Errorf("bench: marshal results: %w", err)
	}
	if err := os.WriteFile(path, append(data, '\n'), 0o644); err != nil {
		return fmt.Errorf("bench: write %s: %w", path, err)
	}
	return nil
}

func filepath_dir(p string) string {
	for i := len(p) - 1; i >= 0; i-- {
		if p[i] == '/' {
			return p[:i]
		}
	}
	return "."
}

func trimNL(s string) string {
	for len(s) > 0 && (s[len(s)-1] == '\n' || s[len(s)-1] == '\r') {
		s = s[:len(s)-1]
	}
	return s
}

func PrintSummary(results []BenchResult) {
	fmt.Println("\n=== THESIS SUMMARY ===")
	fmt.Printf("%-20s  %10s  %12s  %12s  %10s  %8s\n",
		"config", "ops/sec", "p99_lag_ns", "p99_post_cmp", "write_amp", "cmp_sw")
	for _, r := range results {
		p99Post := int64(0)
		if len(r.LagAfterCompaction) > 0 {
			p99Post = percentile(r.LagAfterCompaction, 99)
		}
		fmt.Printf("%-20s  %10.0f  %12d  %12d  %10.4f  %8d\n",
			r.Config.Name, r.OpsPerSec, r.P99ApplyLagNs,
			p99Post, r.WriteAmplification, r.CompactionSwitches)
	}
	fmt.Println("\nThesis check:")
	fmt.Println("  adaptive_both  → expect lowest p99_lag and lowest p99_post_cmp")
	fmt.Println("  mismatched     → expect highest p99_lag under write-heavy load")
	fmt.Println("  static_leveled → moderate lag, highest write_amp")
	fmt.Println("  Document deviations honestly in README §'What the numbers mean'")
}

func startHeliosKV(dataDir string, port int, mode string) *exec.Cmd {
	// Jar lives at helios-kv/helios-kv.jar relative to the project root.
	// dev.helios.Main args: server <dataDir> <port> [COMPACTION_MODE]
	jar := "helios-kv/helios-kv.jar"
	cmd := exec.Command("java", "-jar", jar,
		"server", dataDir, fmt.Sprintf("%d", port), mode)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		panic(fmt.Sprintf("start helios-kv port=%d: %v", port, err))
	}
	return cmd
}

func percentile(sorted []int64, p int) int64 {
	if len(sorted) == 0 {
		return 0
	}
	return sorted[(len(sorted)-1)*p/100]
}
