// calibrun runs the replication bench on a workload that really compacts, over
// several compaction configs, in randomized blocks (every rep contains every
// config once, in a seeded random order). csv on stdout, progress on stderr.
//
//	go build -o calibrun ./research/cmd/calibrun   # run from a dir with helios-kv/helios-kv.jar
//	./calibrun -ops 20000 -keys 100000 -valbytes 1000 -reps 5 -seed 1 > out.csv
package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/manoj-1407/helios/bench"
)

var configs = []bench.CompactionConfig{
	{Name: "static_leveled", LeaderMode: "FORCE_LEVELED", FollowerMode: "FORCE_LEVELED"},
	{Name: "static_tiered", LeaderMode: "FORCE_TIERED", FollowerMode: "FORCE_TIERED"},
	{Name: "no_compaction", LeaderMode: "DISABLED", FollowerMode: "DISABLED"},
	{Name: "adaptive_both", LeaderMode: "ADAPTIVE", FollowerMode: "ADAPTIVE"},
	{Name: "mixed_node0_leveled_others_tiered", LeaderMode: "FORCE_LEVELED", FollowerMode: "FORCE_TIERED"},
}

func main() {
	ops := flag.Int("ops", 20000, "ops per run")
	keys := flag.Int("keys", 100000, "key space")
	valBytes := flag.Int("valbytes", 1000, "value size in bytes (0 = legacy short value)")
	reps := flag.Int("reps", 5, "blocks; each block runs every config once")
	seed := flag.Int64("seed", 1, "seed for block order")
	only := flag.String("only", "", "comma-separated config names (default all)")
	wr := flag.Float64("wr", 0.9, "write ratio")
	flag.Parse()

	bench.Keys, bench.ValueBytes = *keys, *valBytes
	var cs []bench.CompactionConfig
	for _, c := range configs {
		if *only == "" || strings.Contains(","+*only+",", ","+c.Name+",") {
			cs = append(cs, c)
		}
	}
	rng := rand.New(rand.NewSource(*seed))
	fmt.Println("rep,position,config,ops,keys,valbytes,ops_per_sec,commit_p50_ms,commit_p95_ms,commit_p99_ms,ack_p99_ms,commit_timeouts,propose_failures,bridge_apply_errs,drain_s,applied_frac,flushes,compactions,tables_compacted,compaction_busy_ms,max_sstables,logical_mb,flush_mb,compaction_mb,write_amp_engine")
	for rep := 1; rep <= *reps; rep++ {
		order := rng.Perm(len(cs))
		for pos, idx := range order {
			cfg := cs[idx]
			r, err := bench.Run(cfg, *ops, *wr)
			if err != nil {
				fmt.Fprintln(os.Stderr, "run failed:", err)
				os.Exit(1)
			}
			wa := 0.0
			if r.LogicalBytes > 0 {
				wa = float64(r.FlushBytes+r.CompactionBytes) / float64(r.LogicalBytes)
			}
			ms := func(ns int64) float64 { return float64(ns) / 1e6 }
			mb := func(b int64) float64 { return float64(b) / 1e6 }
			fmt.Printf("%d,%d,%s,%d,%d,%d,%.0f,%.2f,%.2f,%.2f,%.2f,%d,%d,%d,%.1f,%.4f,%d,%d,%d,%d,%d,%.1f,%.1f,%.1f,%.3f\n",
				rep, pos+1, cfg.Name, *ops, *keys, *valBytes, r.OpsPerSec,
				ms(r.CommitP50Ns), ms(r.CommitP95Ns), ms(r.CommitP99Ns), ms(r.AckP99Ns),
				r.CommitTimeouts, r.ApplyErrors, r.BridgeApplyErrs, r.DrainSeconds, r.AppliedFraction,
				r.Flushes, r.Compactions, r.TablesCompacted, r.CompactionBusyMs, r.MaxSSTables,
				mb(r.LogicalBytes), mb(r.FlushBytes), mb(r.CompactionBytes), wa)
			fmt.Fprintf(os.Stderr, "[calibrun] rep %d pos %d %s done\n", rep, pos+1, cfg.Name)
			time.Sleep(5 * time.Second)
		}
	}
}
