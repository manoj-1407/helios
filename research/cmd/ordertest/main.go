// ordertest checks whether the benchmark's headline metric (ops/sec) tracks the
// compaction config or the position of the run in the sequence.
//
// phase "same":  one identical config, repeated -reps times back to back.
// phase "perms": all 6 orderings of the 3 thesis configs, so every config lands
//                in every position exactly twice.
//
// Run from a directory containing helios-kv/helios-kv.jar, see REPRODUCIBILITY.md.
// Output is CSV on stdout; progress goes to stderr.
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/manoj-1407/helios/bench"
)

var perms = [][]int{{0, 1, 2}, {0, 2, 1}, {1, 0, 2}, {1, 2, 0}, {2, 0, 1}, {2, 1, 0}}

func main() {
	ops := flag.Int("ops", 15000, "ops per run")
	reps := flag.Int("reps", 6, "repeats in the identical-config phase")
	regime := flag.String("regime", "unset", "label written to the CSV (e.g. default or nodelay)")
	flag.Parse()

	fmt.Println("regime,phase,run,position,config,ops_per_sec,p50_ms,p99_ms,propose_failures,commit_p50_ms,commit_p99_ms,ack_p99_ms,commit_timeouts")
	run := 0
	emit := func(phase string, pos int, cfg bench.CompactionConfig) {
		r, err := bench.Run(cfg, *ops, 0.9)
		if err != nil {
			fmt.Fprintln(os.Stderr, "run failed:", err)
			os.Exit(1)
		}
		run++
		fmt.Printf("%s,%s,%d,%d,%s,%.0f,%.2f,%.2f,%d,%.2f,%.2f,%.2f,%d\n", *regime, phase, run, pos, cfg.Name,
			r.OpsPerSec, float64(r.P50ApplyLagNs)/1e6, float64(r.P99ApplyLagNs)/1e6, r.ApplyErrors,
			float64(r.CommitP50Ns)/1e6, float64(r.CommitP99Ns)/1e6, float64(r.AckP99Ns)/1e6, r.CommitTimeouts)
		fmt.Fprintf(os.Stderr, "[%s] %s run %d done\n", *regime, phase, run)
		time.Sleep(5 * time.Second)
	}

	same := bench.ThesisConfigs[2] // static_leveled
	for i := 0; i < *reps; i++ {
		emit("same", i+1, same)
	}
	for _, p := range perms {
		for pos, idx := range p {
			emit("perms", pos+1, bench.ThesisConfigs[idx])
		}
	}
}
