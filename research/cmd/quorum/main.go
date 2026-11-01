// quorum runs the quorum-masking experiment (research/EXPERIMENT_DESIGN.md E1):
// inject a Raft-log fsync delay on chosen roles of a 3-node cluster and measure
// open-loop commit latency. every rep (block) contains every cell once, in a
// seeded random order. csv on stdout, progress on stderr.
//
//	go build -o quorum ./research/cmd/quorum   # run from a dir with helios-kv/helios-kv.jar
//	./quorum -rate 15 -dur 40s -warmup 5s -reps 4 -seed 1 > out.csv
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

type cell struct {
	label string
	slow  []string
	delay time.Duration
}

func main() {
	rate := flag.Float64("rate", 15, "offered writes/s, open loop")
	dur := flag.Duration("dur", 40*time.Second, "arrival window per run")
	warm := flag.Duration("warmup", 5*time.Second, "arrivals in this window are not measured")
	reps := flag.Int("reps", 4, "blocks; each block runs every cell once")
	seed := flag.Int64("seed", 1, "seed for block order and keys")
	delays := flag.String("delays", "10ms,40ms", "injected delays for non-baseline cells")
	flag.Parse()

	cells := []cell{{"none", nil, 0}}
	var ds []time.Duration
	for _, s := range strings.Split(*delays, ",") {
		d, err := time.ParseDuration(strings.TrimSpace(s))
		if err != nil {
			fmt.Fprintln(os.Stderr, "bad delay:", s)
			os.Exit(2)
		}
		ds = append(ds, d)
	}
	for _, d := range ds {
		cells = append(cells,
			cell{"F1", []string{"F1"}, d},
			cell{"L", []string{"L"}, d},
			cell{"L+F1", []string{"L", "F1"}, d},
			cell{"F1+F2", []string{"F1", "F2"}, d})
	}

	rng := rand.New(rand.NewSource(*seed))
	fmt.Println("rep,position,slow,delay_ms,rate,leader_start,leader_end,offered,measured,failed,unfinished,ack_p50_ms,ack_p99_ms,commit_p50_ms,commit_p95_ms,commit_p99_ms,commit_max_ms,max_inflight,backlog_p99,backlog_max,L_syncs,L_injected_ms,F1_syncs,F1_injected_ms,F2_syncs,F2_injected_ms,L_mean_sync_ms,F1_mean_sync_ms,F2_mean_sync_ms")
	for rep := 1; rep <= *reps; rep++ {
		for pos, idx := range rng.Perm(len(cells)) {
			c := cells[idx]
			r, err := bench.RunQuorum(bench.QuorumSpec{Label: c.label, SlowRole: c.slow, Delay: c.delay, Rate: *rate, Duration: *dur, Warmup: *warm, Seed: *seed + int64(rep)})
			if err != nil {
				fmt.Fprintln(os.Stderr, "run failed:", err)
				os.Exit(1)
			}
			ms := func(ns int64) float64 { return float64(ns) / 1e6 }
			mean := func(role string) float64 {
				f := r.Fsync[role]
				if f[0] == 0 {
					return 0
				}
				return float64(f[1]) / float64(f[0]) / 1e6
			}
			fmt.Printf("%d,%d,%s,%.0f,%.0f,%s,%s,%d,%d,%d,%d,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%d,%d,%d,%d,%.0f,%d,%.0f,%d,%.0f,%.2f,%.2f,%.2f\n",
				rep, pos+1, c.label, float64(c.delay)/1e6, *rate, r.LeaderStart, r.LeaderEnd,
				r.Offered, r.Measured, r.Failed, r.Unfinished,
				ms(r.AckP50Ns), ms(r.AckP99Ns), ms(r.CommitP50Ns), ms(r.CommitP95Ns), ms(r.CommitP99Ns), ms(r.CommitMaxNs),
				r.MaxInFlight, r.BacklogP99, r.BacklogMax,
				r.Fsync["L"][0], ms(r.Fsync["L"][2]), r.Fsync["F1"][0], ms(r.Fsync["F1"][2]), r.Fsync["F2"][0], ms(r.Fsync["F2"][2]),
				mean("L"), mean("F1"), mean("F2"))
			fmt.Fprintf(os.Stderr, "[quorum] rep %d pos %d %s %v done\n", rep, pos+1, c.label, c.delay)
			time.Sleep(3 * time.Second)
		}
	}
}
