// raftload runs raft-only open-loop experiments from a cells file.
//
// each non-comment line of the cells file is
//
//	label rate enforce delays stalls
//
// rate     offered writes/s (open loop)
// enforce  0 or 1: honour rpc deadlines in the in-process transport
// delays   "-" or ROLE=dur[,ROLE=dur]   constant fsync delay, e.g. F1=40ms,L=10ms
// stalls   "-" or ROLE=p/dur[,...]      intermittent stall, e.g. F1=0.05/300ms
//
// roles are L (leader after the initial election), F1 and F2 (the other nodes in
// id order). every rep runs every cell once in a seeded random order.
//
//	go run ./research/cmd/raftload -cells research/plans/x.cells -reps 10 -seed 1 > out.csv
package main

import (
	"bufio"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/manoj-1407/helios/bench"
)

type cell struct {
	label   string
	rate    float64
	enforce bool
	delays  map[string]time.Duration
	stalls  map[string]bench.StallSpec
	dtxt    string
	stxt    string
}

func parseCells(path string) ([]cell, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out []cell
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fs := strings.Fields(line)
		if len(fs) != 5 {
			return nil, fmt.Errorf("bad cell line %q (want 5 fields)", line)
		}
		rate, err := strconv.ParseFloat(fs[1], 64)
		if err != nil {
			return nil, err
		}
		c := cell{label: fs[0], rate: rate, enforce: fs[2] == "1", dtxt: fs[3], stxt: fs[4],
			delays: map[string]time.Duration{}, stalls: map[string]bench.StallSpec{}}
		if fs[3] != "-" {
			for _, kv := range strings.Split(fs[3], ",") {
				p := strings.SplitN(kv, "=", 2)
				d, err := time.ParseDuration(p[1])
				if err != nil {
					return nil, err
				}
				c.delays[p[0]] = d
			}
		}
		if fs[4] != "-" {
			for _, kv := range strings.Split(fs[4], ",") {
				p := strings.SplitN(kv, "=", 2)
				q := strings.SplitN(p[1], "/", 2)
				pr, err := strconv.ParseFloat(q[0], 64)
				if err != nil {
					return nil, err
				}
				d, err := time.ParseDuration(q[1])
				if err != nil {
					return nil, err
				}
				c.stalls[p[0]] = bench.StallSpec{P: pr, D: d}
			}
		}
		out = append(out, c)
	}
	return out, sc.Err()
}

func main() {
	cellsPath := flag.String("cells", "", "cells file")
	reps := flag.Int("reps", 10, "blocks; each block runs every cell once")
	dur := flag.Duration("dur", 20*time.Second, "arrival window per run")
	warm := flag.Duration("warmup", 4*time.Second, "arrivals in this window are not measured")
	seed := flag.Int64("seed", 1, "seed for block order and workload")
	flag.Parse()
	cells, err := parseCells(*cellsPath)
	if err != nil || len(cells) == 0 {
		fmt.Fprintln(os.Stderr, "cells:", err)
		os.Exit(2)
	}
	rng := rand.New(rand.NewSource(*seed))
	hdr := "rep,pos,label,rate,enforce,delays,stalls,dur_s,leader_start,leader_end,offered,issued,dropped,gen_slip_max_ms,measured,failed,lost,unfinished,achieved_per_s," +
		"ack_p50_ms,ack_p99_ms,commit_p50_ms,commit_p95_ms,commit_p99_ms,commit_max_ms,max_inflight,no_leader_frac,term_start,term_end,leader_changes,elections,prevotes"
	for _, r := range []string{"L", "F1", "F2"} {
		hdr += fmt.Sprintf(",%[1]s_syncs,%[1]s_injected_ms,%[1]s_stalls,%[1]s_sync_max_ms,%[1]s_propose_wait_max_ms,%[1]s_repl_wait_max_ms,%[1]s_handleae_wait_max_ms,%[1]s_handleae_wait_ge100,%[1]s_aegap_max_ms,%[1]s_aegap_ge100", r)
	}
	fmt.Println(hdr)
	ms := func(ns int64) float64 { return float64(ns) / 1e6 }
	for rep := 1; rep <= *reps; rep++ {
		for pos, idx := range rng.Perm(len(cells)) {
			c := cells[idx]
			r, err := bench.RunRaftLoad(bench.LoadSpec{Label: c.label, Rate: c.rate, Duration: *dur, Warmup: *warm,
				Delay: c.delays, Stall: c.stalls, EnforceDeadlines: c.enforce, Seed: *seed*1000 + int64(rep)})
			if err != nil {
				fmt.Fprintln(os.Stderr, "run failed:", err)
				os.Exit(1)
			}
			enf := 0
			if c.enforce {
				enf = 1
			}
			fmt.Printf("%d,%d,%s,%.0f,%d,%s,%s,%.0f,%s,%s,%d,%d,%d,%.2f,%d,%d,%d,%d,%.1f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%d,%.4f,%d,%d,%d,%d,%d",
				rep, pos+1, c.label, c.rate, enf, c.dtxt, c.stxt, dur.Seconds(), r.LeaderStart, r.LeaderEnd,
				r.Offered, r.Issued, r.Dropped, ms(r.GenSlipMaxNs), r.Measured, r.Failed, r.Lost, r.Unfinished, r.AchievedPerSec,
				ms(r.AckP50Ns), ms(r.AckP99Ns), ms(r.CommitP50Ns), ms(r.CommitP95Ns), ms(r.CommitP99Ns), ms(r.CommitMaxNs),
				r.MaxInFlight, r.NoLeaderFrac, r.TermStart, r.TermEnd, r.LeaderChanges, r.Elections, r.PreVotes)
			for _, role := range []string{"L", "F1", "F2"} {
				s := r.Role[role]
				fmt.Printf(",%d,%.0f,%d,%.2f,%.2f,%.2f,%.2f,%d,%.2f,%d", s.Syncs, ms(s.InjectedNs), s.Stalls, ms(s.SyncMaxNs),
					ms(s.ProposeLockWait.MaxNs), ms(s.ReplicateLockWait.MaxNs), ms(s.HandleAELockWait.MaxNs),
					s.HandleAELockWait.Buckets[5]+s.HandleAELockWait.Buckets[6],
					ms(s.AEGap.MaxNs), s.AEGap.Buckets[5]+s.AEGap.Buckets[6])
			}
			fmt.Println()
			fmt.Fprintf(os.Stderr, "[raftload] rep %d pos %d %s done\n", rep, pos+1, c.label)
			time.Sleep(500 * time.Millisecond)
		}
	}
}
