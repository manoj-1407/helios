# pre-registration: fsync inside vs outside the leader's locks

written and committed before any run of the plan below (see git history for the order). code state: the commit that adds `Config.AppendOutsideLock`.

## question

does the leader's fsync running while `r.mu` (and the log's own mutex) are held cause the elections seen at leader utilisation near and above 1.0, in this implementation (Helios Raft, in-process 3 nodes, `ChannelTransport` with 100 ms RPC deadlines enforced)?

## intervention (only this changes between arms)

`AppendOutsideLock=false` (baseline): `Propose` holds `r.mu` and `RaftLog.mu` across `Append`, which fsyncs.
`AppendOutsideLock=true`: `Propose` chooses index/term and writes the entry under `r.mu` exactly as before, releases `r.mu`, then calls `RaftLog.SyncDurable()` (one fsync at a time, `RaftLog.mu` not held), then re-takes `r.mu`, checks the entry is still in the log with the same term, advances commit if still leader, and returns. Safety rules in the new arm: (1) log writes and index assignment stay serialised under `r.mu`; (2) `Propose` returns ok only after its entry is durable and still present; (3) replication ships only entries at or below the durable index, so no follower sees an entry before the leader has it on disk, same observable order as baseline; (4) the leader counts its own copy toward a majority only for indexes at or below the durable index; (5) a truncation bumps an epoch so a sync that began earlier cannot mark later entries durable; (6) follower-side `HandleAppendEntries` is unchanged. fsync service capacity is unchanged (one fsync per proposal, serialised), so rho = rate x (delay + ~0.3 ms) means the same in both arms.
tests: `raft/lockscope_test.go` (ordering, durable-before-ack, no replication of non-durable entries, heartbeat path unblocked during fsync only in the new arm, truncate/epoch, reopen), plus the existing suite under `-race`.

attribution limit: the intervention moves fsync out of two locks at once (`r.mu` and `RaftLog.mu`), because moving it out of `r.mu` alone leaves `log.Get` in the heartbeat path blocked on `RaftLog.mu`. a result therefore attributes the effect to "fsync under the two locks", not to `r.mu` alone.

## design

leader fsync delay constant 10 ms on role L (primary). followers undelayed. open-loop Poisson arrivals, seed = 11000 + rep, identical in both arms of a rep (paired). 20 s arrival window, 4 s warmup, same runner as stages 1 to 6, one process per run, cells in a seeded random order inside each rep, no other load on the host.

| cell | rate /s | rho = rate x 10.3 ms |
|---|---|---|
| ctl | 48 | no delay, healthy control |
| r050 | 48 | 0.50 |
| r090 | 87 | 0.90 |
| r100 | 97 | 1.00 |
| r120 | 116 | 1.20 |

each of the 5 cells in both arms = 10 cells. 30 paired reps per cell (300 runs), more if the primary interval is inconclusive. secondary replication at 25 ms leader delay (rates 20, 36, 40, 48) is run only after the primary analysis is written, labelled secondary.

## primary outcome

a run is *disrupted* if the term advanced during the load window (`term_end > term_start`). primary measure: the fraction of runs disrupted, per cell and arm, with an exact McNemar test on the 30 pairs.

## success criterion (confirmatory cells: r100 and r120)

the mechanism is supported if, in both r100 and r120, the new arm's disrupted fraction is at least 30 percentage points lower than baseline and the exact two-sided McNemar p is below 0.025 (Bonferroni over 2 cells). the mechanism is not supported if the new arm is within 15 points of baseline in either cell. anything between is reported as inconclusive and I add reps (up to 60) before concluding.

## no-harm criteria (must all hold, else the intervention is not accepted regardless of the above)

- safety: in every run, in both arms, with no term change (`term_end == term_start`), `lost` must be 0; and in the new arm no acknowledged index may ever hold a different entry than the one acked on any node at the end of a run (the runner's `lost` count). (see amendment 1.)
- r050, r090 and ctl: new arm disrupted fraction not more than 10 points above baseline.
- achieved rate and commit p99 in r050/r090 not worse than baseline by more than the paired 95% interval allows (reported, not gated, but a regression is stated).

## secondary outcomes (descriptive, paired differences with 95% intervals; not gated)

commit p50/p95/p99, achieved throughput, failed proposals, peak heap and goroutines, max and summed `Propose` lock hold and lock wait, replicate lock wait, leader send-gap (max, count >= 50 ms and >= 100 ms), follower AppendEntries arrival gap, injected-delay check (syncs x delay).

## handling rules

- baseline elections are reported, in every cell, in both arms. nothing is excluded for having an election.
- crashed or timed-out runs are listed; a pair with a missing run is dropped from the McNemar test and counted. memory aborts count as disrupted. if more than 5% of runs are missing I report a worst-case sensitivity.
- the stage 5 observation (elections in 9/10 baseline runs at rho 1.0, 0/10 at 0.9) is the prior; stage 1 saw elections near 0.9. I expect the baseline to show elections at r100 and r120 and few at r050/r090, but baseline noise of a few percent exists (3.9% no-fault in earlier stages).

## what this does not test

compaction, quorum masking, the ack-semantics correctness finding (`/kv/put` acks local durable append, not commit), follower-side fsync locks, a real disk, a real network. the correctness and performance findings stay separate.

## amendments

1. 2026-11-06, during the primary batch, after the first 3 reps had been written and before any analysis. the no-harm line originally read "`lost` = 0 in every run of both arms". that was wrong as a criterion: when a leader is deposed, entries it acked after local durable append can be overwritten, which is the separate ack-semantics finding, and the baseline arm showed this in its 3 election runs at r100 (lost 8, 64, 39). the criterion is restated above as lost = 0 in any run without a term change, plus lost counts reported per arm. primary outcome, success criterion and cells are unchanged.
