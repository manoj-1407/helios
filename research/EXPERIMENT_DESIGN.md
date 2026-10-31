# Experiment design

Nothing below has been run yet except the audit experiments in `research/results/`. This is the plan, written so each experiment can fail.

## Stage 0: make the measurements mean something

These are blockers. No result from the current benchmark should be quoted until they are done. Each maps to a finding in `RESEARCH_AUDIT.md`.

| id | change | fixes |
|---|---|---|
| M1 | Enable TCP_NODELAY in `HeliosServer` (set `sun.net.httpserver.nodelay` before creating the server), or send headers and body in one write. | F3 |
| M2 | Workload that really compacts: at least 10^6 keys, Zipf and uniform variants, 1 KiB values, memtable 4 MiB (the engine default). Verify with `sstable_count` and a compaction counter before using any number. | F4 |
| M3 | Clients that wait for commit (`WaitCommitted(index)` on the leader) and record t0, t1, t3 per op. Open-loop arrivals at a fixed fraction of measured capacity, so slow responses do not slow the offered load (coordinated omission). | F1, F2 |
| M4 | Sampler at 100 ms: per node `CommitIndex`, `LastApplied`, leader flag, leader-change count, `sstable_count`, active strategy. Needs small exported accessors in `raft/`. | missing metrics |
| M5 | JVM counters in `/metrics`: flushes, compactions started and finished, bytes compacted, time spent compacting. Replace the broken `compaction_events`. | F6 |
| M6 | Assign engine modes by actual role. Force the initial leader (hold back `Start()` on the other nodes) and record who led. | F7 |
| M7 | One `rand.Rand` per goroutine, seeded from the run seed. | F15 |
| M8 | Randomized run order with a Latin-square block, position recorded in the output. | F9 |
| M9 | Do not kill JVMs on return. Wait until `LastApplied == CommitIndex` on all nodes or a timeout, and report the applied fraction. | F2, F5 |
| M10 | Put the Raft log and the engine on separate directories that can be mapped to separate devices (loop devices, tmpfs vs disk) or shared deliberately. Record which. | confound control |
| M11 | Fault injection: configurable delay inside `RaftLog` around `Sync`, per node, for deterministic fsync slowness; optional cgroup v2 `io.max` on the engine directory when the host allows it. | scenarios |
| M12 | Later: a real network transport so nodes can be separate processes. | F12 |

## E0: redo the original experiment correctly

Question: with M1 to M10 in place, does leader/follower compaction mismatch change commit latency or apply backlog?
Factors: three configs as before, but modes assigned by role (M6). 10 repetitions each, order randomized (M8).
Primary outcome: commit latency p99 (t0 to t3). Secondary: apply backlog p99, time with backlog above 100.
Why bother: closes H0 honestly. A clean negative result goes in the README in place of the current table.

## E1: quorum masking (H1)

Factors: slow set in {none, F1, F2, L, L+F1, F1+F2}, injected fsync delay d in {0, 5, 20, 50} ms on the Raft log of the slow nodes (M11). Engine compaction on, workload from M2, open-loop load at 50% of no-fault capacity.
Duration 120 s per run, first 20 s discarded. 10 repetitions per cell. Cells run in randomized order within blocks.
Primary outcome: commit latency p99. Secondary: p50, p99.9, leader changes, proposal failures.
Prediction and failure threshold: see H1.
Cost: 6 x 4 x 10 x 140 s is about 9 hours of wall time on one machine. Pilot with 3 reps first to estimate variance; the final n is set by a power calculation for a 20% difference in p99, not fixed in advance.

## E2: synchronized maintenance (H2)

Factors: stagger off, stagger on (wire `CompactionCoordinator`), and a desynchronization control (per-node memtable threshold jittered by plus or minus 20%).
Record per node, with a monotonic clock: flush start and end with the log index being flushed, compaction start and end, bytes. Compute (a) dispersion of flush log indices across replicas, (b) fraction of time with at least 2 of 3 replicas compacting, (c) commit p99 in windows where a majority compacts versus not.
Run each variant with the Raft log and the engine on the same device, and again on separate devices. If H2 is right, the effect should shrink or vanish on separate devices.
10 repetitions per cell.

## E3: backlog and failover characterization (H3)

Ten-minute runs under the M2 workload, backlog time series from M4. Then repeat with a leader kill at a random time and record time until the new leader has `LastApplied == CommitIndex`. The linearizable-read path (read-index) is not implemented, so this is a proxy for time-to-serve.
10 repetitions per configuration.

## E4: policies (H4)

Policies: static leveled, static tiered, adaptive (classifier only), a load-aware throttle modelled on SILK's idea (reduce compaction bandwidth when client load is high), and a backlog-driven controller (pressure fed from real `CommitIndex - LastApplied`, with `UpdateApplyLatency` actually called).
Same workload and fault set as E1 and E3. Primary outcome: apply-backlog p99; guard outcome: write throughput must not drop more than 10%.
I would not implement SILK itself. The comparison is against a simple load-aware policy, and any claim has to say so.

## Statistics, fixed in advance

* Randomized, blocked run order for every comparison. Position recorded and included in the analysis.
* At least 10 repetitions per cell for any comparison that will be quoted; more if the pilot variance demands it.
* Report medians with 95% bootstrap confidence intervals (10,000 resamples) and an effect size (Cliff's delta for latency distributions, difference in p99 for headline numbers). Mann-Whitney U for pairwise tests, Holm correction across the comparisons in one experiment.
* One primary outcome per experiment, named before running.
* Discard warm-up, keep raw per-op timestamps for every run, and publish them.
* No result from a single run is called a finding.

## Simulation versus hardware

Everything in this stage runs on one machine with an in-process transport, so it is a controlled model of replication on shared hardware, not a measurement of a cluster. Results are labelled `sim-transport / shared-host` and the label stays on any claim until M12 and a multi-machine run exist. Runs from the sandbox (2 vCPU virtual machine) and from your Dell/WSL2 laptop are different environments and must not be pooled.
