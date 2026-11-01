# Helios research audit

Commit audited: `b700823` (default branch `main`). Branch: `research/helios-compaction-investigation`.
Method: read the Go and Java source end to end for the write path, then reproduce the claims that matter by running code. Anything marked **reproduced** was run on this host (2 vCPU, 8 GiB, ext4, Linux 6.18, OpenJDK 21.0.12, Go 1.24.7). Anything marked **read** comes from the code only. Anything marked **not checked** I did not test.

The headline: the committed benchmark cannot support the Helios thesis, in either direction. Its latency number is dominated by a fixed ~44 ms socket stall, its throughput number does not include commit or apply, and its config labels do not match node roles. That is a measurement problem, not evidence against compaction effects.

## Findings

| id | finding | status | effect on the existing claims |
|---|---|---|---|
| F1 | `/kv/put` and the bench report success when the leader has appended locally (`raft.go:750-775`). No client waits for commit. | read | "Throughput" is the leader's local append rate. |
| F2 | `OpsPerSec` = ops / wall time until all `Propose` calls return (`bench/replication_bench.go`). Commit and apply are not in it. | read | Cannot show a compaction effect on replication. |
| F3 | Every `/put` takes ~44 ms with the default JVM HTTP server and ~1 ms with `-Dsun.net.httpserver.nodelay=true`. Same JVM, same jar, 400 sequential applies, 3 repetitions each: p50 43.94 / 43.96 / 43.96 ms versus 0.99 / 0.90 / 0.97 ms. | **reproduced** | The bench's "P50ApplyLagNs" (about 52 ms in `thesis.json`) is mostly this stall. It is the same for every config, so it hides any real difference. Cause is consistent with Nagle plus delayed ACK on the header/body split; I did not capture packets to prove it. |
| F4 | The workload is 200 keys at a 64 KiB memtable. Using the new JVM counters (below): 40,000 puts over 200 keys produce **0 flushes, 0 sstables, 0 compactions**; the same 40,000 puts over 100,000 keys produce 8 flushes and 3 compactions. | **reproduced** | The bench's key space never triggers compaction in any config. Any difference between configs in `thesis.json` cannot be a compaction effect. Overwrites to the same 200 keys stay inside the memtable. I do not know what produced the 0.37 to 0.41 write amplification in `thesis.json` (my 200-key run reports 0.0 before a forced flush); I have not tried to explain it. |
| F5 | The bench kills the JVMs when `Run` returns, without waiting for `LastApplied == CommitIndex`. | read | Write amplification and apply counts are taken mid-drain. |
| F6 | `compaction_events` increments only if the strategy name differs across two reads inside one `/put` handler (`HeliosServer.java:84-88`). Strategy changes happen on the 2 s compaction tick. | read | The metric is effectively always 0. It is not evidence that nothing compacted. |
| F7 | Modes are assigned by node index (`bridges[i]`), but the leader is whichever node wins the election, and `BenchCluster.Leader()` iterates a map. "Leader=LEVELED, follower=TIERED" is not guaranteed by the harness. | read | The `mismatched` label is not a controlled condition. |
| F8 | The field `ApplyErrors` counts failed `Propose` calls (no leader, or not leader). It does not count apply errors. | read | `thesis.json` reports 32,868 to 35,245 of 50,000. That is a 66 to 70% proposal failure rate, which says leadership was unstable or absent for most of those runs. In my 22 runs per regime at 4,000 ops there were 0 failures. The difference is unexplained; the committed run came from a different host. |
| F9 | Run order is fixed (adaptive, mismatched, static). `thesis.json` ops/s falls with position: 575, 551, 497. | read | Order is confounded with config. See "Order effect" below: **not reproduced on this host.** |
| F10 | The pressure feedback loop is only started in `cmd/helios/main.go:94`, never in the bench. `UpdateApplyLatency` has no callers, so the latency half of the pressure score is permanently 0. | read (grep) | The headline "Raft state feeds back into compaction" mechanism was not exercised by any benchmark. |
| F11 | The committed `helios-kv/helios-kv.jar` has `Main-Class: dev.helios.server.HeliosServer`. The bench starts it with `server <dir> <port> <mode> <memtable>`. `HeliosServer.main` parses its first argument as a port and crashes. The pom sets `dev.helios.Main`. | **reproduced** (NumberFormatException on start) | `go run ./cmd/bench` from a fresh clone fails with "never became healthy". The committed `thesis.json` can only have come from a differently built jar. |
| F12 | There is no network transport. `ChannelTransport` is in-process method calls (`raft/transport.go`). The "cluster" is three Raft nodes in one Go process plus three JVMs, all sharing CPUs and one disk. | read | Network and disk heterogeneity cannot be studied without new work. |
| F13 | `Propose` and `HandleAppendEntries` call `log.Append` (write plus fsync) while holding `r.mu` (`raft.go:756-758`, `691`). | read | A slow fsync on one node blocks that node's vote handling, heartbeats, and state reads. This is a possible coupling between storage speed and election stability. Untested. |
| F14 | `applyCommitted` returns on the first `Apply` error and nothing retries until the next commit signal (`raft.go:808-816`). `commitC` has capacity 1. | read | One failed HTTP call to the JVM stalls a node's apply until more commits arrive. After the last write, it can stall indefinitely. |
| F15 | The bench shares one `rand.Rand` across up to 32 concurrent goroutines. `rand.Rand` is not safe for concurrent use. | read | Workload is not reproducible even with the fixed seed 42, and the race is a correctness bug in the harness. |

## Order effect

The draft hypothesis document called the first config in a fixed order "the slow one". My data does not support that on this host. I ran `research/cmd/ordertest` with 4,000 ops per run: four identical `static_leveled` runs, then all six orderings of the three configs, so each config sits in each position twice. 22 runs per regime.

| regime | same-config ops/s (4 runs) | CV | by config (mean) | by position (mean) |
|---|---|---|---|---|
| default | 2419, 2747, 2413, 2710 | 6.1% | adaptive 2497, mismatched 2623, static 2591 | pos 1: 2578, pos 2: 2606, pos 3: 2528 |
| nodelay | 1272, 1347, 1374, 1436 | 4.3% | adaptive 1449, mismatched 1380, static 1420 | pos 1: 1448, pos 2: 1378, pos 3: 1422 |

Config differences are within run-to-run noise, and so are position differences. So F9 stands as a design flaw in the harness, but the downward trend in `thesis.json` is not shown to be an order effect; it could be thermal or background load on the original machine. 4 to 6 runs per cell is also too few to rule out a small effect.

## Throughput dropped when I removed the stall

With nodelay, ops/s fell from about 2,570 to about 1,360 while p50 per-put time fell from 44 ms to 2 to 3 ms. I have a hypothesis, not a measurement: with the stall, apply goroutines sleep inside the socket and leave the 2 vCPUs free for proposers; without it they run flat out and compete for CPU with the proposers and the three JVMs. If true, "throughput" in this harness is a CPU-contention number, not a storage number. Do not quote either figure as a system property.

## Numbers that do not match

The assignment brief quotes 303, 714 and 721 ops/s. The committed `bench/results/thesis.json` shows 575, 551 and 497. I do not have the source of the quoted numbers; they do not appear in this repository. Everything above compares against the committed file.

## What this does and does not show

It shows the benchmark is invalid for the thesis. It does not show that compaction mismatch has no effect on replication; no valid experiment of that has been run yet. `HYPOTHESES.md` has the replacement hypotheses and `EXPERIMENT_DESIGN.md` has the experiments that would test them.

Raw data: `research/results/` (`applylat.txt`, `ordertest_default.csv`, `ordertest_nodelay.csv`, and the `clean_*.csv` versions with the JVM log lines removed from stdout).

## Round 2: first commit-latency numbers (nodelay server, per-op rng)

Commit `41e9faf` plus a harness change that records, per write on the leader's clock, ack latency (Propose call to return) and commit latency (Propose call to `CommitIndex >= index`, polled every 100 us). Same host as above. 24 runs of 4,000 ops at 90% writes: six identical `static_leveled` runs, then all six orderings of the three configs. Raw data: `results/commit_latency_v1.csv`. Zero proposal failures and zero commit timeouts.

| measure | six identical runs | across configs (means) |
|---|---|---|
| ops/s | 1248 to 1412, CV 5% | adaptive 1314, mismatched 1300, static 1346 |
| commit p50 ms | 8.0 to 14.3, CV 20% | 10.1, 10.8, 10.1 |
| commit p99 ms | 30.7 to 34.1, CV 4% | 38.3, 35.7, 34.9 |
| ack p99 ms | 21.4 to 25.6, CV 6% | not broken out |

What this says: with this harness the three configs are indistinguishable on commit latency and throughput. That is expected, not informative, because compaction is still not shown to run (F4, F6). It is not a negative result for the thesis.

What it does not say: the harness is closed-loop with 32 workers, so latencies include queueing among the clients themselves and are subject to coordinated omission. The ack p99 of ~24 ms is much larger than one fsync on this disk should cost; my guess is that 32 proposers serialize on `r.mu` while the leader fsyncs under the lock (F13). That is a hypothesis; no experiment here isolates it. The next harness step is open-loop arrivals at a fixed fraction of measured capacity (M3 in `EXPERIMENT_DESIGN.md`) and a compaction counter that works (M5), so that "compaction happened" is verified before any comparison.

## Round 3: counters that work, and a direct compaction check

Added to the JVM: completed flushes, completed compactions, tables compacted, and compaction busy time. They are exposed on `/stats` (new JSON fields) and `/metrics` (`helios_flushes_total`, `helios_compactions_total`, `helios_compaction_busy_seconds_total`). Unlike `compaction_events` (F6), they count real work.

Single JVM, `FORCE_LEVELED`, 64 KiB memtable, 40,000 sequential puts through the Go bridge, stats read 3 s after the last put. One run per row, so this is an existence check, not a measurement.

| keys | flushes | compactions | sstables at end | write amplification |
|---|---|---|---|---|
| 200 (the bench's) | 0 | 0 | 0 | 0.00 |
| 100,000 | 8 | 3 | 2 | 5.07 |

So the minimum change that lets the benchmark exercise compaction at all is a larger key space (and values big enough to fill the memtable). Raw output: `results/compaction_check.txt`. The 3 compactions at 100,000 keys busy the engine for about 1 ms in total on this host, which is far too little to matter for replication; a meaningful compaction workload needs larger values or many more writes. That is the next calibration step, not a result.

## Round 4: tracked jar, tests, race detector, commit semantics

* The tracked `helios-kv/helios-kv.jar` is replaced with a jar built from the current source with `Main-Class: dev.helios.Main` (built with `javac` and `jar`, because Maven plugins could not be resolved from this host; it carries no Maven metadata). `go run ./cmd/bench` now starts from the tracked jar with no extra build step. Committing a binary is still a bad idea (F11 in the roadmap); this is the minimum that makes a fresh clone runnable.
* `go test` and `go test -race` pass for `raft` and `chaos`. `store`, `client` and `bench` have no tests, so the per-op rng change (F15) is covered only by the next item.
* `go run -race ./cmd/bench -ops 2000` ran all three configs with the new jar: 0 data-race reports, 0 failed proposals, 0 commit timeouts. One run per config, so this checks for races and launch, not performance.
* New test `raft/commit_semantics_test.go`: for 200 proposals on a 3-node in-process cluster, at the instant `leader.CommitIndex() >= idx`, at least 2 of 3 nodes hold the same term and command at `idx`. Passes under `-race` three times. This supports using that condition as the commit time (t3) in the bench. It does not cover leader changes or crashes.

Still open from this round: the unexplained 0.37 to 0.41 write amplification in `thesis.json`. The fresh bench runs above report 0.71 to 0.75 with the same code path, which is a different number again, so the definition and the moment of measurement (F5) need to be pinned down before write amplification is used anywhere.

## Round 5: compaction calibration, write amplification, two more findings

### New findings

| id | finding | status |
|---|---|---|
| F16 | `LeveledCompaction` and `TieredCompaction` are the same algorithm: both call `CompactionMerge.mergeAll` on every live table and write one output table. They differ only in the trigger (3 tables versus 6, `BASE_TRIGGER_TABLE_COUNT`). Compaction is checked on a 2 s tick, and a flush can happen every few tens of ms, so dozens of tables are waiting at each tick and both trigger every time. There are no levels and no size tiers. "Leveled versus tiered versus adaptive" is therefore "compact at 3 tables versus 6", and in the data below the two are indistinguishable. | **read** (both `plan()` methods) and **reproduced** (identical results, below) |
| F17 | Concurrent writers to one helios-kv JVM race with memtable rotation. `HeliosDB.writeInternal` reads `wal` and `active` with no lock; a writer can hold the old WAL while the flush thread closes it, and `WAL.append` throws `ClosedChannelException` (stack trace captured). The JDK HTTP server then drops the connection with no response, so clients see `EOF`. Unique-key runs, one writer: 0 of 20,000 puts failed. Eight writers: 1,202 of 20,000 (6%). Four writers: about 1.1% (2 runs). | **reproduced**, `results/lost_write_check.txt` |

F17 is scoped. The Raft apply path is one serial writer per node, so the Raft benchmark should not hit it, and I did not change the engine. Any multi-client use of the helios-kv HTTP API does hit it. I tested for the worse variant, silent loss of an acknowledged write: unique keys, every acknowledged put read back after the run. 0 missing and 0 wrong values across 4 runs (about 19,800 acknowledged writes in the 4-writer runs and 18,798 in the 8-writer run). That does not support silent loss in this test; it does not rule it out, and crash durability was not tested. The put handler now catches the exception and answers 500 (previously the connection was dropped), so the failure is visible.

### Write amplification: what it is

`HeliosDB.writeAmplification()` = (bytes of SSTable data files written by flushes + bytes of SSTable data files written as compaction output) / (key + value bytes passed to `put`). It excludes the WAL, excludes bytes read by compaction, and counters live for the JVM's lifetime and never reset. It returns 1.0 when nothing has been written. A value below 1.0 means overwrites collapsed in the memtable, not that the engine amplifies less. `/stats` now also returns `logical_bytes`, `flush_bytes` and `compaction_bytes` separately.

In the bench, the 200-key workload never flushes on its own (F4); the only flush is the final `?flush=1` call, so the numerator is one small table and the denominator is whatever had been applied when it ran. Checked at three sizes with the fixed bench: 2,000 ops gave 0.71 to 0.75 for the three configs; 8,000 ops gave 0.42, 0.21 and 0.19. That is consistent with "roughly constant flush size over applied logical bytes", and with the 0.37 to 0.41 in `thesis.json` being a snapshot taken at an arbitrary point in the apply backlog (F5). It is not proven, and the 0.42 (adaptive) against 0.19 to 0.21 at the same ops is unexplained. Conclusion: do not use bench write amplification at all; use the engine-only numbers below.

### Calibration: a workload that compacts

Engine only (no Raft), single JVM, 64 KiB memtable, 100,000 puts, **one writer** (matches the Raft apply path), uniform keys, one run per cell. Criteria fixed before looking: zero failed puts, at least 10 completed compactions, and compaction busy time measurable against run time. `results/calibration_v2.csv`.

| mode | keys | value B | run s | flushes | compactions | busy ms | compaction MB | logical MB | write amp |
|---|---|---|---|---|---|---|---|---|---|
| leveled | 10,000 | 100 | 41.3 | 164 | 21 | 940 | 23 | 11 | 3.18 |
| leveled | 10,000 | 1000 | 47.9 | 1533 | 24 | 1676 | 219 | 101 | 3.17 |
| leveled | 100,000 | 100 | 42.4 | 168 | 19 | 2888 | 88 | 11 | 9.01 |
| leveled | 100,000 | 1000 | 47.0 | 1537 | 22 | 4936 | 812 | 101 | 9.05 |
| tiered | 10,000 | 100 | 40.7 | 164 | 19 | 956 | 22 | 11 | 3.03 |
| tiered | 10,000 | 1000 | 45.8 | 1533 | 23 | 1640 | 210 | 101 | 3.08 |
| tiered | 100,000 | 100 | 43.1 | 168 | 19 | 2901 | 87 | 11 | 8.93 |
| tiered | 100,000 | 1000 | 45.4 | 1537 | 21 | 4738 | 775 | 101 | 8.68 |

What it shows: all eight cells compact repeatedly, and write amplification comes from compaction (flush-only amplification is 1.01 to 1.11), growing with key count because each compaction rewrites the whole data set (F16). Compaction occupies roughly 2% to 10% of wall time in bursts that grow as data accumulates. Leveled and tiered differ by less than the cell-to-cell spread I would expect from noise, which matches F16; I have one run per cell, so no variance estimate.

What it does not show: any effect on replication. This is engine-only. The compaction duty cycle is small, so a compaction effect on commit latency may be small too. The quorum-masking experiment should not depend on compaction to create slowness; it injects fsync delay directly.

The raw run with 4 concurrent writers (`results/calibration_v1.csv`) is kept for the F17 evidence; its numbers include failed puts and should not be used for anything else (its `busy_ms` also excluded merge time, since fixed).

## Round 6: replicated runs through Raft on a workload that compacts

Harness changes (commit `148f362`): `bench.Keys` and `bench.ValueBytes` set the workload; after proposals return, the run waits for every node to apply the leader's final commit index before reading engine counters (F5); counters come from `/stats` without forcing a flush; p95 commit latency, drain time, applied fraction and bridge apply errors are reported; JVM stdout goes to stderr so CSVs are clean. Driver: `research/cmd/calibrun`, which runs each config once per randomized block (seeded order).

Workload (chosen from the Round 5 calibration criteria before this run, not tuned afterwards): 100,000 keys, 1,000-byte random values, 90% writes, 20,000 ops per run, 32 closed-loop clients, 64 KiB memtable, three JVMs and three in-process Raft nodes on the 2-vCPU host. Five configs, five blocks, 25 runs. Raw data: `results/replicated_calibrated_v1.csv`. An earlier start of this run was killed after about a minute because I ran `go test -race` on the same 2 vCPUs; the CSV here is the clean rerun, and nothing else ran on the host during it.

Health: 0 failed proposals, 0 commit timeouts, 0 bridge apply errors, applied fraction 1.0 in all 25 runs.

Means over 5 runs per config, with 95% t-intervals (n=5, so these are rough):

| config | ops/s | commit p50 ms | commit p95 ms | commit p99 ms | compactions (3 nodes) | engine busy ms | peak sstables | engine write amp |
|---|---|---|---|---|---|---|---|---|
| static_leveled | 1345 ±33 | 13.2 ±1.2 | 29.3 ±0.6 | 38.8 ±1.6 | 43.8 | 3391 | 24 | 6.62 |
| static_tiered | 1324 ±72 | 12.6 ±1.8 | 29.4 ±1.5 | 38.0 ±2.9 | 42.4 | 3575 | 32 | 6.61 |
| adaptive_both | 1391 ±55 | 11.5 ±0.5 | 27.7 ±1.1 | 35.4 ±1.4 | 42.6 | 3641 | 29 | 6.68 |
| node0 leveled, others tiered | 1327 ±66 | 12.6 ±2.2 | 28.9 ±1.3 | 36.9 ±2.0 | 43.4 | 3482 | 20 | 6.64 |
| no_compaction | 1390 ±63 | 11.8 ±1.2 | 29.4 ±1.3 | 38.3 ±2.1 | 0 | 0 | 272 | 1.01 |

Position in the run order (1 to 5) and block number showed no trend (ops/s by position 1339, 1335, 1349, 1368, 1385; by block 1320, 1351, 1404, 1344, 1358).

What this establishes, for this host and this harness only:

* Compaction really ran: about 43 compactions and about 3.4 s of engine busy time summed over the three nodes in every compacting config, engine write amplification about 6.6 against 1.01 without compaction. The Round 1 to 4 problem (nothing compacted) is fixed.
* With that compaction running, throughput and commit latency are not distinguishable from running no compaction at all, at this sample size. The `no_compaction` row is the control and sits inside the same intervals as the other four.
* The nominal "mismatched" and "adaptive" configs are not distinguishable from static ones either. That is expected given F16 (leveled and tiered are the same algorithm with different triggers) and the identical compaction counts above.

What it does not establish:

* It does not show that compaction cannot affect replication. Three things limit the test. (1) Commit does not depend on apply (see `ARCHITECTURE_AND_DATA_FLOW.md`), so commit latency is structurally insulated from engine work unless they share a resource. Here they share CPU only. (2) Apply is the bottleneck in every config: proposals finish in about 15 s and the nodes need about 17 to 19 s more to drain, with no compaction effect on that drain time either (17.2 s with no compaction, 18.0 to 19.0 s with). Each apply is two HTTP round trips to the local JVM (a PUT and a `/stats` GET), which may dwarf a compaction effect of a few percent duty cycle. (3) Every number comes from one 2-vCPU container with an in-process transport and one shared disk.
* The adaptive config shows lower p99 commit latency (35.4 ms) than static leveled (38.8 ms) with non-overlapping 95% intervals. With five configs, four latency metrics and n=5, a gap like this is expected to appear by chance somewhere, and nothing in the mechanism predicts it. I do not treat it as a finding. It would need a pre-registered repeat.
* Commit latency here is closed loop with 32 clients; its ~12 ms median is mostly queueing on the leader's serialized fsync (finding F13, untested). That is why the quorum-masking experiment is open loop.

Write amplification, resolved enough to use: engine write amplification as defined in Round 5 (flush bytes plus compaction output bytes over logical bytes, counters read after the drain) is stable at 6.3 to 7.0 across compacting runs and 1.01 without compaction. The bench's own `WriteAmplification` field (follower 1, forced flush) should still not be used.

## Round 7: quorum masking (E1), open loop

Code: `bench/quorum.go` (`RunQuorum`), driver `research/cmd/quorum`, fsync injection in `raft/log.go` (`SetFsyncDelay`, with per-node counters and a unit test, `raft/fsync_injection_test.go`). Setup: 3 nodes, in-process transport, three JVMs (FORCE_LEVELED; the legacy 200-key workload never compacts, so compaction is not a variable here), pure writes, **open loop at 15 writes/s** with latency measured from each arrival's intended send time, 30 s of arrivals per run with the first 5 s discarded (375 measured writes per run), 9 cells x 4 randomized blocks = 36 runs, seeded order. Raw data: `results/quorum_masking_v1.csv`.

Roles are assigned after the election: L is the leader and F1 and F2 are the other two nodes in id order. The delay is slept before the real fsync inside `RaftLog.Append`, so it is paid under the same locks a slow device would be.

Checks that the experiment did what it says: 0 failed proposals, 0 unfinished writes, no leader change in any of the 36 runs, exactly 375 measured writes in every run. The injected delay was reached: in every run, `injected_ms` on each slow node equals its sync count times the delay (within 10%) and is exactly 0 on the others, and the slow node's mean sync time is the delay plus about 0.3 ms of real fsync. Zero mismatches in 36 runs.

Means over 4 runs per cell with 95% t-intervals (n=4, rough):

| slow node(s) | injected delay | commit p50 ms | commit p99 ms | client-ack p50 ms |
|---|---|---|---|---|
| none | 0 | 1.47 ±0.05 | 2.81 ±0.69 | 1.10 ±0.08 |
| one follower | 10 ms | 1.38 ±0.10 | 3.55 ±1.43 | 1.04 ±0.05 |
| one follower | 40 ms | 1.36 ±0.07 | 3.34 ±1.62 | 1.04 ±0.06 |
| leader | 10 ms | 11.74 ±0.07 | 13.77 ±1.07 | 11.37 ±0.03 |
| leader | 40 ms | 42.09 ±0.04 | 44.11 ±1.14 | 41.67 ±0.03 |
| leader + one follower | 10 ms | 11.71 ±0.13 | 13.17 ±0.55 | 11.39 ±0.11 |
| leader + one follower | 40 ms | 42.02 ±0.09 | 43.68 ±0.82 | 41.66 ±0.07 |
| both followers | 10 ms | 11.94 ±0.05 | 13.95 ±0.83 | 1.05 ±0.06 |
| both followers | 40 ms | 42.15 ±0.13 | 45.53 ±3.24 | 1.00 ±0.08 |

Reading:

* H1 (quorum masking) held in this setup. One slow follower, at 10 ms and at 40 ms, leaves commit p50 unchanged (1.4 ms) and commit p99 within noise of baseline (3.3 to 3.6 ms against 2.8 ms, intervals overlapping). The pre-registered drop criterion in `HYPOTHESES.md` (a single slow follower raising commit p99 by more than 20% with non-overlapping intervals) was not met. The mean p99 is 19% to 26% higher, which is why I am not calling it "no effect": with 375 samples per run and n=4 the p99 estimate is too coarse to exclude a sub-millisecond tail effect.
* Commit latency rises by roughly the injected delay whenever no fast majority exists: slow leader, slow leader plus one follower, or both followers slow. This is the quorum arithmetic and is what the model predicts: commit needs the leader's own fsync plus one follower's.
* Client-ack latency and commit latency come apart exactly where the audit said they would (F1). With both followers slow, the client sees a ~1 ms acknowledgement while the write takes ~42 ms to commit. A benchmark that stops the clock at the acknowledgement would report "no effect" there.
* No queueing built up: at most 1 to 2 writes in flight and apply backlog of at most 2 entries in any run. The offered load is deliberately far below capacity, so this experiment says nothing about saturation behaviour.

Not covered, and where I would look next:

* Delays at or above the 100 ms RPC timeout (a very slow follower then looks dead to the leader), delays that vary over time, and partial slowness (only some fsyncs slow). These are where "slow but alive" differs from "uniformly slow".
* Higher offered load, where the leader's fsync under `r.mu` (F13) should serialize proposals and heartbeats and could couple a slow node to election stability.
* A real network and separate disks. Here the transport is in-process and all fsyncs hit one device, so the injected delay is the only asymmetry, which is cleaner than reality.
* Whether this is new. It is the textbook quorum result, reproduced on this implementation with a verified injection. It is a sanity result and a validated instrument, not a contribution; the literature gap is in the next experiments (slow-but-alive, interaction with background work), not in this one.

## Round 8a: what an acknowledgement means, and where `r.mu` is held across storage

### Acknowledgement semantics (read, then tested)

Path of `POST /kv/put` (`store/kvserver.go`): decode JSON, `handlePut` calls `propose`, which calls `RaftNode.Propose` once and, if it returns `ok`, writes `{"status":"ok"}`. Nothing else is awaited. `Propose` (`raft/raft.go`) takes `r.mu`, appends the entry to the leader's own log, which writes and fsyncs, releases `r.mu`, starts one replication goroutine per peer without waiting for them, and returns the index and term.

So an `ok` from the server means exactly this: the node that believed itself leader at that instant has durably appended the entry to its own Raft log. It does not mean replicated, committed, applied or visible. The bench's "ack" latency is the same event (Propose returning), and its "commit" latency is the later event `leader.CommitIndex() >= idx`, which `raft/commit_semantics_test.go` ties to a majority holding the entry. The server API has no commit-waiting mode; only the bench clients wait for commit, by polling the leader.

The consequence is demonstrated, not just argued: `chaos/ack_semantics_test.go` isolates the leader, writes through it (it acknowledges), lets the other two nodes elect a new leader and commit a different write, then heals the partition. The acknowledged write is gone from every node. This is standard for acknowledge-after-local-append, but it means the Helios API as built offers no durability guarantee beyond one machine, and reads (`/kv/get` reads the local JVM) can miss an acknowledged write for as long as apply lags (up to about 18 s after load in the Round 6 runs; read-after-write was not itself tested).

In Round 7 the "1 ms ack, 42 ms commit" result for slow followers is therefore exactly the semantics above and not an instrumentation error: the leader's own fsync was fast, so it acknowledged immediately, and the write committed only when a slow follower caught up.

### Where `r.mu` is held across storage (read)

| code | holds `r.mu` while | affected by my injected delay |
|---|---|---|
| `Propose` | appending and fsyncing the leader's log | yes |
| `HandleAppendEntries` | `log.TruncateFrom` and `log.Append` (both fsync) | `Append` yes, `TruncateFrom` no |
| `HandleRequestVote` | `SavePersistentState` (write, fsync, rename of the term/vote file) | no |
| `runElection` | `SavePersistentState` | no |
| `becomeFollowerLocked` | `SavePersistentState` | no |

Everything else that needs the same mutex therefore queues behind a slow fsync on that node: `State()`, `Term()`, `CommitIndex()`, the election timer callback `onElectionTimeout`, `HandlePreVote`, `HandleRequestVote`, and, on the leader, every `replicateToAllPeers` and `replicateToPeer` call (each takes `r.mu` at the start and again to process the reply), including the periodic heartbeat. Heartbeats are not a separate path: the heartbeat loop calls the same `replicateToAllPeers`. So a leader whose log fsync takes longer than a heartbeat interval cannot send heartbeats while it is stalled, and a leader stalled for longer than a follower's election timeout (150 to 300 ms by default) leaves followers with nothing to reset their timers. That is the coupling the experiments below test. Many production Raft implementations keep disk writes off the heartbeat path; this implementation does not.

Two limits of my injection follow from the table. It only delays `Append` fsyncs, so the vote and term persistence paths, which a real slow device would also slow, are not delayed. And `lastHeartbeatSeen` and the election timer are reset inside `HandleAppendEntries` after the handler has acquired `r.mu`, so a follower stalled on its own fsync cannot reset its timer while stalled.

Two further code facts that shape the experiments. First, `ChannelTransport` by default runs the handler synchronously and ignores the caller's deadline, so a slow handler can never produce an RPC timeout; the transport now has a `SetEnforceDeadlines` switch (tested in `raft/fsync_injection_test.go`) that makes it behave like a network RPC with the 100 ms `RPCTimeout`. Experiments about "delays above the RPC timeout" are only meaningful with it on, and all stages use it on except where a cell says otherwise. Second, pre-vote exists (`raft/prevote.go`) and should stop a lone slow node from inflating terms; whether it does under these conditions is part of what is measured.

### Observer effect, fixed

The first version of the Raft-only load generator read commit index, term and leader state through `RaftNode` accessors that take `r.mu`, i.e. through the lock under study, so a stalled node would also stall the observers and the stall would be under-counted. `RaftNode` now keeps lock-free mirrors of state, term and commit index (`ObservedState`, `ObservedTerm`, `ObservedCommit`), written next to each mutation, and the generator uses only those. Whether a write that was counted as committed is the entry that actually committed at its index is checked after the run, with injections cleared.

### Stage 0: capacity of the Raft-only leader (no delay)

`research/plans/stage0_capacity.cells`, three runs per rate, 12 s each, `results/raftload_stage0_capacity.csv`. Commit p50 stays at 1.1 to 1.5 ms up to 2,000 writes/s with a p99 that grows from about 5 ms (200/s) to 11 to 22 ms (2,000/s). At 4,000 offered the leader achieves about 3,000 writes/s and commit latency is in the seconds (a queue is growing). At 8,000 and 16,000 offered, achieved throughput falls to about 1,000 to 2,200 writes/s, lower than at 4,000: past saturation the implementation loses throughput, probably from its unbounded one-goroutine-per-peer-per-proposal replication and the generator's own cost on 2 vCPUs. I did not isolate that. Load levels for later stages were fixed from this table before any delay results existed: 500 (low), 2,000 (near the knee), 3,000 (past it) writes/s.
