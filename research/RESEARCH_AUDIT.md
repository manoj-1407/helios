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
