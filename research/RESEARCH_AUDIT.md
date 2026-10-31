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
| F4 | The workload is 200 keys at a 64 KiB memtable. `thesis.json` shows `CompactionSwitches: 0` for all configs and the repo's own README says no merge ran. | read, and the repo's own result | No compaction was demonstrated in any config. Not independently confirmed here because the JVM counter that would show it is broken (F6). |
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
