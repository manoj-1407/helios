# Baselines and metrics

## Baselines

| id | what | status in the repo |
|---|---|---|
| B0 | No compaction (`Mode.DISABLED` exists). Bounds how much of any effect is compaction at all. | exists |
| B1 | Static leveled on all replicas (`FORCE_LEVELED`). | exists |
| B2 | Static tiered on all replicas (`FORCE_TIERED`). | exists |
| B3 | Adaptive classifier only, no replication signal. | exists, but its switch counter is broken (F6) |
| B4 | Staggered compaction across replicas (`CompactionCoordinator`). | code exists, never wired |
| B5 | Load-aware throttle in the spirit of SILK: lower compaction bandwidth when client load is high. A simplified stand-in, not SILK. | to write |
| B6 | Backlog-driven controller: pressure from real apply backlog, with the latency term fed. | to write; needs F10 fixed |
| B7 | Unmodified RocksDB under the same Raft harness, as an external reference for what a mature engine does with the same log. | optional, later |

B0 is the most important and the cheapest. If B0 shows the same commit and backlog behaviour as everything else, compaction is not the variable.

## Metrics

Primary, by experiment:

| experiment | primary metric | definition |
|---|---|---|
| E0, E1 | Commit latency p99 | Leader clock, from `Propose` call (t0) to the moment `CommitIndex >= index` (t3). |
| E2 | Fraction of time a majority is compacting | From per-node compaction intervals on a monotonic clock. |
| E3, E4 | Apply backlog p99 | `CommitIndex - LastApplied` per node, sampled every 100 ms, pooled per run. |

Secondary, always recorded:

| metric | definition |
|---|---|
| Client-ack latency | t0 to t1. Reported separately; this is what the current bench calls throughput. |
| Commit-to-apply delay | Per entry, t3 to t6 on each node. |
| Apply service time | Time of one `Apply` call. This is the number the bench currently calls "lag". |
| Throughput | Committed ops per second, not proposals per second. |
| Leader changes, proposal failures | Counts per run. |
| Flush and compaction counts, bytes compacted, time compacting | Per node, from M5. |
| SSTable count over time | Per node. |
| Write amplification | Physical bytes over logical bytes, computed after all applies drain. |
| Recovery time | Leader kill to new leader `LastApplied == CommitIndex`. |
| CPU and I/O | Process-level CPU time and block I/O per node, from `/proc`. |

Costs, only once H5 is in scope: storage and compute cost per SLO-hour using published provider prices, cited with retrieval date. No prices appear in this repository until they have been looked up.

## Things that must not be reported as metrics

* Client throughput from a harness that does not wait for commit.
* Write amplification from a run where applies had not drained.
* "Compaction events" from the current `/stats` field.
* Any "leader versus follower" comparison without a recorded leader id.
