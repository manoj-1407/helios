# prior art for the r.mu-across-fsync hypothesis

date 2026-11-06. scope: has anyone reported, explained or fixed "a consensus-state lock held across a WAL fsync delays heartbeats, and that causes elections even though the fsync is shorter than the election timeout". labels: report = someone saw it, root cause = someone showed why, fix = a change that shipped.

## result in one paragraph

no source I could read describes this exact mechanism (lock held across fsync, heartbeat path blocked behind it, election with sub-timeout fsync latency at high utilisation). the closest items are about different mechanisms. so this does not kill the experiment, and it does not support a novelty claim either: absence of a hit in the sources I could reach is not evidence of absence. the mitigation (do storage writes off the consensus loop) is established engineering in three libraries, so any result here is a reproduction/engineering check of a known design direction, not a new technique.

## sources read in this pass (fetched 2026-11-06)

| source | label | what it actually says | relevance |
|---|---|---|---|
| etcd-io/etcd #15247 | report, confirmed bug, fixed (#16822, backported to 3.5/3.4) | leader disk blocked (fdatasync) for 60 s; raft steps down but the lessor stays primary; stale lease revokes. root cause = lessor not synced with raft leader. | stalled disk and leadership, but long stall and a lease bug. not heartbeat starvation under load. |
| hashicorp/raft #503 | report, root cause proposed but not verified by maintainers, no fix | leader LogStore hangs indefinitely; cluster hangs, no election, because heartbeats come from per-follower goroutines that keep running. | opposite pathology: heartbeats NOT coupled to the disk, so no election. supports the idea that decoupling heartbeats from storage is what prevents elections, and shows the cost (zombie leader). |
| etcd-io/raft PR #8 (AsyncStorageWrites) | fix (design change, opt-in) | storage writes run off the raft state-machine loop. stated goals: less interference between proposals, batching; reports -29% avg latency, +73% peak throughput. | mitigation exists. the PR text I got does not mention heartbeats or ticks, so I cannot say it was motivated by this. |

## sources I found earlier in the session and did not re-read in this pass

treat as leads, check before citing: NuRaft parallel log appending docs (mitigation), etcd tuning guidance that heartbeat/election timeout must exceed disk latency (documents the general disk-latency to election relationship, not the lock mechanism), the etcd FAQ mirror, CockroachDB async raft log appends, a raft-dev mailing list thread. TiKV/TiDB docs: nothing specific found. the Raft dissertation's statement that followers/leader may write to disk in parallel with replication is known to me only secondhand (the PDF was not reachable); do not quote it.

## not reached

CockroachDB issue/PR pages on the allowlist-blocked path (e.g. #17500 fetch was refused), TiKV issue tracker search, NuRaft issues. these remain unsearched, so "not found" covers only what is listed above.

## consequence for the experiment

1. go ahead, framed as: controlled test of whether moving fsync out of the locks the heartbeat path needs removes the elections seen near fsync-limited saturation in this implementation.
2. no novelty claim. no claim about etcd, TiKV or CockroachDB behaviour.
3. the intervention is the same shape as etcd/raft AsyncStorageWrites and NuRaft parallel appends, but is a much smaller change and is not a reimplementation of either.
