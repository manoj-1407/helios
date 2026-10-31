# Helios: verified architecture and data flow

Everything here comes from reading the code at commit `b700823` and from the experiments in `research/results/`. Where this disagrees with `README.md` or `ARCHITECTURE.md`, the disagreement is listed in the last section. File and line references are to that commit.

## Components as built

| Piece | What it really is |
|---|---|
| `raft/` | Single-group Raft with pre-vote, persisted term/vote, fsynced log (`raft/log.go`), fast conflict resolution, current-term-only commit rule, snapshots. Peers talk through `ChannelTransport`, which is plain in-process method calls (`raft/transport.go`). There is no network transport in the repo. |
| `store/bridge.go` | The Raft `StateMachine`. Each committed entry becomes one synchronous HTTP POST to the node's own helios-kv JVM, followed by a second HTTP GET (`/stats`) on every apply. |
| `store/kvserver.go` | HTTP front end. `/kv/put` calls `RaftNode.Propose` and answers `ok` as soon as the leader has appended locally. |
| `helios-kv/` | Single-node LSM engine over loopback HTTP: DSYNC WAL, ConcurrentSkipListMap memtable, background flush thread, compaction on a 2 s tick, workload classifier. |
| `store/pressure_loop.go` | Pushes `PressureLevel()` from the Raft node into the JVM every 500 ms. Started only in `cmd/helios/main.go:94`. |
| `bench/` | Spawns 3 JVMs and a 3-node in-process Raft cluster on one machine and one disk. |

So a "three-node cluster" in the benchmark is three goroutine groups in one Go process plus three JVMs, all sharing one scheduler pool, one kernel, and one block device.

## Life of one write

```
t0  client calls Propose (bench) or POST /kv/put (KVServer)
      raft.go:750   lock r.mu
      raft.go:758   log.Append(entry)  -> write + fsync, while holding r.mu
t1  Propose returns (idx, term, ok).  KVServer answers "ok" here.        <- client ack
      raft.go:773   replicateToAllPeers(): one goroutine per peer, no in-flight limit
t2  leader sends AppendEntries (RPC timeout 100 ms)
      raft.go:644   follower: lock r.mu, log.Append(entries) -> fsync, reply
t3  leader receives success, sets MatchIndex[peer]
      raft.go:580   maybeAdvanceCommitIndexLocked: majority incl. leader, entry.Term == currentTerm
      raft.go:595   CommitIndex = n ; non-blocking send on commitC (capacity 1)
t4  applyLoop wakes (one goroutine per node)
      raft.go:795   applyCommitted: for each index, sm.Apply(entry), strictly serial
t5  bridge.Apply  (store/bridge.go:104)
      POST /put -> JVM: WAL.append (FileChannel, DSYNC) -> MemTable.put
      GET /stats  -> JVM: builds a JSON string
t6  LastApplied = idx   (raft.go:817)
```

Followers reach t3-equivalent from the leader's next AppendEntries carrying `LeaderCommit` (`raft.go:696`), then run t4 to t6 themselves.

Two structural facts follow from this and they drive the whole research question:

1. Commit (t3) needs only log appends on a majority. State-machine apply (t4 to t6) is off the commit path. A replica whose storage engine is slow still acks AppendEntries on time as long as its Raft log fsync is fast, so storage-engine slowness shows up as apply backlog, not as commit latency, unless the two share a resource (the device that holds the Raft log, CPU, memory bandwidth).
2. The client ack (t1) happens before any follower has the entry. Whatever the bench reports as client throughput or latency is the leader's local append rate.

## What each reported metric actually measures

| Reported as | Where computed | What it is |
|---|---|---|
| `OpsPerSec` | `bench/replication_bench.go:209` | Operations divided by wall time until every `Propose` call has returned. Excludes commit and apply. Failed proposals count as ops. |
| `ApplyErrors` | `replication_bench.go:141-166` | Count of failed `Propose` calls (no leader found, or not leader). Not apply errors. |
| `P50/P99/MaxApplyLagNs` | `store/bridge.go:122` | Wall time of one HTTP POST to the local JVM, per applied entry. A service time. It does not include the wait between commit and apply, which is the real lag. |
| `ReplicationLagNs` (Raft) | `raft.go:523` | AppendEntries round-trip time for the latest batch with entries. Not queueing lag. Never exported by the bench. |
| `RepMetrics` lag | `raft.go:527`, `raft/metrics.go:37` | `leaderLastIndex - matchIndex`. Grows with the open-loop proposal rate, independent of storage speed. |
| `CompactionSwitches` | `bridge.go:127-129` from `HeliosServer.java:85-88` | Counts a strategy change only if it happens between two reads a few microseconds apart inside one `/put` handler. The real switch happens on the 2 s compaction tick. Effectively always 0. |
| `WriteAmplification` | `replication_bench.go:196-199` | Taken from node 1's JVM after a forced flush. Depends on how many applies had completed before the JVMs were killed. |

## What nothing measures today

* Commit latency from the client's point of view (t0 to t3 on the leader).
* Commit-to-apply delay per entry, and the apply backlog `CommitIndex - LastApplied` over time.
* Leader changes during a run (`RaftMetrics.LeaderChanges` exists but has no accessor).
* Number of SSTables, flushes and compaction runs per node over time.
* Which node was leader, and for how long.

Instrumentation points for all of the above are in `EXPERIMENT_DESIGN.md`.

## Where the docs and the code disagree

| Claim | Reality |
|---|---|
| `ARCHITECTURE.md`: acknowledged writes survive loss of a minority. | The ack is sent after the leader's local fsync only (`kvserver.go:99-105`). A leader crash right after the ack can lose the write. |
| `README.md`: the 200-key space "triggers compaction repeatedly". | With 200 keys and a 64 KiB memtable no flush ever happens, so no SSTable exists and compaction cannot run (`research/results/`, section F4 of `RESEARCH_AUDIT.md`). |
| `bench/results/README.md`: "flushes happened (evidenced by non-zero write_amp)". | The only flush is the forced `/stats?flush=1` at the end of the run. |
| `store/pressure_loop.go` / `ReplicationPressureReceiver`: pressure combines lag, apply latency and staleness. | `UpdateApplyLatency` is never called, so the latency term is always 0 and the CRITICAL state is unreachable. |
| `raft/compaction_signal.go`: cross-node compaction stagger. | `CompactionCoordinator` is never constructed or consulted. |
| README "leader" and "follower" compaction modes. | Modes are assigned by bridge index (node 0 gets the "leader" mode). The Raft leader is whoever wins the election. |
| `cmd/helios` and `cmd/bench` both launch the JVM. | They use incompatible jar entry points, and the committed jar matches only `cmd/helios`. |
