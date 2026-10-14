# raft — Consensus Layer

This package implements the Raft consensus algorithm from scratch in Go. It is the component that makes Helios a *distributed* system — without consensus, multiple nodes would have no way to agree on the same sequence of writes.

---

## Contents

1. [What Raft does and why it exists](#what-raft-does-and-why-it-exists)
2. [The Raft guarantee](#the-raft-guarantee)
3. [Leader election](#leader-election)
4. [Pre-vote extension](#pre-vote-extension)
5. [Log replication](#log-replication)
6. [Commit and apply](#commit-and-apply)
7. [Snapshots](#snapshots)
8. [Replication metrics](#replication-metrics)
9. [Compaction coordinator](#compaction-coordinator)
10. [File map](#file-map)
11. [Running tests](#running-tests)

---

## What Raft does and why it exists

Imagine three servers, each storing the same key-value data. A client writes `SET foo bar` to server 1. Servers 2 and 3 also need to have `foo = bar`. What if server 1 crashes after writing locally but before telling the others? What if the network drops the message to server 3 but not server 2? What if servers 2 and 3 simultaneously decide server 1 is dead and both try to become the new primary — do you get two primaries writing conflicting data?

These are the problems consensus solves. Raft provides a **single agreed-upon sequence** of operations that all nodes apply in the same order. If a majority of nodes received an operation, it is **committed** — it will never be lost even if every other node crashes.

---

## The Raft guarantee

**Safety:** at any point in time, there is at most one leader per term. Two leaders can never exist simultaneously. A committed entry will appear in the log of every future leader.

**Liveness:** as long as a majority of nodes can communicate with each other, the cluster will elect a leader and make progress.

**What Raft does NOT guarantee:** liveness in the face of a majority failure. If 2 out of 3 nodes crash, the remaining node cannot form a quorum and will not accept writes. This is correct behavior — it is better to refuse writes than to accept them and potentially diverge from the crashed nodes.

---

## Leader election

Each node runs an **election timer** — a random timeout between 150ms and 300ms. When the timer fires without hearing from a leader, the node becomes a **candidate** and starts an election.

**Why random timeouts?** If all nodes had the same timeout, they would all start elections simultaneously and all fail (split votes). Randomization ensures one node usually fires first and wins before others wake up.

Election steps:
1. Node increments its current term
2. Node votes for itself
3. Node sends `RequestVote` RPCs to all peers
4. If it receives votes from a majority (including itself): it becomes leader, starts sending heartbeats
5. If it receives a `RequestVote` or `AppendEntries` from a node with a higher term: it reverts to follower

**Vote grant rules:**  
A node grants its vote if:
- The candidate's term is at least as large as the voter's current term
- The voter has not already voted in this term
- The candidate's log is at least as up-to-date as the voter's log (last log term comparison, then index comparison)

The log up-to-date check prevents a lagging node with old data from becoming leader.

---

## Pre-vote extension

Standard Raft has a problem with partitioned nodes. If node3 gets network-isolated, it:
1. Times out waiting for heartbeats
2. Increments its term to, say, term 10
3. Tries to hold an election — fails because it can't reach anyone
4. Increments term to 11, tries again... keeps incrementing

When node3 rejoins, it sends a `RequestVote` for term 47. The current stable leader (at term 6) receives this message, sees the higher term, and **steps down** — even though node3 is completely behind on the log. The cluster now has no leader and must hold another election. This causes unnecessary disruption.

**Pre-vote** (`prevote.go`) adds a phase before incrementing the term:

1. Node sends `PreVote` RPCs asking peers: "if I started an election right now, would you vote for me?"
2. If a majority responds "yes": *then* increment term and start the real election
3. If not: don't increment term, stay as follower

Partitioned nodes can never get pre-votes (they can't reach anyone), so their term never increases. When they rejoin, they have a normal term and rejoin as followers without disruption.

This is the same extension etcd uses.

---

## Log replication

After election, the leader:
1. Begins sending periodic **heartbeats** (empty `AppendEntries`) to all followers to reset their election timers
2. When it receives a write, appends it to its own log
3. Sends `AppendEntries` to all followers in parallel
4. Once a majority (including itself) have written the entry to their log: **commits** it
5. Notifies the state machine to apply the committed entry
6. On the next AppendEntries (or heartbeat): tells followers the new commit index, so they also apply it

**Log matching invariant:** if two logs have an entry with the same index and term, all entries before that index are identical. Raft enforces this by including `prevLogIndex` and `prevLogTerm` in every AppendEntries. A follower rejects the message if its log doesn't match at that position — the leader then backs up and resends from the divergence point.

**Fast conflict resolution:** when a follower rejects AppendEntries, it can include the conflicting term in its response. The leader then jumps directly to the start of that term's entries rather than backing up one entry at a time. This reduces the catch-up time from O(n) round trips to O(terms).

---

## Commit and apply

`statemachine.go`

The distinction between **committed** and **applied** is important:

- **Committed** means a majority of nodes have the entry in their log. It will not be lost. But it hasn't been executed yet.
- **Applied** means the entry has been executed — the actual KV put/get/delete has happened.

The apply loop runs in a dedicated goroutine. It watches `commitIndex` and applies entries from `lastApplied + 1` to `commitIndex` in order. Each entry is sent to the bridge as an HTTP call to helios-kv.

The log records the P99 latency of apply operations as part of `ReplicationMetrics`. Slow apply = followers fall behind = higher pressure.

---

## Snapshots

`snapshot.go`

The Raft log grows indefinitely. Without compaction, a restarting node would need to replay millions of log entries. **Snapshots** solve this: the state machine periodically writes the entire current state to a snapshot file, and the Raft log is truncated up to that point.

When a new node joins or a lagging follower is too far behind for log replication to catch up, the leader sends the snapshot directly. The follower discards its log, installs the snapshot, and resumes from there.

The snapshot infrastructure is implemented (`snapshot.go`) but the install-snapshot RPC is not yet wired into the bridge for full cross-node state transfer. This is listed as a future work item.

---

## Replication metrics

`metrics.go`

`ReplicationMetrics` tracks the health of replication in real time. It is the data source for the pressure feedback loop.

```go
type ReplicationMetrics struct {
    mu          sync.RWMutex
    lagPerPeer  map[string]int64     // leaderLastIndex - matchIndex per peer
    applyTimes  []time.Duration      // last 100 apply latencies (ring buffer)
    lastAck     map[string]time.Time // when did we last hear from each peer
}
```

**`PressureLevel() float64`** — the core output:

```
pressure = 0.4 × min(maxLag / 100, 1.0)
         + 0.4 × min(p99ApplyLatency / 500ms, 1.0)
         + 0.2 × min(maxStaleness / 2s, 1.0)

clamped to [0.0, 1.0]
```

Why these weights?
- Lag (0.4): the most direct measure of replication health. If followers are 200 entries behind, that's a serious problem.
- Apply latency (0.4): high apply latency means the storage engine is overloaded — probably because of compaction I/O competing with write operations.
- Staleness (0.2): a soft signal. If we haven't heard from a follower in 2 seconds but everything else is fine, it's a warning but not yet critical.

**`ShouldReduceIOPressure() bool`** — returns true when `PressureLevel() > 0.6`. Used by the coordinator.

**`MaxFollowerLag() int64`** — returns the maximum lag across all followers. Displayed on the dashboard.

**`UpdateLag(peer, leaderLastIndex, matchIndex)`** — called by the replication loop after each successful AppendEntries. Updates lag for that peer.

---

## Compaction coordinator

`compaction_signal.go`

The coordinator prevents simultaneous compaction across the cluster without using consensus.

```go
type CompactionCoordinator struct {
    mu             sync.Mutex
    lastCompaction map[string]time.Time  // nodeID → last compaction start
    staggerWindow  time.Duration         // default 2s
}
```

**`RecordCompactionStart(nodeID)`** — called when a node begins a compaction cycle. Gossiped to other nodes via the bridge heartbeat (soft, best-effort).

**`ShouldDefer(nodeID) bool`** — returns true if another node compacted within the stagger window. If true, the calling node delays its compaction by `DeferDuration(nodeID)`.

**Why no consensus for this?**  
Because compaction timing is a performance optimization, not a correctness requirement. If two nodes compact simultaneously, performance degrades (I/O contention, higher replication lag) but the cluster remains correct. Using Raft consensus to coordinate compaction would add write latency to every compaction decision, which is counterproductive. Best-effort gossip is sufficient.

---

## File map

| File | Purpose |
|------|---------|
| `raft.go` | `RaftNode` struct, constructor, main loop |
| `state.go` | Role enum (leader/follower/candidate), term/vote persistence |
| `log.go` | Log storage, append, truncate, access by index |
| `rpc.go` | `AppendEntries`, `RequestVote` RPC types and HTTP handler |
| `transport.go` | HTTP client for sending RPCs to peers |
| `prevote.go` | Pre-vote phase implementation |
| `statemachine.go` | Apply loop, state machine interface |
| `snapshot.go` | Snapshot creation and install infrastructure |
| `metrics.go` | `ReplicationMetrics`, `PressureLevel()` |
| `compaction_signal.go` | `CompactionCoordinator` |
| `election_test.go` | Leader election, re-election after crash |
| `replication_test.go` | Log replication, fast conflict resolution |
| `log_test.go` | Log append, truncate, index access |
| `state_test.go` | Term/vote persistence across restarts |
| `testhelpers_test.go` | In-process cluster setup for tests |

---

## Running tests

```bash
cd helios-bridge  # or wherever go.mod is
go test ./raft/... -v -timeout 30s
```

The tests spin up in-process Raft clusters with simulated network (channels instead of HTTP) for determinism and speed. Key test scenarios:

- **Election:** single node becomes leader, three-node cluster elects leader, re-election after leader crash
- **Replication:** leader replicates to followers, follower with gap catches up, fast conflict resolution
- **Persistence:** node restarts and recovers its term/vote, log entries survive restart
- **Chaos:** `cluster_test.go` in `chaos/` runs a real multi-process cluster with network partition injection
