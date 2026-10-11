# helios-raft

A Raft consensus engine in Go, built against `helios-raft-spec.md`'s
design. This README reflects what's actually built and passing right
now — Week 1 (single-node election) and Week 2 (3-node clusters,
replication, partition/crash recovery) are complete. Membership changes,
snapshots, and the gRPC bridge to helios-kv are Week 3 scope and are not
implemented (their RPC structs exist as stubs so the code compiles
against the eventual interface; `TakeSnapshot`/`HandleInstallSnapshot`
return explicit "not implemented" errors rather than silently no-opping).

## Status

- [x] **Week 1** — PersistentState, RaftLog, RPC structs, pre-vote, single-node election
- [x] **Week 2** — in-process channel transport, 3-node elections, heartbeats, log replication, fast conflict resolution, network partition simulation, node crash/recover
- [ ] **Week 3** — joint-consensus membership changes, real snapshotting, the gRPC bridge to helios-kv, the actual thesis benchmark (`RunReplicationBench`)

18/18 tests passing, including under `go test -race` (450 normal-mode
test executions + 225 race-mode executions, run repeatedly during
development specifically to chase down intermittent failures — see
below — with zero remaining failures or detected data races at time of
writing).

## Architecture

```
raft/
  state.go        PersistentState (term, votedFor) with atomic fsync'd persistence
  log.go          RaftLog: append-only WAL-style file, byte-offset tracked for
                  physical (not just in-memory) truncation, crash-tolerant replay
  rpc.go          AppendEntries / RequestVote structs
  prevote.go      Pre-vote phase + PreVoteRequest/Response
  transport.go    Transport interface; ChannelTransport (in-process, for tests)
  statemachine.go StateMachine interface; MockStateMachine (for tests --
                  the real implementation is the eventual helios-kv bridge)
  snapshot.go     RPC structs only; TakeSnapshot/HandleInstallSnapshot
                  return explicit not-implemented errors (Week 3)
  raft.go         RaftNode: election, heartbeats, replication, commit
                  advancement, the applier goroutine

chaos/
  network.go      SimulatedNetwork: partition / delay / drop-rate fault injection
  harness.go      Cluster: spins up N in-process RaftNodes over a shared
                  ChannelTransport for fast, deterministic multi-node tests
```

### Why one file per SSTable in helios-kv, and the equivalent decision here

The Raft log is one append-only file with byte offsets tracked
in-memory (`offsets[i]` = where entry `i` starts), so `TruncateFrom`
can call the OS `Truncate` syscall directly at the right byte instead of
rewriting the file. Same reasoning as helios-kv's WAL: a torn trailing
record at the tail is the expected shape of an interrupted write, not a
bug, so `replay()` stops there silently instead of throwing — the same
pattern, in the same words, because it's the same problem in a different
language.

### Locking discipline

Every mutable `RaftNode` field is protected by one mutex (`r.mu`).
Methods named with a `Locked` suffix (`becomeFollowerLocked`,
`resetElectionTimerLocked`, `maybeAdvanceCommitIndexLocked`,
`lastIndexForTermLocked`, `findConflictLocked`) assume the caller already
holds `r.mu` and never acquire or release it themselves — that's what
makes them safely callable from inside another already-locked method
without a self-deadlock. The one rule that has to hold everywhere else:
never call out through `Transport` while holding `r.mu`. Every RPC,
sent or received, releases the lock first. That's what stops two nodes
calling into each other synchronously while each holds its own lock from
deadlocking against each other.

## Real bugs found during development (not hypothetical)

Three of these were caught by writing tests and then running them
*hundreds* of times under load, not by reading the code:

### 1. Stale pre-vote term-stacking (raft.go, `runElection`)

Pre-vote is a blocking, network-RPC-bound phase that can take up to an
RPC timeout to resolve. The original code read `CurrentTerm` once at the
very start, ran the blocking pre-vote round, and then *unconditionally*
did `CurrentTerm++` once it returned — without checking whether anything
had changed `CurrentTerm` while it was waiting.

It can, and did: under a 3-node startup race (two nodes' election timers
firing close together), node A could legitimately grant a real vote to
node B *while A's own pre-vote round was still in flight*. When A's
pre-vote then returned, A proceeded to increment its *already-bumped*
term again and become a candidate — silently discarding the vote it had
just legitimately cast, and stacking a spurious second election on top
of a cluster that had already converged. Traced with sequence-numbered
debug logging across concurrent goroutines until the exact interleaving
was visible; fixed by snapshotting the term before pre-vote and
verifying it's unchanged afterward, abandoning the attempt if not.

### 2. Missing election timer on a specific step-down path (raft.go, `becomeFollowerLocked`)

A candidate that saw a higher term in a vote *response* (not a request)
stepped down to Follower correctly, but that specific call site never
re-armed the election timer. A node that's a Follower with no live timer
can never detect a future leader failure on its own — a real liveness
bug, not a safety one, but still a node that could get permanently stuck
relying on someone else's heartbeats forever. Fixed by moving the timer
reset into `becomeFollowerLocked` itself (always arms a fresh timer on
every transition to Follower) instead of leaving it to individual call
sites, which is what let one get missed in the first place.

### 3. Unsynchronized field exposed on the test-only MockStateMachine

`go test -race` caught a genuine data race: `MockStateMachine.Apply()`
correctly mutated its applied-entries list under its own mutex, but the
field was exported directly, and a test polled it with no synchronization
at all. Fixed by making the field private and adding `AppliedCount()` /
`AppliedEntries()` accessors that copy under lock. Small, but exactly the
shape of bug `-race` exists to catch — it would never reliably show up as
a test failure, only as genuinely undefined behavior under the right
(or wrong) scheduling.

### 4. (Test-only) An artificial fault-injection test had its own race

`TestFastConflictResolution` injects fake high-term log entries directly
into a follower's log file to simulate divergence, bypassing the timer
reset a real AppendEntries would have triggered. In a rare timing window,
the follower's own *pre-existing* election timer could fire before the
leader's corrective heartbeat arrived, and the follower could win a real
election on the strength of a term nobody actually voted it into. Not a
protocol bug — a log entry's term is *supposed* to look authoritative;
the only reason this could happen is that the test fabricated entries
directly instead of simulating a real prior election. Fixed by resetting
the follower's timer immediately after injection, which is white-box
test code reaching into the package directly (same module, intentional).

## Known limitations

- **No membership changes.** Joint consensus (`membership.go`) isn't
  built. Cluster size is fixed at construction time.
- **No real snapshotting.** `TakeSnapshot`/`HandleInstallSnapshot` return
  explicit errors. This isn't a problem yet only because nothing
  truncates the log either — once compaction-driven log truncation is
  added, a follower that falls far enough behind to need entries that
  have already been truncated away will have nowhere to catch up from
  except a snapshot that doesn't exist yet.
- **No gRPC transport.** Only the in-process `ChannelTransport` exists.
  Real cross-process Raft needs an actual network transport
  implementing the same `Transport` interface — that's specifically
  designed to be a drop-in swap, not a rewrite, but it hasn't been
  written.
- **No bridge to helios-kv.** `StateMachine` is implemented only by
  `MockStateMachine` (an in-memory map for tests). The real
  implementation — a gRPC sidecar into the actual Java process — is
  Week 3 scope.
- **Timing-sensitive tests use compressed timeouts for speed**, which
  showed measurable (if low, ~1-5%) flakiness under `go test -race`'s
  instrumentation overhead specifically, fixed by widening the test
  margin. This is a property of testing real wall-clock timers fast,
  not a correctness gap — but it's listed here rather than glossed over.

## What's next

The actual thesis — "adaptive compaction in helios-kv reduces
replication lag divergence compared to static leveled compaction" —
needs, in order: the gRPC bridge (`store/bridge.go`), `BridgeMetrics`
(the instrument that actually measures apply latency against compaction
events), and `RunReplicationBench`'s three-config comparison (adaptive
both / mismatched / static leveled both). None of that can produce a
real number until the bridge exists. Building it is the next concrete
step, not the eBPF layer — eBPF measures the interaction once the
interaction exists to measure; it can't substitute for the bridge.
