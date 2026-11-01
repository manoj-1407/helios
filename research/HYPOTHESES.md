# Hypotheses

## H0: the current Helios hypothesis

> Compaction strategy mismatch between a Raft leader and its followers causes measurable replication lag divergence under write-heavy workloads. (`README.md`)

Status: **not testable as built, and not supported.** The benchmark never runs compaction (F4), the "lag" it reports is a 40 ms network-stack stall (F3), the config labels do not correspond to roles (F7), and the config order is fixed and confounded with config (F9; an order effect was not reproduced on this host, see `RESEARCH_AUDIT.md`). A correct version of this experiment may still be worth running (E0 in `EXPERIMENT_DESIGN.md`), but I would not bet on it.

There is also a structural reason to doubt it. Commit needs only a majority of Raft log appends. Apply is off the commit path (`ARCHITECTURE_AND_DATA_FLOW.md`). So a follower whose compaction differs from the leader's can fall behind in apply without affecting commit latency at all, unless compaction I/O and Raft fsync compete for the same device. The original claim mixes two different effects.

## Replacement hypotheses

Each has a prediction that could fail, a number that would make me drop it, and an experiment id.

### H1: quorum masking

*One replica with slow storage does not raise commit latency in a 3-node group. Commit latency degrades only when a majority of replicas are slow in the same interval.*

Prediction: with injected fsync delay `d` on the Raft log device of one follower, commit p99 stays within noise of the no-fault baseline for any `d`. With the same delay on the leader and one follower, commit p99 rises with `d`.
Drop if: the single-slow-follower case raises commit p99 by more than 20% over baseline with non-overlapping bootstrap CIs. That would mean something other than quorum arithmetic couples followers to the leader (leader-side goroutine pile-up, `r.mu` held across fsync, the 100 ms RPC timeout).
Test: E1.
Status (2026-11-01): **held** for one slow follower at 10 and 40 ms, 15 writes/s open loop, 36 runs (`RESEARCH_AUDIT.md`, Round 7). Not tested at delays near or above the RPC timeout, under load, or with a real network.

### H2: synchronized maintenance

*Replicas applying the same log flush and compact at nearly the same log positions, so the chance that a majority is compacting at once is much higher than for independent nodes.*

Prediction: with identical engine settings and a deterministic workload, the log index at which each replica starts its first N flushes differs by less than 1% across replicas; the fraction of time a majority is in compaction is well above what independent random phases would give. Staggering (the unused `CompactionCoordinator` idea, actually wired in) lowers that fraction and lowers commit p99 when the Raft log and the data share a device.
Drop if: replicas' flush indices diverge by more than 10% (nondeterministic timing dominates), or staggering gives no commit p99 gain with 10 repetitions.
Test: E2.
Prior art to beat: ACaZoo reports about 23% simultaneous-quorum compaction for independent Cassandra nodes. The comparison is only meaningful once I have read it fully.
Relevance caveat: systems with many Raft groups per node (multi-Raft) dilute synchronization because different groups flush at different times. H2 mostly matters for single-group or few-group deployments.

### H3: apply backlog is the real replication cost of background maintenance

*Compaction on a follower shows up as growth in `CommitIndex - LastApplied`, with consequences for follower reads, failover and restart time, but not for commit latency.*

Prediction: with real compaction running (large key space, realistic values), follower apply backlog p99 rises during compaction windows and drains afterwards; time-to-serve after a leader change grows with the backlog at the moment of failure.
Drop if: backlog stays below 10 entries p99 in every configuration (the engine keeps up, nothing to study).
Test: E3.

### H4: replication-state feedback beats local-signal feedback

*A controller that throttles compaction from the replica's own apply backlog reduces backlog tail more than the static strategies and more than a load-aware controller in the style of SILK.*

Prediction: apply-backlog p99 and commit-to-visible p99 under policy P_backlog are lower than under static leveled, static tiered, adaptive (classifier only) and a client-load-aware baseline, with no worse write throughput.
Drop if: P_backlog does not beat the best baseline on p99 backlog with a CI that excludes zero over 10 repetitions, or it loses more than 10% throughput.
Test: E4. Requires fixing the pressure loop first (F10).

### H5: cost view (stretch, not started)

*Choosing replication factor, storage tier and compaction aggressiveness from cost per SLO-hour gives different, cheaper choices than throughput-only tuning.*

Nothing here is measured. It needs cloud price and throttling data from provider documentation, collected and cited, not recalled. Do not pursue until H1 to H3 are settled.

## Ordering

H1 is cheapest and decides whether anything downstream is interesting: if followers are not masked, the whole model changes. H2 is the most novel candidate if it holds and also the easiest to get scooped on, so check LSM-Raft first. H3 is a measurement study and is useful even if it is negative. H4 only makes sense after H3 shows a real backlog problem.


## Status after the load and timeout experiments (2026-11-01)

* H1 (quorum masking): holds at low load for follower delays from 25 to 400 ms (commit p50 unchanged, no elections). It fails at 2,000 writes/s for any follower delay of 25 ms or more on this implementation because replication has no in-flight limit (resource exhaustion, not quorum arithmetic); a one-in-flight-per-peer option restores masking (stage 2a, 2b). Details in `RESEARCH_AUDIT.md`, Round 8b.
* New H6 (storage-induced leadership loss under load): a leader with fsync delay far below the election timeout loses leadership near its fsync-limited capacity. Supported by stage 5 (about 1 election per run at rho 1.0, none at rho 0.9 and below, delays 10 and 25 ms). Mechanism evidence is correlational. Falsifier and next test: move fsync off `r.mu` (or heartbeats off the lock) and repeat at rho 0.5, 0.9, 1.0, 1.2 with 10 repetitions; the hypothesis is dropped if elections at rho 1.0 do not fall markedly. Not yet searched against the literature.
* H2 to H4 (compaction synchronization, backlog, feedback control) are untouched by these experiments.
