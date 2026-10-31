# Research roadmap

Gates are decisions I make from data, written in advance so they cannot drift to fit the results. Effort numbers are rough working days on one person's time.

## Phase 0: stop the bleeding (about 3 days)

1. Land M1 (TCP_NODELAY) in `HeliosServer`. One line, already shown to take apply latency from 44 ms to about 1 ms.
2. Fix the jar entry-point mismatch (F11) and stop committing a binary jar; build it in the Makefile.
3. Remove or correct the claims the audit contradicts: the 200-key "triggers compaction repeatedly" sentence, the "flushes happened" sentence in `bench/results/README.md`, the unsupported novelty sentences, and the README results table, until E0 replaces it.
4. M2 to M9 from `EXPERIMENT_DESIGN.md`.

Gate 0: the new harness shows compaction actually running (nonzero flush and compaction counters, SSTable counts above the trigger) and per-op commit latency recorded. If not, stop and fix; nothing else proceeds.

## Phase 1: cheap decisive experiments (about 5 days)

* E0 (the honest redo of the original claim), pilot with 3 repetitions.
* E1 pilot: single slow follower versus none, one fsync delay level.
* Read LSM-Raft in full and the CockroachDB and TiKV flow-control material, in parallel.

Gate 1, three outcomes:

| result | action |
|---|---|
| E1 shows the quorum masking a slow follower and LSM-Raft does not already cover my questions | go to Phase 2 on H1, H2, H3 |
| E1 shows commit latency rising with one slow follower | my model of the commit path is wrong; find out why (likely `r.mu` held across fsync or goroutine pile-up) before anything else; that finding is itself the paper |
| LSM-Raft or other prior work already covers the premise | stop claiming novelty; keep Helios as an engineering portfolio project with an honest README and move the research effort to another repository from the portfolio ranking |

## Phase 2: full experiments (about 3 weeks)

E1 at full sample size, E2, E3. Fault injection through M11. Everything with at least 10 repetitions and randomized order. If the fsync injection is too coarse, add cgroup io throttling on a host that allows it.

Gate 2: at least one of H1, H2, H3 has an effect that survives (a) separating the Raft log device, (b) a real compaction workload, (c) the 95% CI excluding zero over 10 repetitions. Without that, write the negative result and stop.

## Phase 3: mechanism and policy (about 3 weeks, only if Gate 2 passes)

E4 with the backlog-driven controller against B0 to B5. Real network transport (M12) and a multi-machine run if a cloud budget is available. Compare against an unmodified mature engine under the same harness (B7) to see whether any result is an artifact of the home-made Raft or engine.

## Phase 4: decide what it is

Possible honest outcomes, in order of how much I expect each:

1. A careful measurement and negative or partial result on quorum masking and apply backlog, written as a short paper or a technical report. Publishable at a workshop level if the experiments are clean.
2. A mechanism paper on synchronized maintenance, if H2 holds and is not already known.
3. Engineering-only: the repo is a well-documented teaching and testbed system with a corrected README.

I would not target a top-tier systems conference from Helios as it stands. That could change after Gate 2.

## Things I will not do without your explicit approval

Contact researchers or maintainers, submit anything anywhere, post a public claim, open an issue or PR on someone else's project, or file anything patent-related. I have not assessed patentability; prior-art search for that is a separate task and nothing so far suggests there is anything to patent.

## What I need from you

* The full text of LSM-Raft (ACM DOI 10.1145/3769805).
* Which paper "the fail-slow follower literature" referred to.
* A run of `research/cmd/ordertest` and the Phase 1 pilot on your Dell/WSL2 machine, since the README numbers came from there and the sandbox is a different environment (2 vCPU virtual machine). Results from the two must not be pooled.
* A decision on whether the sandbox's 2 vCPU limit is enough for E1 or whether you can run the long experiments locally.
