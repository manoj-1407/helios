# Portfolio research opportunities

Prepared from the Helios audit plus a shallow look at five other repositories. Scores are my preliminary judgement, not measurements, and each row says how much I actually read. I did not open any private repository.

## Inventory (51 repositories on `manoj-1407`, as listed 2026-10-31)

| class | count | repositories |
|---|---|---|
| Forks of upstream projects (contribution workspaces or mirrors) | 28 | ripgrep, python-pathspec, go-git, pylint, igittigitt, tomlkit, node-ignore, synchronizer, operator, coreutils, nushell, kubescape-network-scanner, node-agent, clap, kubescape, zulip, regolibrary, opentelemetry-collector, website, meshery, flux2, HAMi, flux-operator, velero, HAMi-WebUI, openmrs-core, kustomize-controller, kubevela |
| Original systems and research prototypes, opened | 6 | helios (audited in full), cachemind, wbt-research, swarmfs, nivima, atlas-saga-orchestrator, live-order-book-engine (README and layout only for all but helios) |
| Original, not opened | 17 | permit-to-open, SIH-2026, sih26013, bms, ghw-agents-challenge-5, ghw-agents-challenge-6, manoj-1407 (profile), jpm-quoter-service, nexara, retail-intelligence-platform, and the private marketmind, kronos, knapsack-solver, trajex, LeetCode-Manoj, algoverse |

(The "opened" row lists 7 names because helios is counted there too; 28 + 7 + 16 = 51.)

Dormant by push date (nothing since before 2026-05-08): jpm-quoter-service, nexara, marketmind, kronos, knapsack-solver, trajex, LeetCode-Manoj, algoverse, live-order-book-engine, retail-intelligence-platform. I would not call any of them abandoned without opening them.

I have not looked at what you changed inside the forks. Whether the forked projects offer research questions is a separate pass.

## Ranked opportunities

Scores are 1 (worst) to 5 (best). "Cost" is inverted: 5 means cheap to run experiments.

| rank | project and question | novelty | significance | feasibility | cost | real-world | publication | read depth | notes |
|---|---|---|---|---|---|---|---|---|---|
| 1 | **Helios, reframed**: does quorum commit mask single-replica slow storage, and does deterministic apply synchronize compaction across replicas (H1, H2) | 3 | 3 | 4 | 4 | 3 | 3 | code read in full, experiments run | Novelty is unknown until LSM-Raft is read. A rigorous measurement study is publishable even if the mechanism is weak. |
| 2 | **CacheMind**: when does ranking accuracy fail to give better eviction decisions | 3 | 3 | 5 | 5 | 3 | 3 | README only, plus your description of the new results | The README shows wins on real traces; your notes describe a loss on a phase-shift workload. Those need reconciling before any claim. The README itself says it does not claim a new algorithm. |
| 3 | **Observability cost versus incident signal** (idea, no repo yet; you have an opentelemetry-collector fork) | unknown | 4 | 3 | 3 | 4 | 3 | nothing read | Needs a literature search first (tail-based and adaptive sampling is an existing area). Pick this only if it survives that search. |
| 4 | **Nivima**: playback-adaptive lip sync, Dravidian phoneme-viseme mapping | 4 (if the README claims hold) | 3 | 2 | 2 | 4 | 4 | README and layout only | Claims such as "the only system" are unverified. Needs human perceptual evaluation and GPU time, which are the real costs. The README names ACM Multimedia 2026; check the deadline, it may already have passed. |
| 5 | **wbt-research**: sparse-write classification in the Linux block-layer throttle | 2 | 2 | 5 | 4 | 3 | 2 | README and result summary only | Already ends in "hypothesis not supported; sign flips across runs", which is the right outcome of a careful study. Best used as the methodological reference for the other projects. Posting anything to a kernel list needs your decision. |
| 6 | **SwarmFS**: network-aware piece scheduling | 2 | 2 | 4 | 4 | 2 | 2 | README only | Peer and piece selection is a crowded area. Interesting only with an incentive or churn angle that prior work does not cover; not checked. |
| 7 | **Atlas saga orchestrator**: recovery scheduling under partial failure | 2 | 2 | 4 | 4 | 3 | 1 | README only | Workflow engines and saga patterns are mature. Treat as an engineering showcase unless a specific scheduling question emerges. |
| 8 | **Live order book engine** under adversarial traffic | 1 | 2 | 4 | 4 | 2 | 1 | README only | Market-microstructure systems work needs real exchange data to be credible. |
| 9 | **Cost per SLO of replication choices** (H5, no repo) | unknown | 4 | 2 | 2 | 4 | 3 | nothing built | Needs cited provider pricing and throttling data and a real multi-machine setup. Speculative; do after Helios H1 to H3. |

## What I would do with this

Put the effort into rank 1, with rank 2 running in parallel since it is cheap and the other Claude instance owns it. Rank 5 is already done as far as it can go and should be written up as a short negative result only if you decide to publish it. Ranks 6 to 8 are portfolio items, not research programmes, and that is fine as long as the READMEs do not claim otherwise.

The one cross-cutting pattern worth keeping: wbt-research already shows the habit of repeating runs and reporting a sign flip as a sign flip. Helios did not do that and it shows in the audit. Every other repository should adopt wbt's standard before it makes a claim.
