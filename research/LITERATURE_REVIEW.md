# Literature review

Status: first pass, 2026-10-31. Every entry in `LITERATURE_MATRIX.csv` has a verification column saying how much of the source I actually read. Most are abstract or landing pages, not full papers. Nothing here is a novelty claim.

## What I could verify, and how

Fetched and summarized: SILK (ATC '19), ADOC (FAST '23), Perseus (FAST '23), Fail-Slow at Scale (FAST '18), Copilot (OSDI '20), ACaZoo, the DepFast HotOS abstract, FRS follower recovery (APWeb-WAIM '17), the 2025 LSM survey abstract, PingCAP's TiKV tuning page.

Could not read: LSM-Raft (title and ACM DOI 10.1145/3769805 turned up in search, the ACM page would not load here). CockroachDB's admission-control docs (blocked by the fetch allowlist). I did not read the full text of any paper above, only what the venue pages and one PDF summary expose.

Your earlier notes mention SILK and "the fail-slow follower literature". I do not know which exact paper that second reference was. The closest candidates I found are Copilot (OSDI '20), the DepFast HotOS paper, Perseus and Fail-Slow at Scale. Tell me which one you had in mind.

## Four threads that matter to Helios

### 1. LSM interference and write stalls on a single node

SILK frames LSM tail latency as interference between client writes, flushes and compactions and fixes it with a scheduler that reacts to client load. ADOC reframes write stalls as data overflowing a component faster than it drains and tunes threads and batch size to keep flow balanced; it reports beating SILK on its synthetic workload. Both control background work from local signals. Helios's `ReplicationPressureReceiver` is the same idea with a different signal (follower lag). Nothing I read tests whether a replication signal predicts trouble better than local signals do, and that comparison is the minimum needed for the "feedback loop" to count as a contribution.

### 2. Slow-but-alive replicas in replicated state machines

Copilot shows that a slow leader or slow replica inflates Multi-Paxos latency roughly in proportion to the slowdown, and buys tolerance with a second distinguished replica at an 8% to 13% throughput cost when nothing is slow. The DepFast HotOS paper argues slowness tolerance needs programming support, and builds an RSM with it. Fail-Slow at Scale and Perseus establish that slow hardware is a recurring production phenomenon (101 incident reports from 12 institutions; 304 fail-slow cases among 248K monitored drives over 10 months). All of this models slowness as pauses, network delay or degraded devices. None of the abstracts model slowness that the system itself creates through background storage work.

### 3. Compaction coordination across replicas

ACaZoo (SRDS 2014, venue inferred from search-result URLs) is the closest prior art I found. It moves leadership away from a node that is about to compact, and it reports that the trick helps less when a quorum compacts at once (about 23% of the time with 3 nodes in its setting). That measurement is for independently scheduled Cassandra nodes. In a deterministic state machine that applies the same log everywhere, flush and compaction triggers can line up across replicas by construction, so simultaneous compaction may be the normal case rather than a 23% event. I have not seen this stated or measured. It is a hypothesis (H2 in `HYPOTHESES.md`), not a finding.

### 4. Raft over an LSM store

TiKV's documentation describes separate raftstore and apply pools and a separate Raft-log writer, and treats RocksDB write stalls as a symptom to diagnose by hand; the page I read has no automatic link from compaction state to Raft. LSM-Raft appears to address this area directly and I could not read it. Until I have, I cannot say whether the Helios premise is already covered.

## Claims in the Helios docs that I cannot support yet

`store/README.md` calls the replication-to-compaction feedback loop "the core novel contribution" and `ReplicationPressureReceiver.java` says RocksDB, LevelDB, TiKV and Cassandra "none of them have a feedback loop from the replication layer". I found no source for that statement and one possible counterexample (LSM-Raft, unread). Remove the sentence from the docs until it is checked.

## Next reading, in order

1. LSM-Raft in full (get the PDF through your university access).
2. CockroachDB's admission control and replication flow control documentation, and the Pebble design notes.
3. TiKV flow control (RFCs and source), since the PingCAP tuning page does not cover it.
4. ACaZoo in full, plus papers that cite it (forward citation search on Semantic Scholar).
5. The full text of SILK and ADOC, for their exact workloads and the signals their controllers use.
6. A systematic pass over FAST, OSDI, SOSP, EuroSys, ATC, VLDB and SIGMOD for "replica", "compaction" and "Raft" co-occurrences from 2019 to 2026. I have not done this. I used web search only, not ACM DL, IEEE Xplore, Google Scholar or Semantic Scholar directly.
