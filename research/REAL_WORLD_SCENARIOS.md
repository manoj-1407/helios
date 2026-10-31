# Real-world scenarios

For each scenario: who hits it, what breaks, what I could verify, and how it maps onto a Helios experiment. "Verified" means I read the source page; everything else is a question to investigate, not a fact. I have not invented incident stories, frequencies or costs, and nothing here should be quoted as a statistic except where a source is named.

| # | scenario | who | what could break | evidence status | Helios test |
|---|---|---|---|---|---|
| 1 | One replica's disk gets slow but stays up | Operators of any replicated store | Commit latency if the slow replica is in the quorum path; apply backlog otherwise | Slow-but-alive hardware is documented: 101 incident reports from 12 institutions (Fail-Slow at Scale, FAST '18); 304 fail-slow cases across 248K production drives in 10 months (Perseus, FAST '23). Whether a Raft group masks it was not in the abstracts. | E1 |
| 2 | Compaction on every replica at the same time | Teams running a small Raft group over an LSM engine | Tail latency of commits and reads while a majority compacts | Unverified. ACaZoo reports a related effect for independent Cassandra nodes (about 23% simultaneous-quorum compaction with 3 nodes). | E2 |
| 3 | Compaction competes with Raft log fsync on one device | Anyone who puts the log and the data on the same volume | Commit latency, since log fsync is on the commit path | Plausible from the architecture, not measured by me. The Raft log and the engine can be configured separately in TiKV (PingCAP docs: separate raft log writer) which suggests the concern is real, but the page does not say so. | E2 on shared vs separate devices |
| 4 | Operator changes engine settings during a traffic spike | On-call engineers | Compaction thresholds change while backlog is growing | Nothing found yet. Needs incident postmortems from public sources. | Scripted config change mid-run (extension of E3) |
| 5 | Replica restarts after downtime | Anyone doing rolling upgrades | Recovery time, snapshot transfer, and replay of log into the engine | FRS (APWeb-WAIM '17) targets follower recovery in a replicated database. Helios's Snapshot/Restore path (`handleSnapshot`, `handleRestore`) buffers the whole archive in memory, so a large store would need a different design. | E3 with kill and restart |
| 6 | Storage capacity pressure | Operators on small volumes | Compaction needs temporary space; tiered compaction needs more | Unverified, general LSM knowledge only. Not tested. | later |
| 7 | Cloud block-storage throttling | Anyone on provider-managed volumes | A volume that works at full speed, then hits a throughput or IOPS cap | I have not read provider documentation. Look up each provider's published limits and burst rules and cite them. | cgroup io.max emulation (M11) |
| 8 | Budget forces lower replication or cheaper storage | Teams under cost pressure | Availability and recovery time vs spend | No data. H5 is a question, not a result. | H5, not started |

## What engineers do today, as far as I verified

* TiKV operators watch a Grafana "write stall reason" panel and raise RocksDB limits or background jobs by hand (PingCAP tuning page).
* Fail-slow drives can be detected per drive and isolated; in one production fleet isolating the drives Perseus flagged lowered node-level p99.99 latency by 48%.
* Single-node LSM tuning is automated in research systems (SILK, ADOC) and in RocksDB's own auto-tuning, which ADOC uses as a baseline.

Not verified: what CockroachDB, etcd, Consul or other Raft users do about compaction pressure on replicas. The fetch for CockroachDB's admission-control page was blocked from here.

## What must be true for a Helios result to matter outside the project

1. The effect is visible at the commit or availability level, not only in an internal queue.
2. It appears under an engine configuration someone would actually run (default memtable size, realistic key space, real values).
3. It survives a real network and separate machines. Until then results carry the label `sim-transport / shared-host`.
4. A simple operational fix (separate the log device, stagger compaction by hand) does not remove it. If it does, the contribution is a measurement and a recommendation, not a mechanism.
