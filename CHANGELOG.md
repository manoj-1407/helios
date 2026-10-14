# Changelog

All notable changes to Helios are documented here. Entries are in reverse chronological order.

---

## [1.0.0] — 2026-10-12

Initial public release.

### Added

**Storage engine (helios-kv)**
- LSM-tree storage engine from scratch in Java 21
- Write-Ahead Log (WAL) with crash recovery via `RecoveryManager`
- In-memory `MemTable` backed by `TreeMap` for sorted key access
- SSTable format with sparse index and Murmur3 bloom filter
- Leveled compaction strategy (L0–Ln, bounded read amplification)
- Tiered compaction strategy (size-grouped runs, low write amplification)
- FIFO compaction strategy (time-series and cache workloads)
- Workload classifier with hot-swap between strategies
- `ReplicationPressureReceiver` singleton — adjusts compaction aggressiveness based on Raft lag
- HTTP server exposing `/put`, `/get`, `/delete`, `/health`, `/stats`, `/pressure`

**Consensus (helios-raft)**
- Raft consensus from scratch in Go 1.22
- Leader election with randomized timeouts
- Pre-vote extension (etcd-style) — prevents partitioned nodes from disrupting stable clusters
- Log replication with fast conflict resolution (term-based backoff)
- Persistent term + vote (survives restarts)
- Snapshot infrastructure
- `ReplicationMetrics` — per-peer lag, P99 apply latency, staleness
- `PressureLevel()` — weighted formula producing a 0.0–1.0 replication pressure score
- `CompactionCoordinator` — best-effort staggering of compaction across nodes

**Bridge and feedback loop**
- `HeliosKVBridge` — HTTP client over the JVM API
- `HeliosKVServer` — Raft state machine applying committed entries to the bridge
- `CompactionPressureLoop` — goroutine polling Raft metrics every 500ms, pushing pressure to JVM
- On-shutdown pressure reset (pushes 0.0 so JVM doesn't restart in high-pressure state)

**Client**
- Go SDK client (`client.Client`) with leader routing and automatic retry on 503
- RESP server (`RESPServer`) implementing SET, GET, DEL, PING, COMMAND, QUIT
- RESP array format and inline format both supported

**Dashboard**
- Live web dashboard with Server-Sent Events streaming (no polling, no WebSocket)
- Cluster nodes panel: role, term, commit index, SSTable count
- Compaction strategy panel: strategy name, write amplification
- Replication pressure bars per node
- Write amplification chart: 5-minute rolling canvas chart
- Self-contained single HTML page — no CDN, no build step

**eBPF tracer**
- `sys_write` kprobe — bytes written per syscall
- `sys_fsync` kprobe + kretprobe — fsync duration
- Userspace correlator and Prometheus exporter
- Filter by helios process name

**Operations**
- Single binary entrypoint (`cmd/helios`) with flag-based configuration
- Automatic JVM startup and health check (60 retries × 500ms)
- Graceful shutdown: SIGINT/SIGTERM → pressure reset → JVM kill

**Testing**
- Raft unit tests: election, replication, persistence, fast conflict resolution
- Java unit tests: MemTable, WAL, HeliosDB, compaction strategies
- Chaos test suite: leader crash, follower partition, split brain, restart under load
- Chaos network emulator: drop, delay, partition, heal
- Benchmark suite: replication benchmark, cluster stress benchmark

**Documentation**
- `README.md` — project overview, novel contribution, prior art comparison, quickstart
- `ARCHITECTURE.md` — full system design, data flows, process model
- `helios-kv/README.md` — storage engine internals
- `raft/README.md` — consensus algorithm, pre-vote, metrics
- `store/README.md` — bridge protocol, pressure loop
- `client/README.md` — Go SDK, RESP protocol
- `dashboard/README.md` — SSE, reading the panels
- `ebpf/README.md` — build, tracing, Kubernetes
- `bench/README.md` — benchmark usage, interpreting results
- `chaos/README.md` — chaos test descriptions, harness usage
- `CONTRIBUTING.md` — build, test, extension guide
- `CHANGELOG.md` — this file

---

## Roadmap

Items planned for future releases, roughly in priority order:

### [1.1.0] — Range scans

- `SCAN start end limit` RESP command
- Bridge `/scan` endpoint in helios-kv
- SSTable range iterator (already supported internally, needs HTTP exposure)

### [1.2.0] — Snapshot installation

- Complete the install-snapshot RPC in the Raft transport
- Wire snapshot transfer to the bridge for cross-node state transfer
- Required for new nodes joining a long-running cluster

### [1.3.0] — Prometheus metrics

- `/metrics` endpoint on each node
- Metrics: write throughput, read throughput, P99 latency, SSTable count, compaction cycles, replication lag
- Grafana dashboard template

### [1.4.0] — Authentication

- Token-based auth on the RESP server (`AUTH` command)
- Mutual TLS between Raft nodes
- API key for the bridge HTTP endpoints

### [2.0.0] — Multi-Raft sharding

- Partition keyspace across multiple Raft groups
- Consistent hashing for key assignment
- Cross-shard transactions (two-phase commit)
- Horizontal scalability beyond a single Raft group's throughput ceiling
