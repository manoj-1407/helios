# Helios — Architecture

This document describes how all components of Helios fit together, why the system is structured this way, and what contracts exist between layers. Read this before reading any component-level README.

---

## System overview

Helios is a distributed key-value store. Distributed means data is replicated across multiple machines. Key-value means the API is simple: store a value at a key, retrieve it, delete it. The system guarantees that writes acknowledged to the client are never lost, even if a minority of nodes crash simultaneously.

The system has six major components:

```
┌──────────────────────────────────────────────────────────────────────┐
│                          helios binary                                │
│                                                                       │
│  ┌─────────────┐   ┌──────────────────┐   ┌──────────────────────┐  │
│  │ RESP server │   │  Raft consensus  │   │   Live dashboard     │  │
│  │  (TCP :6379)│   │  (Go)            │   │   (HTTP :8080, SSE)  │  │
│  └──────┬──────┘   │                  │   └──────────────────────┘  │
│         │          │  • pre-vote      │             ▲               │
│         ▼          │  • log repl.     │             │ cluster        │
│  ┌─────────────┐   │  • snapshots     │             │ snapshots      │
│  │ helios SDK  │   │  • metrics       │             │ every 1s       │
│  │ client (Go) │   └────────┬─────────┘             │               │
│  └──────┬──────┘            │                       │               │
│         │                   │ pressure              │               │
│         │                   │ loop (500ms)          │               │
│         └─────────┬─────────┘                       │               │
│                   │                                                  │
│                   ▼                                                  │
│  ┌────────────────────────────────────────────────────────────────┐ │
│  │                     helios-kv  (JVM subprocess)                │ │
│  │                                                                │ │
│  │   PUT /put    GET /get    DEL /delete    POST /pressure        │ │
│  │         ↓           ↓            ↓               ↓            │ │
│  │      HeliosDB                          PressureReceiver        │ │
│  │         ↓                                        ↓            │ │
│  │      WAL → MemTable → flush → SSTables           ↓            │ │
│  │                                          CompactionStrategy    │ │
│  │                        CompactionManager ← WorkloadClassifier  │ │
│  └────────────────────────────────────────────────────────────────┘ │
│                                                                       │
│  eBPF kprobe (kernel) — traces sys_write / sys_fsync on helios PIDs  │
└──────────────────────────────────────────────────────────────────────┘
```

---

## Component responsibilities

### helios-kv (Java)

The storage engine. Owns all data on disk. Accepts HTTP from the Go layer. Knows nothing about Raft, replication, or other nodes — it is a single-node embedded database exposed over HTTP.

**Input:** HTTP requests on its configured port  
**Output:** HTTP responses; files on disk (WAL, SSTables)  
**State:** MemTable (RAM), WAL + SSTables (disk)

### Raft consensus (Go)

Implements distributed agreement. Decides the order in which writes are applied across all nodes. Elects a leader. Replicates log entries. Handles node crashes and network partitions.

**Input:** AppendEntries / RequestVote RPCs from peers; Apply commands from the KV server layer  
**Output:** committed log entries (applied to state machine); replication metrics  
**State:** Raft log (disk), current term + vote (disk), match/next indexes per peer (memory)

### Bridge (Go)

Translates between the Go world (raft-committed entries, pressure metrics) and the Java world (HTTP endpoints). Owns the `/pressure` feedback loop.

**Input:** committed KV commands from Raft state machine; pressure floats from ReplicationMetrics  
**Output:** HTTP calls to helios-kv  
**State:** none (stateless translator)

### RESP server (Go)

Accepts TCP connections from any Redis client. Parses RESP protocol. Routes commands to the helios SDK client.

**Input:** TCP bytes in RESP format  
**Output:** TCP bytes in RESP format  
**State:** none

### helios SDK client (Go)

Knows the address of all nodes. Routes writes to the current leader (retries on 503). Routes reads to any healthy node.

**Input:** Put/Get/Delete calls from RESP server  
**Output:** HTTP calls to helios-kv bridge on the appropriate node  
**State:** last known leader address

### Dashboard (Go)

Polls all nodes via their bridge `/stats` endpoint every second. Streams snapshots to browsers over SSE.

**Input:** HTTP GET /stats from each node's bridge  
**Output:** SSE stream of JSON snapshots; one self-contained HTML page  
**State:** 300-entry ring buffer of historical snapshots (5 minutes)

---

## Data flow: a write

```
redis-cli SET foo bar
     │
     ▼ TCP, RESP protocol
RESP server (Go)
     │ client.Put(ctx, "foo", "bar")
     ▼
helios client — finds leader addr
     │ HTTP POST /put  body: "foo\nbar"
     ▼
helios-kv HeliosServer (JVM, leader node)
     │ db.put(key, value)
     ▼
WAL.append(entry)          ← written to disk first
     │
MemTable.put(key, value)   ← then memory
     │
HTTP 200 OK
     │
     ▼
RESP server → "+OK\r\n" → redis-cli
```

The Raft layer is not in this path for a single-node deployment. In a multi-node cluster, the write first goes through the KV server layer which submits it to Raft; only after Raft commits it to a majority do we call `db.put`.

---

## Data flow: the pressure loop

```
Every 500ms:
  Go pressure loop reads RepMetrics.PressureLevel()
     → 0.4 × (max follower lag / 100)
     + 0.4 × (P99 apply latency / 500ms)
     + 0.2 × (staleness / 2s)
     clamped to [0.0, 1.0]

  If |newPressure - lastSent| > 0.1:
    HTTP POST /pressure  body: "0.7300"
         ↓
    ReplicationPressureReceiver.update(0.73)
         ↓
    PressureState = HIGH
         ↓
    compactionAggressiveness() = 0.3
         ↓
    LeveledCompaction trigger = ceil(3 / 0.3) = 10 tables
    (normally fires at 3 tables — now waits for 10)
```

---

## Process model

The helios binary is a single Go process. It spawns the JVM as a **subprocess** (`exec.Command("java", "-jar", ...)`). The JVM process is not managed by the OS process supervisor — if the Go binary dies, it kills the JVM via a deferred `jvmCmd.Process.Kill()`.

This means:
- one `helios` process per node
- one JVM subprocess per helios process
- Go ↔ Java communication is local HTTP (loopback, ~0.1ms RTT)
- the JVM is never exposed directly to external clients

---

## Persistence model

**What is durable:**
- Every write appended to the WAL before being acknowledged
- Raft log entries (written to disk before AppendEntries responds)
- Raft term + vote (written to disk before any election response)

**What is in memory only:**
- MemTable (recovered from WAL on restart)
- In-flight Raft state (commitIndex, lastApplied — derived from durable log on restart)
- Dashboard history ring buffer

**Recovery sequence on restart:**
1. Go process starts, spawns JVM
2. JVM replays WAL from last flush checkpoint → reconstructs MemTable
3. Go process health-checks JVM (retries 60× with 500ms sleep)
4. Raft node starts, reads term+vote from disk, reads log from disk
5. Raft replays log entries not yet applied to the state machine
6. Pressure loop starts
7. RESP server and dashboard start accepting connections

---

## Network topology (3-node cluster)

```
        ┌─────────────────────────────────────────────┐
        │                   clients                    │
        │     redis-cli / any Redis SDK / browser      │
        └────────┬──────────────────────┬──────────────┘
                 │ RESP :6380           │ HTTP :8080
                 ▼                      ▼
         ┌──────────────┐      ┌──────────────┐
         │    node1     │      │   dashboard  │
         │   (leader)   │      │  (any node)  │
         └──────┬───────┘      └──────────────┘
         Raft   │ :8001
         RPCs   ├──────────────────────┐
                ▼                      ▼
         ┌──────────────┐      ┌──────────────┐
         │    node2     │      │    node3     │
         │  (follower)  │      │  (follower)  │
         └──────────────┘      └──────────────┘
              :8002                  :8003
```

Each node runs its own JVM subprocess on its own KV port. The Raft RPCs go directly between Go processes. The RESP server on any node can accept client connections (the client SDK routes writes to the leader automatically).

---

## Extension points

**Adding a new compaction strategy:**  
Implement `CompactionStrategy` in Java. Register it in `CompactionManager`. The `WorkloadClassifier` can route to it based on workload shape.

**Adding a new RESP command:**  
Add a case to `redis_server.go`'s command dispatch. Map it to a `client.Client` call or a direct bridge call.

**Adding a new Raft metric to the pressure formula:**  
Add a field to `ReplicationMetrics`, update `PressureLevel()`, adjust weights. The pressure loop will automatically pick it up without changes anywhere else.

**Replacing the JVM storage engine:**  
Implement the same HTTP API (`/put`, `/get`, `/delete`, `/health`, `/stats`, `/pressure`) in any language. Point `-jar` at a shell script that starts your process. The Go layer is entirely decoupled.
