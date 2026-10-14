# helios-kv — Storage Engine

helios-kv is a from-scratch LSM-tree storage engine written in Java 21. It is the data layer of the Helios distributed KV store. It runs as a standalone HTTP server — the Go layer talks to it over localhost HTTP.

---

## Contents

1. [What is an LSM-tree](#what-is-an-lsm-tree)
2. [Write path](#write-path)
3. [Read path](#read-path)
4. [SSTable format](#sstable-format)
5. [Compaction](#compaction)
6. [Workload classifier](#workload-classifier)
7. [Replication pressure receiver](#replication-pressure-receiver)
8. [Recovery](#recovery)
9. [HTTP API](#http-api)
10. [Configuration](#configuration)
11. [Building and running standalone](#building-and-running-standalone)
12. [Running tests](#running-tests)

---

## What is an LSM-tree

An LSM-tree (Log-Structured Merge-tree) is a data structure optimized for write-heavy workloads. The core insight: sequential disk writes are orders of magnitude faster than random writes. An LSM-tree turns all writes into sequential appends.

The trade-off: data ends up fragmented across many files. Over time, those files must be merged and sorted to keep reads fast. This merging is called **compaction**.

LSM-trees are used by: Google Bigtable, Apache Cassandra, RocksDB (used by MySQL, TiKV, CockroachDB), LevelDB, HBase, ScyllaDB.

---

## Write path

Every write goes through three stages in order:

### 1. WAL (Write-Ahead Log)

`WAL.java`

Before anything else, the write is appended to the WAL — a simple sequential log file on disk. If the process crashes after the WAL write but before anything else, the write can be replayed on restart.

WAL format: `[8-byte key length][8-byte value length][key bytes][value bytes]`

For deletes, a tombstone marker (value length = -1) is written.

The WAL is append-only. It is never modified. It is only read during recovery.

### 2. MemTable

`MemTable.java`

The write goes into the MemTable — an in-memory sorted map backed by Java's `TreeMap<byte[], byte[]>`. The `TreeMap` keeps keys in sorted order, which makes it efficient to iterate a key range and to flush in sorted order.

The MemTable has a size limit (default 4MB). When it exceeds this limit, it is **frozen** (made immutable) and scheduled for flushing to disk.

### 3. SSTable flush

When a MemTable is frozen, it is flushed to disk as an SSTable — a sorted, immutable file. The flush is a sequential scan of the `TreeMap` written out in order. After the flush, the WAL segment corresponding to this MemTable can be deleted.

---

## Read path

Reads check layers in order, newest to oldest:

1. **Active MemTable** — O(log n) lookup in TreeMap
2. **Immutable MemTable** (if one is being flushed) — same
3. **SSTables, newest first** — for each:
   a. **Bloom filter check** — if the filter says "definitely not here", skip this file (O(1))
   b. **Sparse index lookup** — binary search the index to find which block might contain the key
   c. **Block read** — read and decompress that block, scan for the key

In the worst case (key exists only in the oldest SSTable), this is O(SSTables × log(blocks_per_file)). Leveled compaction keeps this bounded to O(levels) by ensuring at most one file per level overlaps any key range.

---

## SSTable format

`SSTableWriter.java`, `SSTableReader.java`, `SSTable.java`

Each SSTable is a file on disk with this layout:

```
┌─────────────────────────────────────────────┐
│  Data block 0  (4KB, sorted key-value pairs) │
├─────────────────────────────────────────────┤
│  Data block 1                                │
├─────────────────────────────────────────────┤
│  ...                                         │
├─────────────────────────────────────────────┤
│  Data block N                                │
├─────────────────────────────────────────────┤
│  Sparse index  (one entry per block)         │
│  entry: [first key in block → block offset]  │
├─────────────────────────────────────────────┤
│  Bloom filter  (Murmur3, false positive ~1%) │
├─────────────────────────────────────────────┤
│  Footer: [sparse index offset][filter offset]│
│           [magic bytes]                      │
└─────────────────────────────────────────────┘
```

**Why a sparse index instead of a full index?**  
A full index has one entry per key. For a file with 1 million keys at 32 bytes each, that's 32MB just for the index. A sparse index with one entry per 4KB block has ~250 entries for the same file — tiny enough to keep in memory. The trade-off is that after finding the right block, we must scan within that block (at most 4KB, fast).

**Why Bloom filters?**  
A Bloom filter answers "is this key definitely NOT in this file?" in O(1) with very low false positive rate (~1%). For a GET on a key that doesn't exist, without a Bloom filter we would need to open and scan every SSTable. With filters, we skip all of them instantly. This makes non-existent key lookups fast — critical because "cache miss" workloads (checking for keys that aren't there) are common.

**Murmur3 hash:**  
The Bloom filter uses Murmur3 (`MurmurHash3.java`) — a non-cryptographic hash function optimized for speed and distribution quality. Two independent Murmur3 seeds simulate k independent hash functions without k actual hash computations.

---

## Compaction

`CompactionManager.java`, `CompactionStrategy.java`, `LeveledCompaction.java`, `TieredCompaction.java`, `FIFOCompaction.java`, `CompactionMerge.java`, `CompactionPlan.java`

Compaction is the process of merging and sorting SSTables to keep read performance bounded and reclaim space used by deleted or overwritten keys.

### Why compaction is necessary

Every flush creates a new SSTable. Without compaction:
- SSTables accumulate indefinitely
- Read amplification grows: checking 1000 files for every GET
- Space amplification: deleted keys still exist in old files, consuming disk space
- Duplicate keys across files: the same key may appear in many SSTables (each overwrite creates a new entry)

Compaction merges files, keeps only the latest value per key, and removes tombstones (deleted keys).

### Leveled compaction

Best for: **read-heavy workloads**, general purpose

Files are organized into levels L0 through Ln. Each level has a size budget that increases geometrically (e.g., L1=10MB, L2=100MB, L3=1GB). L0 allows overlapping key ranges (files are written in flush order). L1 and above have no key range overlap within a level — any key exists in at most one file per level.

When a level exceeds its budget, the compaction manager picks the most overlapping file, merges it with all overlapping files in the next level, and writes the result to the next level.

**Read amplification:** O(levels) — at most one file per level needs checking.  
**Write amplification:** ~10–30x — each byte is rewritten once per level it passes through.  
**Space amplification:** low — old versions are compacted away quickly.

The trigger threshold in Helios is `BASE_TRIGGER_TABLE_COUNT = 3` for L0. This is adjusted by the replication pressure receiver (see below).

### Tiered compaction

Best for: **write-heavy workloads**

Files are grouped into "tiers" by size. Files of similar size form a run. When a run has too many files (default trigger: 6), they are merged into one larger file, which becomes a candidate for the next tier.

No overlapping files are merged across tiers — the merge is always within one tier. This means fewer bytes rewritten per operation (lower write amplification), but more files to check on reads (higher read amplification).

**Read amplification:** O(tiers × files per tier)  
**Write amplification:** ~3–10x  
**Space amplification:** higher than leveled — old versions may persist longer

### FIFO compaction

Best for: **time-series workloads, cache workloads**

No merging. When total SSTable disk usage exceeds the configured limit, the oldest files are simply deleted. Writes are never rewritten — zero write amplification.

This is only correct for workloads where old data is expected to expire (time-series metrics, event logs, cache-behind stores). Using FIFO for a general-purpose KV store means data loss when old files are deleted.

### CompactionMerge

The merge algorithm (`CompactionMerge.java`) uses a k-way merge sort. It opens N SSTable readers simultaneously, maintains a min-heap of their current positions, and emits keys in sorted order. This is O(n log k) where n is total keys and k is number of files being merged.

Tombstones are dropped during merge only if they are in the lowest level (no older file could have the key). Otherwise tombstones are preserved because they might be needed to shadow older copies.

---

## Workload classifier

`WorkloadClassifier.java`, `WorkloadType.java`

The classifier maintains a sliding window of recent operations and computes a write/read ratio. Based on this ratio and key distribution patterns, it selects the most appropriate compaction strategy:

- High write ratio (> 80% writes) → TIERED
- Balanced or read-heavy → LEVELED  
- Monotonically increasing keys (timestamp pattern) → FIFO

The hot-swap happens without restarting. `CompactionManager` reads the current strategy from the classifier on every compaction cycle. If the strategy changed since last cycle, it switches seamlessly.

---

## Replication pressure receiver

`ReplicationPressureReceiver.java`

A singleton that holds the current replication pressure as a thread-safe `AtomicReference<Double>`. Updated by the Go pressure loop via `HTTP POST /pressure`.

```java
public enum PressureState {
    NORMAL(0.0, 0.3),    // full compaction aggressiveness
    ELEVATED(0.3, 0.6),  // 70% aggressiveness
    HIGH(0.6, 0.85),     // 30% aggressiveness
    CRITICAL(0.85, 1.0)  // 0% — compaction suspended
}
```

`compactionAggressiveness()` returns 1.0, 0.7, 0.3, or 0.0 depending on the current state.

Both `LeveledCompaction` and `TieredCompaction` call this before each cycle:

```java
double aggressiveness = ReplicationPressureReceiver.getInstance().compactionAggressiveness();
if (aggressiveness == 0.0) return CompactionPlan.NONE;
int trigger = (int) Math.ceil(BASE_TRIGGER_TABLE_COUNT / aggressiveness);
```

When `aggressiveness = 0.0` (CRITICAL), `CompactionPlan.NONE` is returned — no compaction happens at all. The node focuses entirely on catching up with the Raft leader.

---

## Recovery

`RecoveryManager.java`

On startup:
1. Find the most recent successful SSTable flush checkpoint
2. Replay all WAL entries written after that checkpoint
3. Re-apply them to a fresh MemTable
4. Return control to HeliosDB

This guarantees: any write that received a 200 OK from the server will survive a crash, because it was written to the WAL before the OK was sent.

Edge cases handled:
- Truncated WAL entries (crash mid-write): the partial entry is detected via length validation and discarded. Only complete entries are replayed.
- Empty WAL: recovery completes with an empty MemTable.
- Multiple WAL segments: replayed in order.

---

## HTTP API

All endpoints accept and return plain text (not JSON) for simplicity and debuggability.

### `GET /health`
Returns `200 OK` with body `ok`. Used by the Go layer to wait for the JVM to be ready.

### `POST /put`
Body: `<key>\n<value>` (raw bytes, newline separator)  
Returns: `200 OK` on success, `400` if body has no newline, `405` if not POST

Example:
```bash
curl -X POST -d $'hello\nworld' http://localhost:7001/put
```

### `POST /get`
Body: `<key>` (raw bytes)  
Returns: `200 OK` with value bytes, `404` if key not found, `405` if not POST

Example:
```bash
curl -X POST -d 'hello' http://localhost:7001/get
# → world
```

### `POST /delete`
Body: `<key>` (raw bytes)  
Returns: `200 OK`, `405` if not POST

### `GET /stats`
Returns JSON:
```json
{
  "compactionStrategy": "LEVELED",
  "writeAmplification": 18.4,
  "sstableCount": 12,
  "memtableSizeBytes": 2097152,
  "compactionEvents": 4
}
```

### `POST /pressure`
Body: float string (e.g. `"0.7300"`)  
Returns: `200 OK`  
Updates the `ReplicationPressureReceiver` singleton.

---

## Configuration

The JVM is started with three positional arguments:

```
java -jar helios-kv.jar <data-dir> <port> <mode>
```

| Argument | Example | Description |
|----------|---------|-------------|
| `data-dir` | `/tmp/helios-data` | Directory for WAL and SSTables |
| `port` | `7001` | HTTP listen port |
| `mode` | `ADAPTIVE` | Starting compaction mode: ADAPTIVE, LEVELED, TIERED, FIFO |

`ADAPTIVE` starts with LEVELED and lets the WorkloadClassifier hot-swap as it observes traffic.

---

## Building and running standalone

```bash
cd helios-kv
mkdir -p out
find src/main -name "*.java" > sm.txt
javac -d out @sm.txt
cd out
jar --create --file=../helios-kv.jar \
  --main-class=dev.helios.server.HeliosServer \
  $(find . -name "*.class" | sed 's|^\./||')
cd ..

# run standalone
java -jar helios-kv.jar /tmp/helios-data 7001 ADAPTIVE
```

Test with curl:
```bash
curl -X POST -d $'foo\nbar' http://localhost:7001/put
curl -X POST -d 'foo' http://localhost:7001/get
curl http://localhost:7001/stats
```

---

## Running tests

```bash
cd helios-kv
mkdir -p out-test
find src -name "*.java" > all.txt
javac -d out-test @all.txt
cd out-test
java dev.helios.AllTests
```

Test coverage:
- `MemTableTest` — put, get, delete, overwrite, tombstone
- `WALTest` — append, replay, recovery after truncation
- `HeliosDBTest` — full end-to-end: writes, reads, flush trigger, compaction trigger
- `CompactionTest` — leveled level promotion, tiered size grouping, FIFO deletion
