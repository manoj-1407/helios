# store — Bridge and KV Server

The `store` package is the glue between Raft consensus and the helios-kv storage engine. It translates committed Raft log entries into HTTP calls to the JVM, and pumps real-time replication pressure metrics from Raft back into the compaction layer.

---

## Files

| File | Purpose |
|------|---------|
| `bridge.go` | HTTP client wrapping helios-kv's API |
| `kvserver.go` | Raft state machine: applies committed entries to the bridge |
| `pressure_loop.go` | Goroutine that polls Raft metrics and pushes pressure to JVM |
| `command.go` | Command encoding/decoding for Raft log entries |

---

## bridge.go

`HeliosKVBridge` is a thin HTTP client over helios-kv. Every method corresponds to one helios-kv endpoint.

```go
type HeliosKVBridge struct {
    addr   string        // e.g. "127.0.0.1:7001"
    client *http.Client
}
```

**Methods:**

`Put(key, value string) error`  
POST `/put` with body `key\nvalue`. The newline is the separator — helios-kv's `handlePut` reads until the first `\n` for the key, then takes the rest as the value.

`Get(key string) ([]byte, bool, error)`  
POST `/get` with body `key`. Returns the value bytes. A 404 response means the key does not exist (returns `nil, false, nil` — not an error).

`Delete(key string) error`  
POST `/delete` with body `key`.

`FetchStats() (*NodeStats, error)`  
GET `/stats`. Returns JSON with compaction strategy, write amplification, SSTable count.

`PushPressure(pressure float64) error`  
POST `/pressure` with body `"0.7300"` (4 decimal places). Updates `ReplicationPressureReceiver` in the JVM.

`HealthCheck() error`  
GET `/health`. Returns nil if the JVM responds with 200.

**Why raw bytes over HTTP instead of gRPC?**  
Debuggability is the primary reason. Any developer can `curl -X POST -d 'mykey' http://localhost:7001/get` and immediately see the value. There are no generated proto files, no codec configuration, no stub libraries. The overhead (HTTP framing, ~0.1ms) is negligible compared to disk I/O latency.

---

## kvserver.go

`HeliosKVServer` implements the Raft state machine interface. The Raft apply loop calls `Apply(entry)` for each committed log entry.

```go
type HeliosKVServer struct {
    bridge *HeliosKVBridge
    mu     sync.Mutex
}

func (s *HeliosKVServer) Apply(entry []byte) error {
    cmd := decodeCommand(entry)
    switch cmd.Type {
    case CmdPut:    return s.bridge.Put(cmd.Key, cmd.Value)
    case CmdDelete: return s.bridge.Delete(cmd.Key)
    }
}
```

**Ordering guarantee:** because the Raft apply loop calls `Apply` sequentially in log order, writes reach helios-kv in the same order across all nodes. This means all nodes have identical state after applying the same log entries — the fundamental distributed systems requirement.

**Error handling:** if `Apply` returns an error (e.g. JVM crashed), the apply loop retries with backoff. The Raft log is not advanced past the failed entry. This means the cluster stalls rather than losing a committed write.

---

## pressure_loop.go

`CompactionPressureLoop` is a background goroutine that implements the core novel contribution of Helios: feeding real-time replication pressure back into the compaction layer.

```go
type CompactionPressureLoop struct {
    node         *raft.RaftNode
    bridge       *HeliosKVBridge
    stopC        chan struct{}
    lastPressure float64
}
```

**Operation:**

```
every 500ms:
  pressure = node.RepMetrics.PressureLevel()
  
  if |pressure - lastPressure| > 0.1:
    bridge.PushPressure(pressure)
    lastPressure = pressure
```

The 0.1 threshold prevents flooding the JVM with identical pressure updates during stable periods. The JVM only needs to know when pressure meaningfully changes — not receive 120 identical "0.0300" updates per minute.

**On shutdown:**

```go
func (l *CompactionPressureLoop) Stop() {
    close(l.stopC)
    l.bridge.PushPressure(0.0)  // reset JVM to NORMAL pressure
}
```

Pushing 0.0 on shutdown ensures the JVM doesn't restart in a high-pressure state from the previous run. If this reset weren't sent, a JVM restart after a cluster under heavy load would incorrectly start in CRITICAL state and refuse to compact.

**Polling interval choice (500ms):**  
Fast enough to respond to sudden lag spikes within one second. Slow enough that the polling overhead (one HTTP round trip per 500ms per node) is negligible. Raft heartbeat interval is typically 50–150ms, so multiple heartbeat cycles happen between each pressure poll — plenty of data to form an accurate picture.

---

## command.go

`Command` is the type stored in the Raft log. Before a write enters the Raft log, it is encoded as a `Command`. After the log commits it, `Apply` decodes it.

```go
type Command struct {
    Type  CmdType // CmdPut or CmdDelete
    Key   string
    Value string   // empty for deletes
}
```

Encoding is simple length-prefixed binary: `[1 byte type][4 bytes key length][key][4 bytes value length][value]`. This is more compact than JSON and doesn't require a schema file like protobuf.

**Why not store raw HTTP request bytes in the log?**  
HTTP requests include headers, content-type, and other metadata irrelevant to the state machine. The Command type captures exactly what needs to be applied — nothing more. This keeps log entries small and the apply path simple.

---

## The full write flow (multi-node)

```
client PUT foo=bar
    │
    ▼
HeliosKVServer.Submit(Command{Put, "foo", "bar"})
    │ encodes to bytes
    ▼
raft.RaftNode.Propose(entry)
    │ appends to local log
    │ sends AppendEntries to all peers
    │ waits for majority acknowledgment
    ▼
majority acked → commitIndex advances
    │
    ▼
apply loop: HeliosKVServer.Apply(entry)
    │ decodes Command
    ▼
bridge.Put("foo", "bar")
    │ HTTP POST /put  body: "foo\nbar"
    ▼
helios-kv: WAL append → MemTable insert → 200 OK
    │
    ▼
HTTP 200 back to client
```

The client sees 200 only after the write is committed to a majority AND applied to the local state machine. This is the linearizable read/write guarantee: if you read after a successful write, you will see that write.
