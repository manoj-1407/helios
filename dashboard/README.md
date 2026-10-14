# dashboard — Live Cluster Dashboard

The dashboard is a real-time web interface for monitoring the Helios cluster. It shows node health, compaction state, write amplification trends, and replication pressure — all updating live without page refresh.

---

## Access

Start helios with `-dash :8080` and open `http://localhost:8080` in any browser.

No login, no configuration, no build step. The entire UI is served as a single self-contained HTML page from the Go binary.

---

## What you see

### Header bar

```
⊙ Helios  ● LIVE   leader: node1   max lag: 0 entries
```

- **LIVE / STALE**: LIVE means the SSE stream is connected and data is being received. STALE means the connection dropped and the last update was more than 3 seconds ago.
- **leader**: which node is the current Raft leader. All writes must go to this node.
- **max lag**: maximum entries any follower is behind the leader. 0 is ideal. Values above 50 suggest a struggling follower.

### Cluster nodes panel

One row per node showing:
- **Role**: Leader or Follower (or Candidate during an election)
- **Term**: the current Raft term. Each election increments this. Monotonically increasing.
- **Commit index**: how many log entries have been committed to this node. All nodes should show the same number in a healthy cluster.
- **SSTables**: how many SSTable files currently exist on this node. Higher numbers mean more compaction work is pending.

### Compaction strategy panel

- **Strategy**: which compaction algorithm is active (Leveled / Tiered / FIFO). In ADAPTIVE mode, this can change as the WorkloadClassifier observes write/read patterns.
- **Write amplification**: ratio of bytes written by compaction to bytes written by the user. Lower is better. Leveled compaction typically shows 10–30x. Tiered shows 3–10x. FIFO shows 1x. Very high values (>50x) indicate heavy compaction load.

### Replication pressure panel

One bar per node, 0–100%.

- **0–30% (NORMAL)**: compaction is running at full aggressiveness. Healthy.
- **30–60% (ELEVATED)**: compaction threshold raised ~43%. Follower is slightly behind.
- **60–85% (HIGH)**: compaction threshold raised ~3x. Follower is struggling.
- **85–100% (CRITICAL)**: compaction suspended entirely. This node is falling behind on replication and needs all disk bandwidth for log application.

### Write amplification chart

A canvas-based line chart showing write amplification over the last 5 minutes (300 data points, one per second). This lets you see the effect of load spikes, compaction cycles, and strategy switches over time.

A sudden spike in write amplification usually means a large compaction cycle just ran. A flat line at 1x usually means FIFO mode.

---

## How it works technically

### Server-Sent Events (SSE)

The dashboard uses SSE instead of polling or WebSockets.

**Polling** would require the browser to send a new HTTP request every second. Each request has overhead (headers, connection setup) and creates a delay between the server having data and the browser receiving it.

**WebSockets** are bidirectional — the client can also send data. For a read-only monitoring dashboard, this bidirectionality adds complexity (handshake protocol, frame parsing, ping/pong) for no benefit.

**SSE** is a one-way HTTP stream. The server sends `data: {...}\n\n` lines. The browser's built-in `EventSource` API fires an event for each line. No extra libraries needed. Reconnects automatically on disconnect. Works through HTTP proxies and load balancers that reject WebSocket upgrades. Perfect for monitoring dashboards.

Server-side:
```go
w.Header().Set("Content-Type", "text/event-stream")
w.Header().Set("Cache-Control", "no-cache")
w.Header().Set("Connection", "keep-alive")

for {
    snapshot := collectClusterSnapshot()
    data, _ := json.Marshal(snapshot)
    fmt.Fprintf(w, "data: %s\n\n", data)
    flusher.Flush()
    time.Sleep(time.Second)
}
```

Browser-side:
```javascript
const es = new EventSource('/events');
es.onmessage = (e) => {
    const snapshot = JSON.parse(e.data);
    updateUI(snapshot);
};
```

### Data collection

The dashboard server polls each node's `/stats` endpoint every second. The poll is concurrent (all nodes in parallel with a 800ms timeout). Results are assembled into a `ClusterSnapshot`:

```go
type ClusterSnapshot struct {
    Timestamp   time.Time
    LeaderID    string
    MaxLag      int64
    Nodes       []NodeSnapshot
}

type NodeSnapshot struct {
    ID                 string
    Role               string
    Term               uint64
    CommitIndex        uint64
    SSTableCount       int
    CompactionStrategy string
    WriteAmplification float64
    Pressure           float64
}
```

Snapshots are kept in a 300-entry ring buffer (5 minutes of history) for the write amplification chart.

### Self-contained HTML

The entire dashboard — HTML structure, CSS styles, JavaScript logic, canvas chart rendering — is a single Go string constant in `dashboard/server.go`. No static files, no CDN dependencies, no build pipeline. The binary is fully self-contained: deploy it anywhere and the dashboard works.

This is a deliberate choice for operational simplicity. A monitoring dashboard that requires npm, webpack, or network access to a CDN to render is fragile. This one has zero dependencies at runtime.

---

## Reading the dashboard during failure scenarios

### Leader crash

The dashboard will show:
1. The old leader node disappears from the "Cluster nodes" panel (its `/stats` returns connection refused)
2. "max lag" briefly increases as followers wait for heartbeats
3. One of the remaining nodes changes role from Follower → Leader (within ~300ms)
4. Normal operation resumes

### Follower under high load

Watch for:
- SSTable count increasing rapidly on one node (flush backlog)
- Replication pressure bar rising for that node
- Write amplification spiking (compaction catching up)
- "max lag" increasing (that node falling behind)

If replication pressure exceeds 60%, the pressure loop will push HIGH state to that node's JVM, suppressing compaction. You should see the SSTable count stabilize and lag stop growing as the node catches up on replication.

### Strategy switch

If write patterns change (e.g. a batch load job starts), watch the Compaction Strategy panel. In ADAPTIVE mode, the strategy may switch from LEVELED to TIERED. Write amplification will drop (fewer bytes rewritten per compaction), but P99 read latency may increase slightly (more files to check per read).
