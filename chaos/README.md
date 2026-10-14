# chaos — Chaos Test Suite

The chaos package tests Helios under failure conditions — node crashes, network partitions, disk slowdowns. Chaos tests verify that the system's correctness guarantees hold even when things go wrong.

---

## What chaos testing is

Unit tests verify that individual functions work correctly. Integration tests verify that components work together. Chaos tests verify that the system remains correct when the environment is hostile.

The fundamental question a chaos test answers: **"If I inject this failure, does the system still uphold its guarantees?"**

For Helios, the guarantees are:
1. **No committed write is lost** — if a write returned 200 OK, it must survive crashes
2. **No split brain** — two leaders cannot simultaneously accept writes
3. **Liveness** — after failures heal, the cluster resumes accepting writes
4. **Consistency** — all nodes eventually have identical state

---

## Tests

### `TestLeaderCrash`

**What it does:**  
Starts a 3-node cluster. Writes 100 keys. Kills the leader process mid-operation. Waits for re-election. Verifies all 100 keys are readable from the new leader.

**What it proves:**  
No committed write is lost across a leader crash. The WAL and Raft log survive process termination.

**Expected behavior:**
- New leader elected within ~300ms
- All writes that returned 200 OK before the crash are present
- Writes in-flight at crash time may be lost (they were not committed)

### `TestFollowerPartition`

**What it does:**  
Starts a 3-node cluster. Partitions one follower (drops all its incoming/outgoing messages). Continues writing. Heals the partition. Verifies the follower catches up via log replication (not snapshot).

**What it proves:**  
A follower that was offline for a period catches up incrementally via AppendEntries, not by requiring a full state transfer. The Raft log on the leader is retained until all followers have consumed it.

**Expected behavior:**
- During partition: cluster operates normally (2/3 = majority)
- After heal: follower's commit index catches up to leader's within ~2 seconds
- No snapshot transfer (lag was within log retention window)

### `TestSplitBrain`

**What it does:**  
Partitions a 3-node cluster into {node1} and {node2, node3}. Attempts writes to both sides. Verifies that node1 (minority) refuses writes (cannot form quorum). Verifies node2 or node3 (majority) accepts writes.

**What it proves:**  
The minority side correctly refuses writes. Only one partition has a functioning leader at any time. No conflicting writes are accepted.

**Expected behavior:**
- node1 (alone): all writes time out or return error
- node2 + node3: one of them becomes leader, writes succeed
- After partition heals: node1 catches up as a follower with no data conflicts

### `TestRestartUnderLoad`

**What it does:**  
Starts a 3-node cluster. Runs writes continuously. Kills all nodes simultaneously. Restarts all nodes. Verifies all committed writes are recovered.

**What it proves:**  
The WAL and Raft log are durable across simultaneous full-cluster restart. The recovery path (WAL replay + Raft log replay) is correct.

**Expected behavior:**
- After restart: one node becomes leader
- All writes that returned 200 OK before shutdown are present
- No data corruption in SSTable files

---

## Running the chaos tests

```bash
# run all chaos tests
go test ./chaos/... -v -timeout 120s

# run a specific test
go test ./chaos/... -v -run TestLeaderCrash -timeout 30s

# run with race detector (recommended — catches concurrent state bugs)
go test ./chaos/... -race -v -timeout 120s
```

Chaos tests are slower than unit tests — they run real processes with real network and real disk I/O. Allow 30–60 seconds per test.

---

## Network simulation

`chaos/network.go`

The `NetworkEmulator` intercepts messages between nodes by acting as a proxy. It can:

- **Drop messages**: simulate packet loss (`DropBetween(from, to, probability)`)
- **Delay messages**: simulate network latency spikes (`DelayChan(from, to, delay)`)
- **Partition**: completely isolate a node (`Partition(nodeID)`)
- **Heal**: restore normal connectivity (`Heal(nodeID)`)

Messages are intercepted at the HTTP transport layer — each node's Raft transport is configured to route through the emulator instead of directly to peers.

---

## Chaos harness

`chaos/harness.go`

`ClusterHarness` manages a full multi-process Helios cluster for testing. It:
- Starts helios processes with unique ports and data directories
- Provides `Kill(nodeID)`, `Start(nodeID)`, `Partition(nodeID)`, `Heal(nodeID)`
- Provides a `Client()` that automatically routes to the current leader
- Collects stdout/stderr from each process for test failure output
- Cleans up all processes and temp directories on test completion

---

## Adding a new chaos test

```go
func TestYourScenario(t *testing.T) {
    h := chaos.NewClusterHarness(t, 3)
    defer h.Cleanup()
    h.Start()
    h.WaitForLeader(t, 5*time.Second)

    c := h.Client()

    // write some data
    for i := 0; i < 50; i++ {
        require.NoError(t, c.Put(ctx, fmt.Sprintf("key%d", i), "value"))
    }

    // inject failure
    h.Partition("node2")
    time.Sleep(2 * time.Second)

    // write more data
    for i := 50; i < 100; i++ {
        require.NoError(t, c.Put(ctx, fmt.Sprintf("key%d", i), "value"))
    }

    // heal and verify
    h.Heal("node2")
    time.Sleep(3 * time.Second)

    for i := 0; i < 100; i++ {
        val, found, err := c.Get(ctx, fmt.Sprintf("key%d", i))
        require.NoError(t, err)
        require.True(t, found)
        require.Equal(t, "value", val)
    }
}
```

---

## What chaos tests do NOT cover

- **Byzantine failures**: a node sending intentionally wrong data. Raft assumes crash-fail model (nodes either work correctly or stop). Detecting and handling malicious nodes requires BFT (Byzantine Fault Tolerant) consensus, which is significantly more complex.
- **Disk corruption**: silently returning wrong data from storage. A production system would add checksums to SSTable blocks and WAL entries.
- **Clock skew**: election timers are wall-clock based. Extreme clock skew between nodes could cause liveness issues. Monotonic clocks (`time.Now()` in Go uses monotonic readings) mitigate this.
- **Memory exhaustion**: MemTable growing without bound. A production system would add backpressure on writes when MemTable exceeds a limit.
