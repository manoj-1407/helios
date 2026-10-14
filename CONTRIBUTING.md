# Contributing to Helios

This document explains how to build, test, and extend Helios. Whether you want to fix a bug, add a feature, or understand how everything works before making a change — start here.

---

## Development environment

### Requirements

| Tool | Version | Purpose |
|------|---------|---------|
| Go | 1.22+ | helios binary, consensus, bridge |
| Java JDK | 21+ | helios-kv storage engine |
| clang | 18+ | eBPF kernel program |
| libbpf-dev | latest | eBPF helper headers |
| redis-cli | any | manual testing |
| git | any | version control |

On Ubuntu/Debian:
```bash
sudo apt install openjdk-21-jdk clang-18 libbpf-dev redis-tools
```

Go installation: https://go.dev/dl/

### Clone and build

```bash
git clone https://github.com/manoj-1407/helios.git
cd helios

# build storage engine
cd helios-kv && mkdir -p out
find src/main -name "*.java" > sm.txt && javac -d out @sm.txt
cd out && jar --create --file=../helios-kv.jar \
  --main-class=dev.helios.server.HeliosServer \
  $(find . -name "*.class" | sed 's|^\./||')
cd ../..

# build Go binary
go build -o helios ./cmd/helios/

# verify
./helios --help
```

---

## Project structure

```
helios/
├── cmd/
│   ├── helios/main.go        ← single-node entrypoint
│   └── bench/main.go         ← benchmark runner entrypoint
├── raft/                     ← Raft consensus implementation
│   ├── raft.go
│   ├── metrics.go            ← ReplicationMetrics, PressureLevel()
│   ├── compaction_signal.go  ← CompactionCoordinator
│   └── *_test.go
├── store/                    ← bridge between Raft and JVM
│   ├── bridge.go
│   ├── kvserver.go
│   └── pressure_loop.go      ← the core feedback loop
├── client/                   ← Go SDK + RESP server
│   ├── client.go
│   └── redis_server.go
├── dashboard/
│   └── server.go             ← SSE dashboard
├── chaos/                    ← chaos tests
├── bench/                    ← benchmarks
├── ebpf/                     ← eBPF kernel tracer
└── helios-kv/                ← Java storage engine
    └── src/main/java/dev/helios/
        ├── server/HeliosServer.java
        ├── core/             ← WAL, MemTable, HeliosDB
        ├── compaction/       ← Leveled, Tiered, FIFO, Manager
        ├── classifier/       ← WorkloadClassifier, PressureReceiver
        ├── sstable/          ← SSTableWriter, SSTableReader, SparseIndex
        └── bloom/            ← BloomFilter, MurmurHash3
```

---

## Running tests

### Go unit tests

```bash
# all tests
go test ./... -v

# specific package
go test ./raft/... -v

# with race detector (always use this before a PR)
go test ./... -race

# chaos tests (slower — need real processes)
go test ./chaos/... -v -timeout 120s
```

### Java unit tests

```bash
cd helios-kv
mkdir -p out-test
find src -name "*.java" > all.txt
javac -d out-test @all.txt
cd out-test
java dev.helios.AllTests
```

### Manual smoke test

```bash
./helios -id node1 -data /tmp/h1 -jar helios-kv/helios-kv.jar \
  -jvm 127.0.0.1:7001 -kvport 7001 -redis :6380 -dash :8080 &

sleep 3

redis-cli -p 6380 SET test ok
redis-cli -p 6380 GET test   # should print "ok"

pkill helios
```

---

## How to add a new compaction strategy

1. Create `helios-kv/src/main/java/dev/helios/compaction/YourStrategy.java`:

```java
package dev.helios.compaction;

public class YourStrategy implements CompactionStrategy {

    @Override
    public CompactionPlan plan(List<SSTable> tables) {
        // your logic
        // return CompactionPlan.NONE if nothing to do
        // return new CompactionPlan(filesToMerge, outputLevel) otherwise
    }

    @Override
    public String name() { return "YOUR_STRATEGY"; }
}
```

2. Register it in `CompactionManager.java`:

```java
case "YOUR_STRATEGY" -> new YourStrategy();
```

3. Add it to `WorkloadClassifier.java` if the classifier should automatically select it.

4. Add it to the `mode` flag options in `cmd/helios/main.go`.

5. Write a test in `CompactionTest.java` that verifies:
   - It triggers at the right time
   - It produces correct output (all keys present, correct values, tombstones dropped where appropriate)
   - It interacts correctly with `ReplicationPressureReceiver`

---

## How to add a new Raft metric to the pressure formula

1. Add a field to `ReplicationMetrics` in `raft/metrics.go`:

```go
type ReplicationMetrics struct {
    // existing fields...
    yourMetric float64
}
```

2. Add an update method:

```go
func (m *ReplicationMetrics) UpdateYourMetric(val float64) {
    m.mu.Lock()
    m.yourMetric = val
    m.mu.Unlock()
}
```

3. Call it from the appropriate place in `raft.go`.

4. Add it to `PressureLevel()`:

```go
yourScore := math.Min(m.yourMetric/yourThreshold, 1.0)
pressure := 0.35*lagScore + 0.35*latencyScore + 0.15*stalenessScore + 0.15*yourScore
```

(Adjust weights so they sum to 1.0)

5. Update `raft/metrics_test.go` with cases that exercise the new metric.

---

## How to add a new RESP command

1. In `client/redis_server.go`, add a case to the command dispatch:

```go
case "YOURCOMMAND":
    if len(parts) < 2 {
        writeError(w, "ERR wrong number of arguments")
        return
    }
    // call c.Put / c.Get / c.Delete or a new client method
    result, err := c.YourOperation(ctx, parts[1])
    if err != nil {
        writeError(w, err.Error())
        return
    }
    writeBulk(w, result)
```

2. If it requires a new helios-kv endpoint, add it to:
   - `HeliosServer.java` (Java handler)
   - `bridge.go` (Go HTTP call)
   - `client.go` (Go SDK method)

3. Document it in `client/README.md`.

---

## Code style

### Go

- `gofmt` for formatting (enforced by CI)
- Exported symbols get doc comments
- Error messages are lowercase, no period at end (`"helios write: connection refused"` not `"Helios Write: Connection Refused."`)
- Contexts are always the first parameter
- No global state except singletons that are explicitly documented

### Java

- Standard Java naming conventions
- `final` fields where possible
- `AtomicReference` / `AtomicLong` for shared mutable state — no raw synchronized blocks unless necessary
- Each class has one responsibility — no god objects

---

## Before opening a PR

1. `go test ./... -race` passes
2. Java tests pass
3. Manual smoke test passes on a 3-node cluster
4. `go vet ./...` reports no issues
5. New code has tests
6. `ARCHITECTURE.md` updated if you added a new component or changed a contract between components

---

## Questions

Open an issue on GitHub describing what you're trying to do. If it's a design question (should this be in Raft or in the bridge? should this be a new JVM endpoint?), include what you've considered and why you're unsure. The issue is also the right place to propose a new feature before writing code.
