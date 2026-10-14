# ebpf — Kernel I/O Tracer

The eBPF tracer attaches kernel probes to the `sys_write` and `sys_fsync` system calls for any process in the Helios cluster. It lets you correlate application-level events (compaction cycles, pressure changes) with kernel-level I/O metrics (bytes flushed, fsync latency) without modifying the application.

---

## Why eBPF?

Application-level metrics tell you what the application *thinks* it's doing. `"Compaction wrote 100MB"` tells you the application submitted 100MB of writes. It does not tell you:
- How long those writes actually took to hit persistent storage
- Whether `fsync` blocked for 800ms while the kernel flushed the page cache
- What fraction of time the disk was busy
- Whether the JVM's GC paused mid-compaction and caused a latency spike

eBPF observes these things at the kernel level, for every write and fsync, with sub-microsecond overhead and zero changes to the application code.

**eBPF** (extended Berkeley Packet Filter) is a Linux kernel feature that lets you load verified programs into the kernel. These programs run in response to kernel events (a syscall being called, a function being entered, a network packet arriving) and can record data into ring buffers that userspace reads. The kernel verifier checks the program before loading — it cannot crash the kernel, cannot loop infinitely, cannot access arbitrary memory.

---

## What it traces

### `sys_write` kprobe

Fires on every `write()` system call. Records:
- Process ID
- File descriptor
- Bytes written
- Timestamp (nanoseconds)

### `sys_fsync` kprobe + kretprobe

The kprobe fires when `fsync()` is entered. The kretprobe fires when it returns. The difference is fsync duration — how long the kernel held the write until it hit persistent storage.

Records:
- Process ID
- File descriptor
- fsync duration (nanoseconds)
- Return code

---

## What the userspace correlator does

`userspace/correlator.go`

Reads events from the eBPF ring buffer. For each event, it:
1. Looks up the process name — events are filtered to Helios processes only (processes named `helios` or with `helios-kv` in their command line)
2. Tags the event with a node ID (derived from the data directory path or port)
3. Feeds the event into the exporter

`userspace/exporter.go`

Aggregates events into:
- **Bytes written per second** (rolling 1s window per node)
- **fsync latency P50/P95/P99** (rolling 100-event window per node)
- **Write throughput (ops/sec)**

Exports as:
- Structured JSON log lines to stdout
- Prometheus metrics on `:9090/metrics` (optional, enable with `-metrics`)

---

## Correlation with application events

By running the eBPF tracer alongside the cluster, you can answer questions like:

**"Did replication pressure actually reduce compaction I/O?"**
- Watch `bytes_written_per_sec` for a follower node
- Trigger a load spike on the leader
- Observe pressure rising in the dashboard
- Observe `bytes_written_per_sec` dropping as compaction is suppressed

**"What is the actual fsync latency on this machine?"**
- `fsync_p99_ns` gives the real number, including OS buffering and disk scheduler latency
- Application-reported latency is often lower because it measures the time to submit the write, not the time to flush it

**"Is compaction causing write stalls?"**
- A spike in `fsync_p99_ns` during a compaction cycle confirms that the disk is saturated
- Cross-reference with Raft apply latency (from `ReplicationMetrics`) to see if it's causing follower lag

---

## Build requirements

- Linux kernel 5.8+ (required for eBPF ring buffers)
- clang 18+
- libbpf-dev
- `bpf_helpers.h` (from libbpf-dev or kernel headers)

Check your environment:
```bash
clang --version     # need 18+
ls /usr/include/bpf/bpf_helpers.h  # must exist
uname -r            # need 5.8+
```

On Ubuntu/Debian:
```bash
sudo apt install clang-18 libbpf-dev linux-headers-$(uname -r)
```

---

## Build

```bash
cd ebpf
./build.sh
```

The build script:
1. Compiles `kernel/helios_trace.bpf.c` with clang to BPF bytecode
2. Generates Go bindings with `bpf2go`
3. Builds the userspace loader + correlator + exporter as a Go binary

Output: `ebpf/helios-ebpf`

---

## Running

```bash
# must be run as root or with CAP_BPF capability
sudo ./ebpf/helios-ebpf -pids $(pgrep -d, -f helios)

# with Prometheus metrics
sudo ./ebpf/helios-ebpf -pids $(pgrep -d, -f helios) -metrics :9090
```

Sample output:
```json
{"ts":"2026-10-05T21:30:01Z","node":"node1","event":"write","fd":7,"bytes":4096,"ns":1234567}
{"ts":"2026-10-05T21:30:01Z","node":"node1","event":"fsync","fd":7,"duration_ns":823000,"rc":0}
{"ts":"2026-10-05T21:30:02Z","node":"node1","metric":"write_bytes_per_sec","value":52428800}
{"ts":"2026-10-05T21:30:02Z","node":"node1","metric":"fsync_p99_ns","value":1100000}
```

---

## Kubernetes / containerized environments

eBPF in containers requires:
- The container must run privileged, or have `CAP_BPF` + `CAP_SYS_ADMIN`
- The host kernel version applies (not the container's)
- Run as a DaemonSet sidecar alongside Helios pods

Example Kubernetes DaemonSet snippet:
```yaml
securityContext:
  capabilities:
    add: ["BPF", "SYS_ADMIN", "NET_ADMIN"]
  privileged: true
```

---

## File map

| File | Purpose |
|------|---------|
| `kernel/helios_trace.bpf.c` | BPF program loaded into kernel |
| `userspace/loader.go` | Loads the BPF program, attaches kprobes |
| `userspace/types.go` | Event struct definitions (must match BPF C structs) |
| `userspace/correlator.go` | Reads ring buffer, filters, tags events |
| `userspace/exporter.go` | Aggregates and outputs metrics |
| `cmd/helios-ebpf/main.go` | Entry point, flag parsing |
| `build.sh` | Build script |
| `Makefile` | Alternative make-based build |
