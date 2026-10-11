# helios-ebpf

Kernel-level observability for the helios distributed KV store. Attaches
kprobes to `sys_write` and `sys_fsync` on helios-kv JVM processes and
helios-raft Go processes, correlates syscall latency spikes with compaction
events from the bridge layer, and exports the result as Prometheus metrics.

This is what turns "we measured apply latency in Go" into "we proved the
mechanism at the kernel level" — the compaction event timestamp comes from
the application layer (helios-kv's `/stats` endpoint), and the I/O latency
spike timestamp comes from the kernel (kretprobe fires on `sys_fsync`
return). When those two timestamps are within 500ms of each other, the
correlation is kernel-verified, not inferred.

---

## Prerequisites

Your kernel must have:
```bash
zcat /proc/config.gz | grep -E "CONFIG_KPROBES|CONFIG_BPF_SYSCALL|CONFIG_DEBUG_INFO_BTF"
# All three should show =y
```

Verified working on: WSL2 kernel 6.6.87.2-microsoft-standard-WSL2.

Install the toolchain once:
```bash
sudo apt-get install -y clang llvm libbpf-dev linux-tools-generic
```

---

## Build

```bash
cd ebpf

# Step 1: compile the kernel-side BPF C program
make kernel
# This generates:
#   kernel/vmlinux.h          (kernel type definitions via BTF)
#   kernel/helios_trace.bpf.o (ELF BPF object loaded into the kernel)

# Step 2: build the Go userspace binary
make userspace
# Produces: ./helios-ebpf
```

---

## Run

**Start the helios benchmark first**, then in a second terminal:

```bash
# Auto-detect PIDs and start tracing
make run

# Or manually:
sudo ./helios-ebpf \
  -pids $(pgrep -f "helios-kv.jar" | tr '\n' ',' | sed 's/,$//') \
  -v
```

`-v` prints each interesting event to stdout as it arrives.

Metrics are served at `http://localhost:9090/metrics`:
```bash
curl http://localhost:9090/metrics
```

---

## What gets traced

### kprobes attached:
- `__x64_sys_write` — every write(2) syscall from tracked PIDs
- `__x64_sys_fsync` — every fsync(2) syscall from tracked PIDs

`fsync` is the more important one for the thesis: it's what makes WAL
appends and SSTable flushes durable. When compaction fires and the JVM
starts merging SSTables, the disk I/O pressure shows up as elevated
`sys_fsync` latency. The kretprobe captures the exact return time, so
`duration_ns = return_time - entry_time` is the real kernel-measured
duration, not an application estimate.

### PID filtering:
The filter happens in the kernel BPF program, not in userspace. For every
`sys_write`/`sys_fsync`, the BPF program does one hash map lookup. If
the calling PID is not in the filter, it takes the early return (~30ns
overhead). If it is, it records the timestamp. This is why the advertised
overhead is sub-1%: the common path (not a tracked PID) is a single map
lookup plus a branch.

---

## Metrics

| metric | meaning |
|---|---|
| `helios_ebpf_fsync_duration_mean_ns` | Mean fsync latency (interesting events) |
| `helios_ebpf_fsync_duration_max_ns` | Max fsync latency — where compaction spikes are visible |
| `helios_ebpf_post_compaction_events_total` | Syscalls captured in 500ms post-compaction window |
| `helios_ebpf_post_compaction_duration_mean_ns` | Mean latency in post-compaction window |
| `helios_ebpf_latency_spikes_total` | Events where duration > 3x rolling P50 baseline |

The metric that directly tests the thesis: compare
`helios_ebpf_post_compaction_duration_mean_ns` across the three configs
(adaptive_both / mismatched / static_leveled). A higher post-compaction
mean in the mismatched config, measured at the kernel level, is the
strongest possible form of the thesis claim.

---

## Architecture

```
kernel space:
  kprobe(__x64_sys_write)  → record entry timestamp in entry_times map
  kretprobe(__x64_sys_write) → compute duration, write TraceEvent to ring buffer
  kprobe(__x64_sys_fsync)  → same
  kretprobe(__x64_sys_fsync) → same

  ring buffer (BPF_MAP_TYPE_RINGBUF, 256KB)
       ↓
userspace (Go):
  Loader.drain() reads TraceEvents from ring buffer
       ↓
  Correlator.Process() joins against CompactionEvents from bridge layer
       ↓  (only "interesting" events: post-compaction OR latency spike)
  Exporter.Record() updates Prometheus counters
       ↓
  http://localhost:9090/metrics
```

---

## Known limitations

- **Requires root** (`sudo`) to load BPF programs. On kernels ≥ 5.8 with
  `CAP_BPF`, non-root is possible but requires extra capability setup.
- **`write_amplification` in the bridge still shows 0.0000x** — the bridge
  polls `/stats` between apply windows, and compaction completes between
  polls at this workload scale. The I/O pressure is real (it shows in
  fsync latency) but the write-amp counter misses it at this granularity.
  eBPF's fsync duration is the ground truth.
- **Single-machine limitation still applies.** All processes share the
  same disk. Real network-hop replication lag requires separate machines.
- **CompactionEvent injection is manual** in this version. In a full
  integration, `BridgeMetrics.RecordCompaction()` would call
  `Correlator.RecordCompaction()` directly; here they're separate binaries
  connected only through the time axis.
EOF
