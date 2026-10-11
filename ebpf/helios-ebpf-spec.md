# helios-ebpf — Specification

Companion to `helios-raft-spec.md`, same format. This is a planning
document, not implementation — Project 3 hasn't been built yet.

## What this exists to do

helios-raft's thesis needs a "ground truth" that doesn't depend on
self-reported, in-process timestamps from either Java (helios-kv) or Go
(helios-raft) being honest about their own timing. If a compaction event
slows down disk I/O and that shows up as replication lag, you want to
*see* that at the kernel level — actual syscall latency, actual timing
between an `fsync()` call and a heartbeat send — not just two
applications agreeing with themselves about what happened.

That's the whole job of this component: trace syscall-level timing on
each Raft node, correlate it against the compaction events and
replication lag metrics the other two components already expose, and
produce one piece of output — a timestamp-correlated view showing
"compaction event at T, syscall latency spike at T+ε, replication lag
spike at T+ε+δ" — that becomes the evidence behind the README's Core
Finding section.

## A finding that changes this spec, found by actually testing it

Before writing a kprobe-heavy design and discovering the problem during
implementation, **I tested real eBPF feasibility directly** in the
sandbox this was developed in (full methodology: compiled a real kprobe
program targeting `__x64_sys_write`, loaded it into the kernel via
libbpf, attempted to attach it).

**kprobes failed to attach** — not a permissions issue (the sandbox runs
as root with `CAP_BPF`, `CAP_SYS_ADMIN`, and `CAP_PERFMON`, and `bpf()`
syscalls work fine for *loading* programs) but a kernel build issue:
`zcat /proc/config.gz | grep KPROBES` shows `# CONFIG_KPROBES is not
set`. The bytecode verifier accepts a kprobe program just fine; there's
no kprobe subsystem in this particular kernel to attach it *to*.

**uprobes worked.** Same sandbox, `CONFIG_UPROBES=y`, a real uprobe
attach against a test binary's function returned a live link with no
error.

**XDP is untested but plausible**: `CONFIG_XDP_SOCKETS=y`, and there's a
real NIC (`eth0`, `virtio_net` driver, which supports at least
generic/SKB-mode XDP in modern kernels).

This is specific to the sandbox this was tested in, not a general claim
about eBPF — but it's exactly the kind of thing that's invisible until
you actually try it, and WSL2's kernel (what the target machine for this
project actually runs) is a custom Microsoft build with its own history
of feature gaps relative to a stock Ubuntu kernel. **Before writing a
single line of the kprobe path, run this on the actual WSL2 target
machine:**

```bash
zcat /proc/config.gz 2>/dev/null | grep -E "CONFIG_KPROBES|CONFIG_UPROBES|CONFIG_BPF_SYSCALL"
# or, if /proc/config.gz isn't available:
grep -E "CONFIG_KPROBES|CONFIG_UPROBES" /boot/config-$(uname -r) 2>/dev/null
```

If `CONFIG_KPROBES=y` there, build the design below as written — Tier 1
is the real, intended measurement. If it's `is not set` like this
sandbox, skip straight to Tier 2 and don't burn time on a kprobes
implementation that can't load on the target machine either way. This
single command is a go/no-go gate for the whole kernel-tracing layer and
costs ten seconds to check.

## Two-tier design

### Tier 1 (if `CONFIG_KPROBES=y` on the target): syscall-level ground truth

kprobes on `sys_read`, `sys_write`, `sys_fsync` (this matters more than
plain `sys_write` — fsync latency is what actually correlates with
compaction-driven I/O contention, since both helios-kv's WAL/SSTable
writes and helios-raft's log writes go through fsync, not just write),
filtered by PID to the specific helios-kv and helios-raft processes. Each
probe records `(timestamp, pid, syscall, duration_ns)` into a BPF ring
buffer that userspace drains.

### Tier 2 (works in the dev sandbox today, and is a legitimate fallback
even with kprobes available — see "Why build both" below): uprobes on
process-boundary functions

Instead of tracing generic syscalls, attach uprobes directly to specific
exported functions at the moment they're called:

- On the **Go side** (helios-raft): a uprobe on `replicateToPeer` and on
  `RaftLog.Append`'s return — both are real, named Go functions in a
  compiled binary, which is exactly what uprobes are good at, and this
  was the configuration verified working in the sandbox test above.
- On the **Java side** (helios-kv): JVM uprobing is its own real
  complexity (JIT-compiled code doesn't sit at a fixed address the way a
  Go/C binary's symbols do) — out of scope for a first pass. Use
  helios-kv's own existing instrumentation instead (it already times
  `WAL.append()` and flush operations) and treat the JVM side as
  application-level timestamps correlated against the Go side's
  kernel-verified ones, rather than trying to uprobe into JIT-compiled
  Java directly.

### Why build both instead of just whichever the target machine supports

Tier 1 gives true syscall-level visibility (the actual disk I/O latency,
independent of which language or function called it). Tier 2 gives
function-level correlation (which specific operation was happening) but
only on the Go side directly. The strongest version of the Core Finding
uses Tier 1 timing *labeled* by Tier 2's "what was running" context —
build both if the target supports kprobes, and fall back to Tier 2 alone
plus helios-kv's existing application-level timers if it doesn't. Either
way, say explicitly in the README which tier actually produced the
numbers — this spec's whole reason for existing is that this is
contingent on facts that need checking, not assumed.

## XDP — explicitly secondary

XDP (DDoS-style packet filtering at wire speed) is real, demo-able, and
genuinely impressive on its own, but it has nothing to do with the
storage-consensus correlation thesis. Build it last, present it as a
separate capability ("this engine can also do X"), and do not let it
become the thing that eats the time budget meant for Tier 1/2 — the
actual Core Finding doesn't need a single packet filtered.

## Repository layout

```
ebpf/
  kernel/
    trace_syscalls.bpf.c     Tier 1: kprobes on sys_read/write/fsync, PID-filtered
    trace_functions.bpf.c    Tier 2: uprobes on specific Go function symbols
    xdp_filter.bpf.c         packet filtering at wire speed (separate, lower priority)
    common.h                 shared map definitions, ring buffer layout
  userspace/
    loader.go                compiles (or loads pre-compiled) .bpf.o, attaches probes,
                              drains the ring buffer -- this is the libbpf-via-cgo or
                              cilium/ebpf Go binding layer
    correlator.go             joins syscall/uprobe events against helios-kv's
                              compaction-event timestamps and helios-raft's
                              replication-lag metrics on a shared time axis
    exporter.go               Prometheus exporter for both raw trace data and
                              the correlated view
    dashboard/                Grafana dashboard JSON (compaction events,
                              replication lag, syscall latency, all on one
                              timeline)
```

## Design decisions

**Go bindings: `cilium/ebpf`, not cgo+libbpf.** Both are real options.
`cilium/ebpf` is a pure-Go library that can load pre-compiled `.bpf.o`
files without needing libbpf as a runtime dependency or cgo's build
complexity — better fit for a project that already has enough moving
parts (JVM + Go + kernel) without also debugging cgo linker issues.
Compile the `.bpf.c` files once with clang (the same way the sandbox
feasibility test did: `clang -O2 -target bpf -D__TARGET_ARCH_x86 -c
file.bpf.c -o file.bpf.o`), commit the `.bpf.o` outputs or generate them
in a build step, load them with `cilium/ebpf` at runtime.

**Ring buffer, not perf buffer.** `BPF_MAP_TYPE_RINGBUF` (kernel 5.8+)
is the modern choice — single shared buffer instead of one per-CPU perf
buffer, simpler consumer code, lower overhead. Confirm the target kernel
supports it (`CONFIG_BPF_SYSCALL=y` plus a 5.8+ kernel; this sandbox runs
6.18, almost certainly fine on any reasonably current WSL2 kernel too,
but check rather than assume — same spirit as the kprobes check above).

**PID filtering happens in the BPF program, not in userspace.** Reading
every single `sys_write` system-wide and filtering in Go would generate
enormous overhead on a busy machine; the eBPF program itself checks
`bpf_get_current_pid_tgid()` against a small allow-list map (populated by
userspace with the actual helios-kv/helios-raft PIDs at startup) before
ever writing to the ring buffer. This is the actual mechanism behind the
original thesis's "under 0.5% overhead" framing — overhead scales with
how much work happens *inside* the probe, and a single map lookup plus an
early return for non-matching PIDs is close to free.

**Clock source: `bpf_ktime_get_ns()` in the kernel, monotonic clock in
both Go and Java userspace, all three reconciled to one offset at
startup.** Comparing timestamps across the kernel and two different
language runtimes only works if they're all using the same underlying
clock (or a known, measured offset between them) — this needs to be
established once, explicitly, and documented in the correlator code, not
assumed to "just work" because all three happen to call something named
similarly.

## Implementation order

1. **Verify the kprobes/uprobes/ring-buffer facts above on the actual
   target machine.** Not optional, not last — first. This determines
   whether step 2 builds Tier 1, Tier 2, or both.
2. Tier 2 first regardless of what step 1 finds (uprobes work in every
   environment tested so far, and exercising the Go-side loader/ring
   buffer/correlator pipeline doesn't need Tier 1 to exist yet).
3. Tier 1, if the target supports it.
4. `correlator.go` joining real timestamps from both — at this point
   helios-raft's `BridgeMetrics` and helios-kv's compaction-event
   timestamps need to actually exist (Week 3 of the raft spec), so this
   step is gated on that work too, not just on the eBPF side being ready.
5. Prometheus exporter + Grafana dashboard.
6. XDP, last, as a separate capability.

## Tests / validation plan

- Unit test the PID-filtering logic in isolation (inject known PIDs,
  confirm only matching ones produce ring buffer entries) -- this can run
  without real kprobes/uprobes attached, using a mock ring buffer.
- An integration test that attaches Tier 2 uprobes to a real running
  helios-raft test binary, triggers a known number of `RaftLog.Append`
  calls, and asserts the same count of events came through the ring
  buffer -- this is a real, runnable test against the actual verified-
  working mechanism, not Tier 1's contingent one.
- A Tier 1 integration test, gated behind a build tag or an explicit
  `CONFIG_KPROBES` check at test startup that skips (not fails) when
  unavailable -- this is the one test in the whole project that should
  *expect* to sometimes not run, and say so explicitly in its skip
  message, rather than the suite silently passing for the wrong reason.
- A correlator test with synthetic timestamps (a fake compaction event,
  a fake lag spike, a fake syscall latency series) verifying the join
  logic produces the right "T, T+ε, T+ε+δ" output shape before ever
  pointing it at real data.

## What the README's eBPF section should say

Whichever tier(s) actually got built, say so plainly, the same way the
helios-kv README says plainly that RocksDB was never actually
benchmarked. "Tier 1 kprobe tracing was not available on the development
kernel and was not implemented; Tier 2 uprobe tracing on the Go process
boundary was built and is what produced the timestamps in the Core
Finding below" is a complete, honest, defensible sentence. Claiming
kernel-level syscall tracing that turned out not to be buildable on the
actual target is the kind of gap a senior engineer reviewing the repo
checks for first.
