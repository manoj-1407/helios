// SPDX-License-Identifier: GPL-2.0
//
// helios_trace.bpf.c — kernel-side eBPF program
//
// Attaches kprobes to __x64_sys_write and __x64_sys_fsync.
// For each call from a tracked PID (helios-kv JVM or helios-raft Go process),
// records the entry timestamp, then on the matching kretprobe computes the
// duration and writes a TraceEvent into the ring buffer for userspace to drain.
//
// PID filtering happens here in the kernel — not in userspace. Without it,
// every sys_write on the entire machine would be captured, which would cost
// orders of magnitude more overhead. A BPF_MAP_TYPE_HASH keyed by PID lets
// userspace register the exact PIDs to trace; anything not in the map gets
// a single map lookup (~30ns) and an early return, which is negligible.
//
// Why ring buffer (BPF_MAP_TYPE_RINGBUF) over perf buffer:
// Perf buffer allocates one buffer per CPU and requires userspace to poll
// each CPU separately. Ring buffer is a single shared buffer, simpler
// consumer code, and lower memory overhead. Available since kernel 5.8;
// this WSL2 kernel is 6.6, so no compatibility concern.

#include <linux/bpf.h>
#include <linux/ptrace.h>
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_tracing.h>
#include <bpf/bpf_core_read.h>

// ── data structures shared between kernel and userspace ──────────────────

#define SYSCALL_WRITE  1
#define SYSCALL_FSYNC  2

// TraceEvent is what ends up in the ring buffer and what userspace reads.
// Kept deliberately small: timestamp + duration + pid + syscall type.
// Everything else (correlation with compaction events, lag computation) is
// done in the Go correlator — the kernel side just captures raw timing.
struct TraceEvent {
    __u64 timestamp_ns;   // ktime_get_ns() at syscall entry
    __u64 duration_ns;    // kretprobe fires on return; duration = return_time - entry_time
    __u32 pid;
    __u32 syscall_type;   // SYSCALL_WRITE or SYSCALL_FSYNC
    __u32 fd;             // file descriptor (from sys_write args); 0 for fsync
    __u32 bytes;          // bytes written (from sys_write args); 0 for fsync
};

// ── maps ──────────────────────────────────────────────────────────────────

// pid_filter: populated by userspace with the PIDs we care about.
// Key = pid (u32), Value = 1 (u8, just a presence marker).
// Max 64 entries -- we're tracking at most a handful of JVM/Go processes.
struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __uint(max_entries, 64);
    __type(key, __u32);
    __type(value, __u8);
} pid_filter SEC(".maps");

// entry_times: scratch space to hold the entry timestamp between the kprobe
// (entry) and kretprobe (return). Key = pid, Value = entry_ns.
// Per-CPU hash would be faster but adds complexity; at the call rates
// helios produces (hundreds/sec, not millions) a regular hash is fine.
struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __uint(max_entries, 64);
    __type(key, __u32);
    __type(value, __u64);
} entry_times SEC(".maps");

// entry_args: scratch space to hold fd+bytes between kprobe and kretprobe.
struct SyscallArgs {
    __u32 fd;
    __u32 bytes;
    __u32 syscall_type;
    __u32 _pad;
};
struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __uint(max_entries, 64);
    __type(key, __u32);
    __type(value, struct SyscallArgs);
} entry_args SEC(".maps");

// events: the ring buffer userspace drains.
// 256KB is more than enough for the event rate helios produces.
struct {
    __uint(type, BPF_MAP_TYPE_RINGBUF);
    __uint(max_entries, 256 * 1024);
} events SEC(".maps");

// ── helper ────────────────────────────────────────────────────────────────

static __always_inline int is_tracked(__u32 pid) {
    __u8 *v = bpf_map_lookup_elem(&pid_filter, &pid);
    return v != NULL;
}

// ── kprobes ───────────────────────────────────────────────────────────────

// sys_write entry: record entry time and fd/bytes.
SEC("kprobe/__x64_sys_write")
int BPF_KPROBE(helios_sys_write_entry, struct pt_regs *regs)
{
    __u32 pid = bpf_get_current_pid_tgid() >> 32;
    if (!is_tracked(pid))
        return 0;

    __u64 ts = bpf_ktime_get_ns();
    bpf_map_update_elem(&entry_times, &pid, &ts, BPF_ANY);

    struct SyscallArgs args = {};
    args.syscall_type = SYSCALL_WRITE;
    // On x86-64, sys_write args: rdi=fd, rsi=buf, rdx=count
    args.fd    = (__u32)PT_REGS_PARM1_CORE(regs);
    args.bytes = (__u32)PT_REGS_PARM3_CORE(regs);
    bpf_map_update_elem(&entry_args, &pid, &args, BPF_ANY);
    return 0;
}

// sys_write return: compute duration, emit event.
SEC("kretprobe/__x64_sys_write")
int BPF_KRETPROBE(helios_sys_write_exit)
{
    __u32 pid = bpf_get_current_pid_tgid() >> 32;
    __u64 *entry_ts = bpf_map_lookup_elem(&entry_times, &pid);
    if (!entry_ts)
        return 0;

    __u64 duration = bpf_ktime_get_ns() - *entry_ts;
    bpf_map_delete_elem(&entry_times, &pid);

    struct SyscallArgs *args = bpf_map_lookup_elem(&entry_args, &pid);
    struct TraceEvent *e = bpf_ringbuf_reserve(&events, sizeof(*e), 0);
    if (!e)
        return 0;

    e->timestamp_ns  = *entry_ts;
    e->duration_ns   = duration;
    e->pid           = pid;
    e->syscall_type  = SYSCALL_WRITE;
    e->fd            = args ? args->fd    : 0;
    e->bytes         = args ? args->bytes : 0;

    bpf_map_delete_elem(&entry_args, &pid);
    bpf_ringbuf_submit(e, 0);
    return 0;
}

// sys_fsync entry: fsync is what actually makes a write durable.
// This is the syscall most directly correlated with WAL append latency
// and compaction I/O pressure -- more useful than sys_write alone for
// the thesis measurement.
SEC("kprobe/__x64_sys_fsync")
int BPF_KPROBE(helios_sys_fsync_entry, struct pt_regs *regs)
{
    __u32 pid = bpf_get_current_pid_tgid() >> 32;
    if (!is_tracked(pid))
        return 0;

    __u64 ts = bpf_ktime_get_ns();
    bpf_map_update_elem(&entry_times, &pid, &ts, BPF_ANY);

    struct SyscallArgs args = {};
    args.syscall_type = SYSCALL_FSYNC;
    args.fd = (__u32)PT_REGS_PARM1_CORE(regs);
    bpf_map_update_elem(&entry_args, &pid, &args, BPF_ANY);
    return 0;
}

SEC("kretprobe/__x64_sys_fsync")
int BPF_KRETPROBE(helios_sys_fsync_exit)
{
    __u32 pid = bpf_get_current_pid_tgid() >> 32;
    __u64 *entry_ts = bpf_map_lookup_elem(&entry_times, &pid);
    if (!entry_ts)
        return 0;

    __u64 duration = bpf_ktime_get_ns() - *entry_ts;
    bpf_map_delete_elem(&entry_times, &pid);

    struct SyscallArgs *args = bpf_map_lookup_elem(&entry_args, &pid);
    struct TraceEvent *e = bpf_ringbuf_reserve(&events, sizeof(*e), 0);
    if (!e)
        return 0;

    e->timestamp_ns  = *entry_ts;
    e->duration_ns   = duration;
    e->pid           = pid;
    e->syscall_type  = SYSCALL_FSYNC;
    e->fd            = args ? args->fd : 0;
    e->bytes         = 0;

    bpf_map_delete_elem(&entry_args, &pid);
    bpf_ringbuf_submit(e, 0);
    return 0;
}

char LICENSE[] SEC("license") = "GPL";
