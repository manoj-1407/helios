package ebpf

// TraceEvent mirrors the C struct TraceEvent in helios_trace.bpf.c.
// Must match exactly — byte layout, field sizes, padding.
// C layout: [u64 timestamp_ns][u64 duration_ns][u32 pid][u32 syscall_type][u32 fd][u32 bytes]
type TraceEvent struct {
	TimestampNs uint64
	DurationNs  uint64
	Pid         uint32
	SyscallType uint32
	Fd          uint32
	Bytes       uint32
}

const (
	SyscallWrite = 1
	SyscallFsync = 2
)

func (e *TraceEvent) SyscallName() string {
	switch e.SyscallType {
	case SyscallWrite:
		return "write"
	case SyscallFsync:
		return "fsync"
	default:
		return "unknown"
	}
}

// CorrelatedEvent joins a raw TraceEvent with context about what was
// happening in helios at the time — did a compaction event precede this
// syscall? How does this duration compare to baseline for this PID?
type CorrelatedEvent struct {
	TraceEvent
	ProcessName        string
	MsSinceCompaction  float64 // -1 if no compaction seen yet
	IsPostCompaction   bool    // true if within 500ms of a compaction event
	BaselineP50Ns      uint64  // rolling P50 for this PID, for anomaly detection
	IsLatencySpike     bool    // true if DurationNs > 3x baseline P50
}

// CompactionEvent is emitted by the Go bridge when it detects a strategy
// change via the helios-kv /stats endpoint. The eBPF correlator uses
// these timestamps to classify which TraceEvents occurred in the
// post-compaction window.
type CompactionEvent struct {
	TimestampNs    uint64
	Pid            uint32
	OldStrategy    string
	NewStrategy    string
}
