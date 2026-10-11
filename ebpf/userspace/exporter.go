package ebpf

import (
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
)

// Exporter maintains Prometheus-style counters and histograms from
// CorrelatedEvents and serves them on a /metrics HTTP endpoint.
// A real implementation would use the prometheus/client_golang library;
// this is a hand-rolled minimal version for the same reason helios-kv's
// MurmurHash3 is hand-rolled: pulling in prometheus/client_golang requires
// module resolution at build time, and this needs to compile and run cleanly
// without internet access on the first try.
type Exporter struct {
	mu sync.Mutex

	// raw counters
	totalEvents          atomic.Int64
	postCompactionEvents atomic.Int64
	latencySpikes        atomic.Int64

	// per-syscall latency sums (for mean computation; a real histogram
	// would use buckets, but mean+max is enough for the thesis display)
	writeDurationSumNs atomic.Int64
	writeCount         atomic.Int64
	fsyncDurationSumNs atomic.Int64
	fsyncCount         atomic.Int64

	// post-compaction specific
	postCmpDurationSumNs atomic.Int64
	postCmpCount         atomic.Int64

	// max observed (for the thesis "spike" claim)
	maxFsyncNs atomic.Int64
	maxWriteNs atomic.Int64

	addr string
}

func NewExporter(addr string) *Exporter {
	return &Exporter{addr: addr}
}

// Record updates metrics from a CorrelatedEvent.
func (e *Exporter) Record(ev CorrelatedEvent) {
	e.totalEvents.Add(1)

	switch ev.SyscallType {
	case SyscallWrite:
		e.writeDurationSumNs.Add(int64(ev.DurationNs))
		e.writeCount.Add(1)
		for {
			cur := e.maxWriteNs.Load()
			if int64(ev.DurationNs) <= cur { break }
			if e.maxWriteNs.CompareAndSwap(cur, int64(ev.DurationNs)) { break }
		}
	case SyscallFsync:
		e.fsyncDurationSumNs.Add(int64(ev.DurationNs))
		e.fsyncCount.Add(1)
		for {
			cur := e.maxFsyncNs.Load()
			if int64(ev.DurationNs) <= cur { break }
			if e.maxFsyncNs.CompareAndSwap(cur, int64(ev.DurationNs)) { break }
		}
	}

	if ev.IsPostCompaction {
		e.postCompactionEvents.Add(1)
		e.postCmpDurationSumNs.Add(int64(ev.DurationNs))
		e.postCmpCount.Add(1)
	}
	if ev.IsLatencySpike {
		e.latencySpikes.Add(1)
	}
}

// Serve starts the HTTP metrics server. Blocks until the server errors.
func (e *Exporter) Serve() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", e.handleMetrics)
	mux.HandleFunc("/health",  func(w http.ResponseWriter, r *http.Request) { fmt.Fprintln(w, "ok") })
	fmt.Printf("[helios-ebpf] metrics available at http://%s/metrics\n", e.addr)
	return http.ListenAndServe(e.addr, mux)
}

func (e *Exporter) handleMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")

	meanWrite, meanFsync, meanPostCmp := int64(0), int64(0), int64(0)
	if n := e.writeCount.Load(); n > 0 { meanWrite = e.writeDurationSumNs.Load() / n }
	if n := e.fsyncCount.Load(); n > 0 { meanFsync = e.fsyncDurationSumNs.Load() / n }
	if n := e.postCmpCount.Load(); n > 0 { meanPostCmp = e.postCmpDurationSumNs.Load() / n }

	fmt.Fprintf(w, "# HELP helios_ebpf_syscall_events_total Total syscall events captured by kprobes\n")
	fmt.Fprintf(w, "# TYPE helios_ebpf_syscall_events_total counter\n")
	fmt.Fprintf(w, "helios_ebpf_syscall_events_total %d\n\n", e.totalEvents.Load())

	fmt.Fprintf(w, "# HELP helios_ebpf_write_duration_mean_ns Mean sys_write duration (interesting events only)\n")
	fmt.Fprintf(w, "# TYPE helios_ebpf_write_duration_mean_ns gauge\n")
	fmt.Fprintf(w, "helios_ebpf_write_duration_mean_ns %d\n\n", meanWrite)

	fmt.Fprintf(w, "# HELP helios_ebpf_write_duration_max_ns Max sys_write duration observed\n")
	fmt.Fprintf(w, "# TYPE helios_ebpf_write_duration_max_ns gauge\n")
	fmt.Fprintf(w, "helios_ebpf_write_duration_max_ns %d\n\n", e.maxWriteNs.Load())

	fmt.Fprintf(w, "# HELP helios_ebpf_fsync_duration_mean_ns Mean sys_fsync duration — directly correlated with WAL append latency\n")
	fmt.Fprintf(w, "# TYPE helios_ebpf_fsync_duration_mean_ns gauge\n")
	fmt.Fprintf(w, "helios_ebpf_fsync_duration_mean_ns %d\n\n", meanFsync)

	fmt.Fprintf(w, "# HELP helios_ebpf_fsync_duration_max_ns Max sys_fsync duration — compaction spike visibility\n")
	fmt.Fprintf(w, "# TYPE helios_ebpf_fsync_duration_max_ns gauge\n")
	fmt.Fprintf(w, "helios_ebpf_fsync_duration_max_ns %d\n\n", e.maxFsyncNs.Load())

	fmt.Fprintf(w, "# HELP helios_ebpf_post_compaction_events_total Syscall events in 500ms post-compaction window\n")
	fmt.Fprintf(w, "# TYPE helios_ebpf_post_compaction_events_total counter\n")
	fmt.Fprintf(w, "helios_ebpf_post_compaction_events_total %d\n\n", e.postCompactionEvents.Load())

	fmt.Fprintf(w, "# HELP helios_ebpf_post_compaction_duration_mean_ns Mean syscall duration in post-compaction window\n")
	fmt.Fprintf(w, "# TYPE helios_ebpf_post_compaction_duration_mean_ns gauge\n")
	fmt.Fprintf(w, "helios_ebpf_post_compaction_duration_mean_ns %d\n\n", meanPostCmp)

	fmt.Fprintf(w, "# HELP helios_ebpf_latency_spikes_total Events where duration exceeded 3x rolling P50 baseline\n")
	fmt.Fprintf(w, "# TYPE helios_ebpf_latency_spikes_total counter\n")
	fmt.Fprintf(w, "helios_ebpf_latency_spikes_total %d\n\n", e.latencySpikes.Load())
}
