package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	ebpf "github.com/manoj-1407/helios/ebpf/userspace"
)

func main() {
	bpfObj  := flag.String("bpf", "", "path to helios_trace.bpf.o (auto-detected if empty)")
	pidsStr := flag.String("pids", "", "comma-separated PIDs to trace (e.g. 1234,5678,9012)")
	addr    := flag.String("addr", "0.0.0.0:9090", "Prometheus metrics listen address")
	verbose := flag.Bool("v", false, "print each correlated event to stdout")
	flag.Parse()

	if os.Geteuid() != 0 {
		fmt.Fprintln(os.Stderr, "error: helios-ebpf requires root (sudo) to load BPF programs")
		os.Exit(1)
	}

	objPath := *bpfObj
	if objPath == "" {
		var err error
		objPath, err = ebpf.FindBPFObject()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			fmt.Fprintln(os.Stderr, "\nBuild the BPF object first:")
			fmt.Fprintln(os.Stderr, "  cd ebpf/kernel && make")
			os.Exit(1)
		}
	}
	fmt.Printf("[helios-ebpf] loading BPF object: %s\n", objPath)

	// Pipeline: ring buffer → raw events → correlator → correlated events → exporter
	rawC         := make(chan ebpf.TraceEvent, 4096)
	correlatedC  := make(chan ebpf.CorrelatedEvent, 1024)

	loader, err := ebpf.NewLoader(objPath, rawC)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load BPF: %v\n", err)
		fmt.Fprintln(os.Stderr, "\nCommon causes:")
		fmt.Fprintln(os.Stderr, "  - Not running as root")
		fmt.Fprintln(os.Stderr, "  - BPF object not compiled (run: cd ebpf/kernel && make)")
		fmt.Fprintln(os.Stderr, "  - Kernel CONFIG_KPROBES=n (check: zcat /proc/config.gz | grep KPROBES)")
		os.Exit(1)
	}
	fmt.Println("[helios-ebpf] kprobes attached: __x64_sys_write, __x64_sys_fsync")

	// Register PIDs
	if *pidsStr != "" {
		for _, part := range strings.Split(*pidsStr, ",") {
			part = strings.TrimSpace(part)
			pid, err := strconv.ParseUint(part, 10, 32)
			if err != nil {
				fmt.Fprintf(os.Stderr, "invalid PID %q: %v\n", part, err)
				continue
			}
			if err := loader.RegisterPID(uint32(pid)); err != nil {
				fmt.Fprintf(os.Stderr, "registering PID %d: %v\n", pid, err)
			} else {
				fmt.Printf("[helios-ebpf] tracking PID %d\n", pid)
			}
		}
	} else {
		fmt.Println("[helios-ebpf] warning: no PIDs specified (-pids flag) — no events will be captured")
		fmt.Println("             usage: sudo ./helios-ebpf -pids $(pgrep -f helios-kv.jar | tr '\\n' ',')")
	}

	correlator := ebpf.NewCorrelator(correlatedC)
	exporter   := ebpf.NewExporter(*addr)

	// Correlator goroutine: drain raw events, correlate, forward
	go func() {
		for e := range rawC {
			correlator.Process(e)
		}
	}()

	// Exporter goroutine: record metrics, optionally print
	go func() {
		for e := range correlatedC {
			exporter.Record(e)
			if *verbose {
				kind := "spike"
				if e.IsPostCompaction { kind = "post-compaction" }
				fmt.Printf("[%s] pid=%d %s duration=%.2fms baseline_p50=%.2fms ms_since_cmp=%.1f\n",
					kind,
					e.Pid,
					e.SyscallName(),
					float64(e.DurationNs)/1e6,
					float64(e.BaselineP50Ns)/1e6,
					e.MsSinceCompaction,
				)
			}
		}
	}()

	// Stats ticker
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			// Print a brief summary line every 10s so you know it's working
			fmt.Printf("[helios-ebpf] running — metrics at http://%s/metrics\n", *addr)
		}
	}()

	// Metrics server in background
	go func() {
		if err := exporter.Serve(); err != nil {
			fmt.Fprintf(os.Stderr, "metrics server: %v\n", err)
		}
	}()

	// Wait for Ctrl+C
	sigC := make(chan os.Signal, 1)
	signal.Notify(sigC, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigC
	fmt.Printf("\n[helios-ebpf] received %v, shutting down...\n", sig)

	loader.Stop()
	close(rawC)
	fmt.Println("[helios-ebpf] done")
}
