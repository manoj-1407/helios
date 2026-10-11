package ebpf

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"
	"unsafe"

	libbpf "github.com/cilium/ebpf"
	"github.com/cilium/ebpf/link"
	"github.com/cilium/ebpf/ringbuf"
	"github.com/cilium/ebpf/rlimit"
)

// Loader loads helios_trace.bpf.o into the kernel, attaches kprobes,
// populates the PID filter map, and drains the ring buffer.
//
// Why cilium/ebpf instead of cgo+libbpf:
// cilium/ebpf is pure Go — no cgo, no runtime linking against libbpf.so,
// no CGO_ENABLED=1 requirement. It can load pre-compiled .bpf.o files
// and manage maps/links entirely from Go. This is what Cilium itself uses
// in production; it's not a lightweight wrapper, it's a real BPF library.
type Loader struct {
	mu      sync.Mutex
	objs    *bpfObjects
	links   []link.Link
	reader  *ringbuf.Reader
	eventC  chan<- TraceEvent
	stopC   chan struct{}
	wg      sync.WaitGroup
}

// bpfObjects holds references to the maps and programs inside the loaded
// BPF object. The field names correspond to the map/program names in the
// .bpf.c file (cilium/ebpf generates these at load time by reading the
// ELF section names).
type bpfObjects struct {
	// maps
	PidFilter  *libbpf.Map
	EntryTimes *libbpf.Map
	EntryArgs  *libbpf.Map
	Events     *libbpf.Map
	// programs
	HeliosSysWriteEntry *libbpf.Program
	HeliosSysWriteExit  *libbpf.Program
	HeliosSysFsyncEntry *libbpf.Program
	HeliosSysFsyncExit  *libbpf.Program
}

// NewLoader loads the pre-compiled BPF object from bpfObjPath, attaches
// kprobes, and starts draining the ring buffer into eventC.
// Requires CAP_BPF or running as root.
func NewLoader(bpfObjPath string, eventC chan<- TraceEvent) (*Loader, error) {
	// Remove the default RLIMIT_MEMLOCK restriction that would prevent
	// loading BPF maps. cilium/ebpf provides a helper for this that works
	// on kernels >= 5.11 (which set per-cgroup limits instead of rlimit).
	if err := rlimit.RemoveMemlock(); err != nil {
		return nil, fmt.Errorf("removing memlock rlimit: %w", err)
	}

	spec, err := libbpf.LoadCollectionSpec(bpfObjPath)
	if err != nil {
		return nil, fmt.Errorf("loading BPF collection spec from %s: %w", bpfObjPath, err)
	}

	coll, err := libbpf.NewCollection(spec)
	if err != nil {
		return nil, fmt.Errorf("creating BPF collection: %w", err)
	}

	objs := &bpfObjects{
		PidFilter:           coll.Maps["pid_filter"],
		EntryTimes:          coll.Maps["entry_times"],
		EntryArgs:           coll.Maps["entry_args"],
		Events:              coll.Maps["events"],
		HeliosSysWriteEntry: coll.Programs["helios_sys_write_entry"],
		HeliosSysWriteExit:  coll.Programs["helios_sys_write_exit"],
		HeliosSysFsyncEntry: coll.Programs["helios_sys_fsync_entry"],
		HeliosSysFsyncExit:  coll.Programs["helios_sys_fsync_exit"],
	}

	for name, m := range map[string]*libbpf.Map{
		"pid_filter": objs.PidFilter, "entry_times": objs.EntryTimes,
		"entry_args": objs.EntryArgs, "events": objs.Events,
	} {
		if m == nil {
			return nil, fmt.Errorf("BPF map %q not found in %s", name, bpfObjPath)
		}
	}

	var links []link.Link

	// Attach kprobes. Each attach call inserts a breakpoint at the
	// function's entry point; the kretprobe variant fires on return.
	for _, probe := range []struct {
		fn   string
		prog *libbpf.Program
		ret  bool
	}{
		{"__x64_sys_write", objs.HeliosSysWriteEntry, false},
		{"__x64_sys_write", objs.HeliosSysWriteExit, true},
		{"__x64_sys_fsync", objs.HeliosSysFsyncEntry, false},
		{"__x64_sys_fsync", objs.HeliosSysFsyncExit, true},
	} {
		var l link.Link
		var lerr error
		if probe.ret {
			l, lerr = link.Kretprobe(probe.fn, probe.prog, nil)
		} else {
			l, lerr = link.Kprobe(probe.fn, probe.prog, nil)
		}
		if lerr != nil {
			for _, existing := range links {
				existing.Close()
			}
			coll.Close()
			return nil, fmt.Errorf("attaching kprobe %s (ret=%v): %w", probe.fn, probe.ret, lerr)
		}
		links = append(links, l)
	}

	reader, err := ringbuf.NewReader(objs.Events)
	if err != nil {
		for _, l := range links {
			l.Close()
		}
		coll.Close()
		return nil, fmt.Errorf("creating ring buffer reader: %w", err)
	}

	loader := &Loader{
		objs:   objs,
		links:  links,
		reader: reader,
		eventC: eventC,
		stopC:  make(chan struct{}),
	}

	loader.wg.Add(1)
	go loader.drain()

	return loader, nil
}

// RegisterPID adds a PID to the kernel-side filter map so its syscalls
// get captured. Call this for each helios-kv JVM process and helios-raft
// Go process you want to trace. Safe to call concurrently.
func (l *Loader) RegisterPID(pid uint32) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	val := uint8(1)
	return l.objs.PidFilter.Put(pid, val)
}

// UnregisterPID removes a PID from the filter map (call on process exit).
func (l *Loader) UnregisterPID(pid uint32) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.objs.PidFilter.Delete(pid)
}

// drain reads TraceEvents from the ring buffer and forwards them to eventC.
// Runs in its own goroutine until Stop() is called.
func (l *Loader) drain() {
	defer l.wg.Done()
	for {
		record, err := l.reader.Read()
		if err != nil {
			select {
			case <-l.stopC:
				return // normal shutdown
			default:
				// transient read error — log and continue
				fmt.Fprintf(os.Stderr, "[helios-ebpf] ring buffer read error: %v\n", err)
				time.Sleep(10 * time.Millisecond)
				continue
			}
		}

		if len(record.RawSample) < int(unsafe.Sizeof(TraceEvent{})) {
			continue // truncated record, skip
		}

		var e TraceEvent
		e.TimestampNs = binary.LittleEndian.Uint64(record.RawSample[0:8])
		e.DurationNs  = binary.LittleEndian.Uint64(record.RawSample[8:16])
		e.Pid         = binary.LittleEndian.Uint32(record.RawSample[16:20])
		e.SyscallType = binary.LittleEndian.Uint32(record.RawSample[20:24])
		e.Fd          = binary.LittleEndian.Uint32(record.RawSample[24:28])
		e.Bytes       = binary.LittleEndian.Uint32(record.RawSample[28:32])

		select {
		case l.eventC <- e:
		case <-l.stopC:
			return
		default:
			// eventC is full — drop rather than block the drain goroutine,
			// which would cause the ring buffer to fill and drop events in
			// the kernel instead (worse: kernel drops are invisible).
		}
	}
}

// Stop detaches kprobes, closes the ring buffer reader, and waits for
// the drain goroutine to exit.
func (l *Loader) Stop() {
	close(l.stopC)
	l.reader.Close()
	l.wg.Wait()
	for _, lnk := range l.links {
		lnk.Close()
	}
}

// FindBPFObject locates helios_trace.bpf.o relative to the current
// executable, which handles both "run from the repo root" and
// "installed somewhere".
func FindBPFObject() (string, error) {
	exe, err := os.Executable()
	if err != nil {
		return "", err
	}
	candidates := []string{
		filepath.Join(filepath.Dir(exe), "..", "kernel", "helios_trace.bpf.o"),
		filepath.Join(filepath.Dir(exe), "helios_trace.bpf.o"),
		"ebpf/kernel/helios_trace.bpf.o",
		"kernel/helios_trace.bpf.o",
	}
	for _, c := range candidates {
		if _, err := os.Stat(c); err == nil {
			return c, nil
		}
	}
	return "", fmt.Errorf("helios_trace.bpf.o not found (tried: %v) — run 'make' in ebpf/kernel/ first", candidates)
}

// mustBeLinux panics if called on a non-Linux platform — BPF is Linux-only
// and there's no point pretending otherwise.
func init() {
	if runtime.GOOS != "linux" {
		panic("helios-ebpf requires Linux")
	}
}
