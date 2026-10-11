GOBIN ?= $(shell go env GOPATH)/bin

.PHONY: all kernel userspace clean run help

all: kernel userspace

# Step 1: compile the BPF C program to an ELF object
kernel:
	$(MAKE) -C kernel all

# Step 2: build the Go userspace binary
userspace:
	go mod download
	go build -o helios-ebpf ./cmd/helios-ebpf/

clean:
	$(MAKE) -C kernel clean
	rm -f helios-ebpf

# Run: automatically detect helios-kv and helios-raft PIDs and start tracing
run: all
	@echo "Detecting helios PIDs..."
	@PIDS=$$(pgrep -f "helios-kv.jar" | tr '\n' ',' | sed 's/,$$//'); \
	RAFT_PIDS=$$(pgrep -f "helios-raft" | tr '\n' ',' | sed 's/,$$//'); \
	ALL_PIDS=$${PIDS},$${RAFT_PIDS}; \
	ALL_PIDS=$$(echo $$ALL_PIDS | tr -s ',' | sed 's/^,//;s/,$$//'); \
	if [ -z "$$ALL_PIDS" ]; then \
		echo "No helios processes found. Start the benchmark first, then run 'make run'."; \
		echo "Or specify PIDs manually: sudo ./helios-ebpf -pids PID1,PID2,PID3"; \
		exit 1; \
	fi; \
	echo "Tracing PIDs: $$ALL_PIDS"; \
	sudo ./helios-ebpf -pids $$ALL_PIDS -v

help:
	@echo "helios-ebpf build targets:"
	@echo "  make all       — build kernel BPF object + Go binary"
	@echo "  make kernel    — compile helios_trace.bpf.c → helios_trace.bpf.o"
	@echo "  make userspace — compile Go loader/correlator/exporter"
	@echo "  make run       — build and start tracing (auto-detects PIDs)"
	@echo "  make clean     — remove build artifacts"
	@echo ""
	@echo "Manual usage:"
	@echo "  sudo ./helios-ebpf -pids PID1,PID2 -v"
	@echo "  Metrics: curl http://localhost:9090/metrics"
