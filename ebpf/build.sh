#!/bin/bash
# Run this once on your WSL2 machine to build helios-ebpf.
# Requires: sudo apt-get install -y clang llvm libbpf-dev linux-tools-generic (already done)
# Requires: internet access for go mod tidy
set -e

echo "=== Step 1: compile BPF kernel program ==="
cd kernel
make
cd ..

echo "=== Step 2: fetch Go dependencies ==="
go mod tidy

echo "=== Step 3: build Go binary ==="
go build -o helios-ebpf ./cmd/helios-ebpf/

echo ""
echo "Build complete. Run with:"
echo "  sudo ./helios-ebpf -pids \$(pgrep -f 'helios-kv.jar' | tr '\n' ',') -v"
echo "  Metrics: curl http://localhost:9090/metrics"
