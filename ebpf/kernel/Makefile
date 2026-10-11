# Builds helios_trace.bpf.o using CO-RE (Compile Once, Run Everywhere).
# CO-RE uses BTF (BPF Type Format) embedded in the kernel (CONFIG_DEBUG_INFO_BTF=y,
# which this WSL2 kernel has) so the compiled .o runs on any kernel with the same
# BTF info -- no need to carry around kernel headers or rebuild per-kernel.
#
# The vmlinux.h header is generated from the running kernel's BTF via bpftool.
# It contains every kernel type definition in a single header, which is what
# lets BPF CO-RE programs access kernel structs without including kernel headers.

CLANG ?= clang
ARCH  ?= x86

BPF_CFLAGS = \
    -O2 -g \
    -target bpf \
    -D__TARGET_ARCH_$(ARCH) \
    -I/usr/include \
    -I/usr/include/bpf \
    -I. \
    -Wall \
    -Wno-unused-value \
    -Wno-pointer-sign \
    -Wno-compare-distinct-pointer-types

.PHONY: all clean vmlinux

all: vmlinux.h helios_trace.bpf.o

# Generate vmlinux.h from the running kernel's BTF.
# Requires bpftool; if it's not available, fall back to the system BTF file.
vmlinux.h:
	@if command -v bpftool >/dev/null 2>&1; then \
		echo "generating vmlinux.h from running kernel BTF via bpftool..."; \
		bpftool btf dump file /sys/kernel/btf/vmlinux format c > vmlinux.h; \
	elif [ -f /sys/kernel/btf/vmlinux ]; then \
		echo "bpftool not found; trying python-based BTF dump..."; \
		python3 /usr/share/bpftool/btf_to_h.py /sys/kernel/btf/vmlinux > vmlinux.h 2>/dev/null || \
		(echo "ERROR: cannot generate vmlinux.h — install bpftool: sudo apt-get install linux-tools-common linux-tools-generic" && exit 1); \
	else \
		echo "ERROR: /sys/kernel/btf/vmlinux not found — kernel needs CONFIG_DEBUG_INFO_BTF=y"; \
		exit 1; \
	fi

helios_trace.bpf.o: helios_trace.bpf.c vmlinux.h
	$(CLANG) $(BPF_CFLAGS) -c helios_trace.bpf.c -o helios_trace.bpf.o
	@echo "compiled: helios_trace.bpf.o"
	@file helios_trace.bpf.o

clean:
	rm -f helios_trace.bpf.o vmlinux.h
