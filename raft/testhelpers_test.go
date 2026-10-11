package raft

import (
	"os"
	"testing"
)

func fileSize(t *testing.T, path string) int64 {
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	return info.Size()
}

func truncateFile(t *testing.T, path string, size int64) {
	if err := os.Truncate(path, size); err != nil {
		t.Fatalf("truncate %s to %d: %v", path, size, err)
	}
}
