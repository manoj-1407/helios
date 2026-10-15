package raft

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"sync"
)

type LogEntry struct {
	Term    uint64
	Index   uint64
	Command []byte
}

// RaftLog is the persistent log abstraction every Raft node keeps. Index 0
// is always a sentinel entry (Term: 0, Index: 0, Command: nil) so that
// "the entry before index 1" is a real, addressable thing instead of a
// special case scattered through raft.go -- LastIndexAndTerm() on a
// completely empty log just returns the sentinel's (0, 0), which is
// exactly the right answer for "this log has nothing in it yet."
//
// On-disk format is one append-only file, record framing deliberately
// mirroring helios-kv's WAL (CRC32 checksum, length-prefixed, explicit
// torn-trailing-record tolerance on recovery) -- same engineering
// reasoning applies in both languages: a half-written record at the tail
// is the expected shape of an interrupted write, not a bug, so recovery
// stops there instead of throwing.
//
//	[term uint64][index uint64][cmdLen uint32][checksum uint32][cmd bytes]
//
// TruncateFrom needs to physically shrink the file, not just the
// in-memory slice -- a crash after an in-memory-only truncate would replay
// the "removed" entries right back on the next restart. That requires
// knowing the exact byte offset where each entry starts, which is what
// the parallel `offsets` slice is for: offsets[i] is the file offset
// where entries[i]'s record begins, so TruncateFrom(index) can call
// file.Truncate(offsets[index]) directly instead of rewriting the file.
type RaftLog struct {
	mu      sync.RWMutex
	entries []LogEntry
	offsets []int64 // offsets[i] is the byte offset in file where entries[i] starts
	path    string
	file    *os.File
}

const logRecordHeaderSize = 8 + 8 + 4 + 4 // term + index + cmdLen + checksum

// NewRaftLog opens (or creates) the log file at path and replays whatever
// is already there. A brand new node and a node recovering from a crash
// go through exactly the same code path here -- there's no separate
// "first boot" branch, which is itself a small correctness feature: it
// means recovery logic actually runs on every single startup, not just
// the ones where something went wrong, so a bug in recovery would show up
// immediately instead of hiding until the first real crash.
func NewRaftLog(path string) (*RaftLog, error) {
	l := &RaftLog{
		path:    path,
		entries: []LogEntry{{Term: 0, Index: 0, Command: nil}}, // sentinel
		offsets: []int64{0},
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("opening raft log file: %w", err)
	}
	l.file = f

	if err := l.replay(); err != nil {
		f.Close()
		return nil, fmt.Errorf("replaying raft log: %w", err)
	}
	return l, nil
}

func (l *RaftLog) replay() error {
	info, err := l.file.Stat()
	if err != nil {
		return err
	}
	size := info.Size()

	var offset int64 = 0
	for offset+logRecordHeaderSize <= size {
		header := make([]byte, logRecordHeaderSize)
		if _, err := l.file.ReadAt(header, offset); err != nil {
			break // torn header at EOF -- stop, don't fail, same as helios-kv's WAL
		}
		term := binary.BigEndian.Uint64(header[0:8])
		index := binary.BigEndian.Uint64(header[8:16])
		cmdLen := binary.BigEndian.Uint32(header[16:20])
		storedChecksum := binary.BigEndian.Uint32(header[20:24])

		if cmdLen > 64*1024*1024 {
			break // garbage header -- treat as the end of valid data
		}
		bodyStart := offset + logRecordHeaderSize
		if bodyStart+int64(cmdLen) > size {
			break // truncated body -- this is where a crash happened
		}
		cmd := make([]byte, cmdLen)
		if cmdLen > 0 {
			if _, err := l.file.ReadAt(cmd, bodyStart); err != nil {
				break
			}
		}
		if crc32.ChecksumIEEE(cmd) != storedChecksum {
			break // corrupt, not just truncated -- same treatment: stop here
		}

		l.entries = append(l.entries, LogEntry{Term: term, Index: index, Command: cmd})
		l.offsets = append(l.offsets, offset)
		offset = bodyStart + int64(cmdLen)
	}

	// If replay stopped early because of a torn/corrupt trailing record,
	// physically truncate the file to the last clean boundary -- otherwise
	// the next Append() would write new, valid records right after garbage
	// bytes that a future replay might misinterpret.
	if offset < size {
		if err := l.file.Truncate(offset); err != nil {
			return fmt.Errorf("truncating torn trailing record: %w", err)
		}
		if err := l.file.Sync(); err != nil {
			return fmt.Errorf("fsyncing after truncating torn record: %w", err)
		}
	}
	return nil
}

// Append writes entries to disk (fsync) before returning -- this is the
// persistence guarantee log entries get. Caller must already hold
// whatever invariant ordering Raft requires (entries persisted before
// replying to AppendEntries); this method itself just guarantees that
// once it returns nil, the entries survive a crash.
func (l *RaftLog) Append(entries ...LogEntry) error {
	if len(entries) == 0 {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	offset, err := l.file.Seek(0, os.SEEK_END)
	if err != nil {
		return fmt.Errorf("seeking to end of log file: %w", err)
	}

	for _, e := range entries {
		header := make([]byte, logRecordHeaderSize)
		binary.BigEndian.PutUint64(header[0:8], e.Term)
		binary.BigEndian.PutUint64(header[8:16], e.Index)
		binary.BigEndian.PutUint32(header[16:20], uint32(len(e.Command)))
		binary.BigEndian.PutUint32(header[20:24], crc32.ChecksumIEEE(e.Command))

		if _, err := l.file.Write(header); err != nil {
			return fmt.Errorf("writing log record header: %w", err)
		}
		if len(e.Command) > 0 {
			if _, err := l.file.Write(e.Command); err != nil {
				return fmt.Errorf("writing log record body: %w", err)
			}
		}

		l.offsets = append(l.offsets, offset)
		l.entries = append(l.entries, e)
		offset += int64(logRecordHeaderSize) + int64(len(e.Command))
	}

	if err := l.file.Sync(); err != nil {
		return fmt.Errorf("fsyncing log file: %w", err)
	}
	return nil
}

// Get returns the entry at the given index (1-based; index 0 is the
// sentinel and is a valid, addressable entry too).
func (l *RaftLog) Get(index uint64) (LogEntry, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if index >= uint64(len(l.entries)) {
		return LogEntry{}, false
	}
	return l.entries[index], true
}

// LastIndexAndTerm returns the index and term of the last entry -- used in
// RequestVote/PreVote to check log up-to-date-ness, and in AppendEntries
// to know what PrevLogIndex/PrevLogTerm to send a follower.
func (l *RaftLog) LastIndexAndTerm() (uint64, uint64) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	last := l.entries[len(l.entries)-1]
	return last.Index, last.Term
}

// TruncateFrom removes all entries from index onward (called when a
// leader's AppendEntries tells us our log diverged at that point) and
// physically truncates the backing file to match, then fsyncs. Skipping
// the fsync here is exactly the bug the class doc warns about: a crash
// between truncating in memory and the bytes actually being gone would
// resurrect the "removed" entries on the next replay.
func (l *RaftLog) TruncateFrom(index uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if index == 0 {
		return fmt.Errorf("cannot truncate the sentinel entry at index 0")
	}
	if index >= uint64(len(l.entries)) {
		return nil // nothing to truncate, index is past the end already
	}

	fileOffset := l.offsets[index]
	if err := l.file.Truncate(fileOffset); err != nil {
		return fmt.Errorf("truncating log file: %w", err)
	}
	if err := l.file.Sync(); err != nil {
		return fmt.Errorf("fsyncing after truncate: %w", err)
	}
	l.entries = l.entries[:index]
	l.offsets = l.offsets[:index]
	return nil
}

// Slice returns entries in [lo, hi) -- used for AppendEntries batching.
// Returns a copy, not a reference into the internal slice, so the caller
// can't accidentally corrupt RaftLog state by mutating what they got back.
func (l *RaftLog) Slice(lo, hi uint64) []LogEntry {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if lo > uint64(len(l.entries)) {
		lo = uint64(len(l.entries))
	}
	if hi > uint64(len(l.entries)) {
		hi = uint64(len(l.entries))
	}
	if lo >= hi {
		return nil
	}
	out := make([]LogEntry, hi-lo)
	copy(out, l.entries[lo:hi])
	return out
}

func (l *RaftLog) Close() error {
	return l.file.Close()
}

// TermAt returns the term of the entry at the given index.
// Returns 0, nil for the sentinel (index 0).
// Returns 0, error if the index is out of range.
func (l *RaftLog) TermAt(index uint64) (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if index >= uint64(len(l.entries)) {
		return 0, fmt.Errorf("log: TermAt(%d) out of range (len=%d)", index, len(l.entries))
	}
	return l.entries[index].Term, nil
}

// CompactTo removes all entries with Index ≤ upTo from the in-memory slice
// and truncates the backing file to remove only the compacted records.
// This is called after a snapshot is persisted so the log doesn't grow
// forever. Unlike TruncateFrom (which removes a diverging suffix),
// CompactTo removes a committed prefix.
//
// The sentinel at position 0 is replaced with a placeholder entry at
// upTo so that LastIndexAndTerm() and Slice() continue to work — the
// "last entry before our window" is still addressable.
func (l *RaftLog) CompactTo(upTo uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if upTo == 0 || upTo >= uint64(len(l.entries)) {
		return nil // nothing to compact or already past the end
	}

	// The file offset where the first kept entry starts.
	keepFrom := int64(l.offsets[upTo+1]) // offsets has len(entries) elements

	// Compact in memory: replace entries[0..upTo] with a single sentinel
	// at upTo carrying the right term/index so log lookups still work.
	// (The file content for those entries is gone after the truncate below.)
	kept := l.entries[upTo+1:]
	sentinel := LogEntry{Term: l.entries[upTo].Term, Index: upTo, Command: nil}
	newEntries := make([]LogEntry, 1+len(kept))
	newEntries[0] = sentinel
	copy(newEntries[1:], kept)

	keptOffsets := l.offsets[upTo+1:]
	newOffsets := make([]int64, 1+len(keptOffsets))
	newOffsets[0] = 0
	// Shift file offsets so they're relative to the new file start.
	for i, off := range keptOffsets {
		newOffsets[i+1] = off - keepFrom
	}

	// Rewrite file: copy the tail we're keeping to a temp path, then rename.
	// This avoids the window where the file is partly overwritten.
	tmpPath := l.path + ".compact.tmp"
	tmpF, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("log compact: open tmp: %w", err)
	}

	// Read the tail from the current file.
	tailLen, err := l.file.Seek(0, 2) // seek to end to know file size
	if err != nil {
		tmpF.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("log compact: seek: %w", err)
	}
	tail := make([]byte, tailLen-keepFrom)
	if _, err := l.file.ReadAt(tail, keepFrom); err != nil {
		tmpF.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("log compact: read tail: %w", err)
	}
	if _, err := tmpF.Write(tail); err != nil {
		tmpF.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("log compact: write tmp: %w", err)
	}
	if err := tmpF.Sync(); err != nil {
		tmpF.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("log compact: sync tmp: %w", err)
	}
	tmpF.Close()

	if err := os.Rename(tmpPath, l.path); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("log compact: rename: %w", err)
	}

	// Re-open so we have a writable handle to the new file.
	l.file.Close()
	newF, err := os.OpenFile(l.path, os.O_RDWR, 0o644)
	if err != nil {
		return fmt.Errorf("log compact: reopen: %w", err)
	}
	l.file = newF
	l.entries = newEntries
	l.offsets = newOffsets
	return nil
}
