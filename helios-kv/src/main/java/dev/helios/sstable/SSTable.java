package dev.helios.sstable;

import dev.helios.bloom.BloomFilter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Immutable handle to one flushed SSTable: where its data file lives, the
 * sparse index and bloom filter that were built while writing it, the
 * compaction level it belongs to, and a monotonic id used both for the
 * filename and for tie-breaking "which SSTable is newer" during reads
 * (newest-to-oldest is the read order, since a more recent write shadows
 * an older one for the same key).
 *
 * <h3>Reference counting</h3>
 * Every SSTable starts with a reference count of 1 — the reference held by the
 * live-SSTable list in HeliosDB. Callers that need the file to stay alive across
 * an async operation (typically SSTableReader performing file I/O) call
 * {@link #retain()} before opening the file and {@link #release()} in a
 * {@code finally} block. When the count reaches zero the backing file is deleted.
 *
 * <p>The ordering that makes this safe: CompactionManager removes the SSTable from
 * the live list <em>before</em> calling {@code release()} on it. A reader that
 * grabbed the reference while it was still in the list will have called
 * {@code retain()} first, so the count never drops to zero while the file is open.
 * A reader that races with the removal — arriving after the table left the list —
 * never sees the table at all and so never opens the file.
 */
public final class SSTable {

    private final Path dataFile;
    private final BloomFilter bloomFilter;
    private final SparseIndex sparseIndex;
    private final int level;
    private final long id;
    private final byte[] minKey;
    private final byte[] maxKey;
    private final int entryCount;
    // Set by SSTableReader.open() after reading the footer; defaults to 2 for
    // freshly-written tables (which are always v2). Only v1 legacy files get 1.
    private int formatVersion = SSTableWriter.FORMAT_VERSION;

    /**
     * Reference count. Starts at 1 (held by the live-SSTable list).
     * retain() / release() are the only mutators; release() deletes the file at zero.
     */
    private final AtomicInteger refCount = new AtomicInteger(1);

    public SSTable(Path dataFile, BloomFilter bloomFilter, SparseIndex sparseIndex,
                   int level, long id, byte[] minKey, byte[] maxKey, int entryCount) {
        this.dataFile = dataFile;
        this.bloomFilter = bloomFilter;
        this.sparseIndex = sparseIndex;
        this.level = level;
        this.id = id;
        this.minKey = minKey;
        this.maxKey = maxKey;
        this.entryCount = entryCount;
    }

    public Path dataFile() { return dataFile; }
    public BloomFilter bloomFilter() { return bloomFilter; }
    public SparseIndex sparseIndex() { return sparseIndex; }
    public int level() { return level; }
    public long id() { return id; }
    public byte[] minKey() { return minKey; }
    public byte[] maxKey() { return maxKey; }
    public int entryCount() { return entryCount; }
    public int formatVersion() { return formatVersion; }
    public void setFormatVersion(int v) { this.formatVersion = v; }

    /**
     * Increments the reference count. Call this before any operation that keeps
     * the file open asynchronously — i.e. before calling into SSTableReader.
     * Must be paired with a {@link #release()} in a {@code finally} block.
     *
     * @throws IllegalStateException if the table has already been fully released
     *         (count already at zero, meaning someone called retain() after the
     *         file was deleted — always a bug).
     */
    public void retain() {
        int prev = refCount.getAndIncrement();
        if (prev <= 0) {
            // Undo the increment so the count stays accurate, then throw.
            refCount.decrementAndGet();
            throw new IllegalStateException(
                "retain() called on already-released SSTable " + dataFile);
        }
    }

    /**
     * Decrements the reference count. When it reaches zero the backing data file
     * is deleted from disk. Safe to call from any thread; idempotent at zero.
     */
    public void release() {
        int remaining = refCount.decrementAndGet();
        if (remaining < 0) {
            // Over-release: restore to zero to keep the count consistent and log loudly
            // rather than crashing — a negative count is a double-release bug, but
            // crashing here would surface as an unrelated error at the call site.
            refCount.set(0);
            System.err.println("[helios-kv] WARN: over-release on SSTable " + dataFile +
                               " (count went negative). This is a ref-counting bug.");
            return;
        }
        if (remaining == 0) {
            try {
                Files.deleteIfExists(dataFile);
            } catch (IOException e) {
                // Log but don't throw: a failed delete leaves a stale file on disk
                // (disk-space waste) but does NOT affect correctness — the table is
                // already gone from the live-SSTable list and will never be read again.
                System.err.println("[helios-kv] WARN: could not delete retired SSTable "
                                   + dataFile + ": " + e.getMessage());
            }
        }
    }

    /** Cheap pre-filter before even touching the bloom filter or the file: is the key in this table's key range at all? */
    public boolean mayContainRange(byte[] key) {
        if (minKey == null || maxKey == null) return false;
        return java.util.Arrays.compareUnsigned(key, minKey) >= 0
                && java.util.Arrays.compareUnsigned(key, maxKey) <= 0;
    }

    @Override
    public String toString() {
        return "SSTable[level=" + level + ", id=" + id + ", entries=" + entryCount
                + ", file=" + dataFile.getFileName() + "]";
    }
}
