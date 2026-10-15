package dev.helios.sstable;

import dev.helios.bloom.BloomFilter;

import java.nio.file.Path;

/**
 * Immutable handle to one flushed SSTable: where its data file lives, the
 * sparse index and bloom filter that were built while writing it, the
 * compaction level it belongs to, and a monotonic id used both for the
 * filename and for tie-breaking "which SSTable is newer" during reads
 * (newest-to-oldest is the read order, since a more recent write shadows
 * an older one for the same key).
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
