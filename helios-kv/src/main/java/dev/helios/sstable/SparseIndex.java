package dev.helios.sstable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * In-memory sparse index over an SSTable's data file: one entry every N
 * keys (a "block"), storing the actual key bytes at that boundary plus the
 * byte offset where its record starts. A point lookup binary-searches this
 * list to find which block the target key would fall in, then the caller
 * scans forward in the data file from that offset.
 *
 * This stores real key bytes rather than just key *lengths*. An index that
 * only remembers "the key at this offset was 7 bytes long" can't actually
 * binary search anything — you'd still have to seek and read to compare.
 * Storing the key itself means the entire navigation step happens in
 * memory with zero disk I/O, and only the final block scan touches disk.
 * That's the whole point of a sparse index: O(log blocks) in memory,
 * O(block size) on disk, instead of O(log n) disk seeks.
 */
public final class SparseIndex {

    public static final class Entry implements Comparable<Entry> {
        final byte[] key;
        final long offset;

        Entry(byte[] key, long offset) {
            this.key = key;
            this.offset = offset;
        }

        @Override
        public int compareTo(Entry other) {
            return Arrays.compareUnsigned(key, other.key);
        }
    }

    private final List<Entry> entries = new ArrayList<>();

    public void add(byte[] key, long offset) {
        entries.add(new Entry(key, offset));
    }

    /**
     * Returns the data-file offset to start a forward scan from in order to
     * find {@code key}, or 0 if {@code key} could only exist before the
     * first indexed block. Because the data file is sorted, the caller can
     * stop scanning the moment it reads a key greater than the target —
     * there's no need to scan past the next block boundary.
     */
    public long floorOffset(byte[] key) {
        int lo = 0, hi = entries.size() - 1;
        int result = -1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            int cmp = Arrays.compareUnsigned(entries.get(mid).key, key);
            if (cmp <= 0) {
                result = mid;
                lo = mid + 1;
            } else {
                hi = mid - 1;
            }
        }
        return result == -1 ? 0L : entries.get(result).offset;
    }

    public int blockCount() {
        return entries.size();
    }

    public byte[] minKey() {
        return entries.isEmpty() ? null : entries.get(0).key;
    }

    /**
     * Serialized layout: [count: int32] then repeated [keyLen: int32][offset: int64][key bytes].
     * Small enough that we don't bother with a smarter encoding — sparse
     * indexes are, by construction, a small fraction of the data they index.
     */
    public byte[] toBytes() {
        int total = 4;
        for (Entry e : entries) total += 4 + 8 + e.key.length;
        java.nio.ByteBuffer buf = java.nio.ByteBuffer.allocate(total);
        buf.putInt(entries.size());
        for (Entry e : entries) {
            buf.putInt(e.key.length);
            buf.putLong(e.offset);
            buf.put(e.key);
        }
        return buf.array();
    }

    public static SparseIndex fromBytes(byte[] data) {
        SparseIndex idx = new SparseIndex();
        java.nio.ByteBuffer buf = java.nio.ByteBuffer.wrap(data);
        int count = buf.getInt();
        for (int i = 0; i < count; i++) {
            int keyLen = buf.getInt();
            long offset = buf.getLong();
            byte[] key = new byte[keyLen];
            buf.get(key);
            idx.entries.add(new Entry(key, offset));
        }
        return idx;
    }
}
