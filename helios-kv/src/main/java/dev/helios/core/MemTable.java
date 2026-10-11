package dev.helios.core;

import java.util.Arrays;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * In-memory write buffer. Backed by ConcurrentSkipListMap rather than a
 * TreeMap because writes and reads need to interleave without a global
 * lock — TreeMap's iterators throw ConcurrentModificationException the
 * moment a background flush thread starts reading while a foreground
 * thread is still writing, and wrapping it in a ReentrantReadWriteLock
 * just to get the same guarantee defeats the point. ConcurrentSkipListMap
 * gives weakly-consistent iteration (a flush sees a point-in-time-ish
 * snapshot, never a torn read) without blocking concurrent put()/get().
 *
 * One sharp edge that bit the first version of this class:
 * ConcurrentSkipListMap, like ConcurrentHashMap, throws a NullPointerException
 * if you try to store a null value — null is reserved internally to mean
 * "absent". Tombstones (deletes) are represented everywhere else in this
 * engine (WAL, SSTableReader, the public HeliosDB API) as a literal null
 * value, which is the natural representation. So internally, this class
 * stores a private TOMBSTONE sentinel instead and translates at the two
 * boundaries that matter — get() and snapshot() — back into real null,
 * so nothing outside this file ever has to know the sentinel exists.
 *
 * The size accounting is approximate on purpose. We're tracking key+value
 * byte lengths, not skip-list node overhead (forward pointers, levels),
 * so the real heap footprint runs higher than sizeBytes() reports. That's
 * fine — we flush at a threshold comfortably below available heap, we
 * don't need byte-exact accounting to decide "is it time to flush".
 */
public final class MemTable {

    /** Reference-identity sentinel — byte[] has no overridden equals(), so this is only ever "equal" to itself. Never returned to callers. */
    private static final byte[] TOMBSTONE = new byte[0];

    private final ConcurrentSkipListMap<byte[], byte[]> table;
    private final AtomicLong sizeBytes;
    private final long maxSizeBytes;

    public MemTable(long maxSizeBytes) {
        this.table = new ConcurrentSkipListMap<>(Arrays::compareUnsigned);
        this.sizeBytes = new AtomicLong(0);
        this.maxSizeBytes = maxSizeBytes;
    }

    /**
     * A null value marks a tombstone (delete) at this class's public
     * boundary. We never physically remove a key from the MemTable on
     * delete — that would let an older value sitting in an already-flushed
     * SSTable "win" on the next read. The tombstone has to survive until
     * compaction sees it and can safely drop both the tombstone and the
     * shadowed value together.
     */
    public void put(byte[] key, byte[] value) {
        byte[] toStore = (value == null) ? TOMBSTONE : value;
        byte[] previous = table.put(key, toStore);

        int newLen = (value == null) ? 0 : value.length;
        int oldLen = (previous == null || previous == TOMBSTONE) ? 0 : previous.length;
        long delta = key.length + newLen - (previous == null ? 0 : key.length + oldLen);
        sizeBytes.addAndGet(Math.max(delta, 0));
    }

    public void delete(byte[] key) {
        put(key, null);
    }

    /** Returns the value, or null for both "deleted" and "never present" — callers that must tell those apart should use containsKey() first. */
    public byte[] get(byte[] key) {
        byte[] v = table.get(key);
        return (v == TOMBSTONE) ? null : v;
    }

    public boolean containsKey(byte[] key) {
        return table.containsKey(key);
    }

    public boolean shouldFlush() {
        return sizeBytes.get() >= maxSizeBytes;
    }

    public long sizeBytes() {
        return sizeBytes.get();
    }

    public int entryCount() {
        return table.size();
    }

    public boolean isEmpty() {
        return table.isEmpty();
    }

    /**
     * Point-in-time view for flushing, with TOMBSTONE sentinels translated
     * back to real null so SSTableWriter — which only knows about the
     * public null-means-tombstone convention — doesn't need to know this
     * class has a sentinel at all. This does mean a flush copies the live
     * entry set into a plain TreeMap rather than handing out the skip list
     * directly; for a structure that's already bounded by maxSizeBytes
     * before it gets here, that copy is a rounding error next to the disk
     * I/O the flush is about to do, and it buys back a real correctness
     * guarantee (no sentinel ever escapes this file) instead of a fake
     * "zero-copy" that quietly corrupts deletes into empty-value writes.
     */
    public NavigableMap<byte[], byte[]> snapshot() {
        NavigableMap<byte[], byte[]> copy = new TreeMap<>(Arrays::compareUnsigned);
        for (Map.Entry<byte[], byte[]> e : table.entrySet()) {
            copy.put(e.getKey(), e.getValue() == TOMBSTONE ? null : e.getValue());
        }
        return copy;
    }
}
