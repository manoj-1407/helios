package dev.helios.bloom;

import java.util.BitSet;

/**
 * Per-SSTable bloom filter, sized for a target false positive rate.
 *
 * Design note: instead of calling a hash function k separate times (the
 * naive approach, and the thing that makes bloom filters slow), this uses
 * Kirsch-Mitzenmacher double hashing — one 64-bit MurmurHash3 call split
 * into two 32-bit halves (h1, h2), then the i-th probe is g_i(x) = h1 + i*h2.
 * This is provably as good as k independent hash functions for the false
 * positive math, at a fraction of the cost. That's the actual reason every
 * production bloom filter (RocksDB's included) does it this way rather than
 * the textbook "k hash functions" description.
 */
public final class BloomFilter {

    private final BitSet bits;
    private final int hashCount;
    private final int bitSize;
    private int insertedKeys = 0;

    public BloomFilter(long expectedInsertions, double falsePositiveRate) {
        if (expectedInsertions <= 0) expectedInsertions = 1;
        this.bitSize = optimalBitSize(expectedInsertions, falsePositiveRate);
        this.hashCount = optimalHashCount(bitSize, expectedInsertions);
        this.bits = new BitSet(bitSize);
    }

    /** Used when deserializing a filter from disk — bits are restored directly. */
    private BloomFilter(BitSet bits, int hashCount, int bitSize) {
        this.bits = bits;
        this.hashCount = hashCount;
        this.bitSize = bitSize;
    }

    public void add(byte[] key) {
        long hash = MurmurHash3.hash64(key);
        int h1 = (int) (hash >>> 32);
        int h2 = (int) hash;
        for (int i = 0; i < hashCount; i++) {
            bits.set(probe(h1, h2, i));
        }
        insertedKeys++;
    }

    /** False means "definitely not present" — safe to skip this SSTable entirely. */
    public boolean mightContain(byte[] key) {
        long hash = MurmurHash3.hash64(key);
        int h1 = (int) (hash >>> 32);
        int h2 = (int) hash;
        for (int i = 0; i < hashCount; i++) {
            if (!bits.get(probe(h1, h2, i))) return false;
        }
        return true;
    }

    private int probe(int h1, int h2, int i) {
        long combined = h1 + (long) i * h2;
        long mod = combined % bitSize;
        if (mod < 0) mod += bitSize;
        return (int) mod;
    }

    public int insertedKeys() { return insertedKeys; }
    public int sizeBits() { return bitSize; }
    public int hashCount() { return hashCount; }

    /** Approximate current false positive probability given what's actually been inserted. */
    public double currentFalsePositiveRate() {
        if (insertedKeys == 0) return 0.0;
        return Math.pow(1 - Math.exp(-hashCount * (double) insertedKeys / bitSize), hashCount);
    }

    private static int optimalBitSize(long n, double p) {
        double m = -n * Math.log(p) / (Math.log(2) * Math.log(2));
        return Math.max(64, (int) Math.ceil(m));
    }

    private static int optimalHashCount(int m, long n) {
        return Math.max(1, (int) Math.round((double) m / n * Math.log(2)));
    }

    // --- serialization: bloom filters live alongside SSTables on disk ---

    public byte[] toBytes() {
        byte[] raw = bits.toByteArray();
        byte[] out = new byte[16 + raw.length];
        putInt(out, 0, bitSize);
        putInt(out, 4, hashCount);
        putInt(out, 8, insertedKeys);
        putInt(out, 12, raw.length);
        System.arraycopy(raw, 0, out, 16, raw.length);
        return out;
    }

    public static BloomFilter fromBytes(byte[] data) {
        int bitSize = getInt(data, 0);
        int hashCount = getInt(data, 4);
        int inserted = getInt(data, 8);
        int rawLen = getInt(data, 12);
        byte[] raw = new byte[rawLen];
        System.arraycopy(data, 16, raw, 0, rawLen);
        BloomFilter bf = new BloomFilter(BitSet.valueOf(raw), hashCount, bitSize);
        bf.insertedKeys = inserted;
        return bf;
    }

    private static void putInt(byte[] arr, int off, int v) {
        arr[off] = (byte) (v >>> 24);
        arr[off + 1] = (byte) (v >>> 16);
        arr[off + 2] = (byte) (v >>> 8);
        arr[off + 3] = (byte) v;
    }

    private static int getInt(byte[] arr, int off) {
        return ((arr[off] & 0xff) << 24) | ((arr[off + 1] & 0xff) << 16)
                | ((arr[off + 2] & 0xff) << 8) | (arr[off + 3] & 0xff);
    }
}
