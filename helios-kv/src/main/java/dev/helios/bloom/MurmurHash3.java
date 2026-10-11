package dev.helios.bloom;

/**
 * Minimal 64-bit MurmurHash3 (x64_128 variant, lower 64 bits only).
 *
 * There is no MurmurHash3 in the JDK standard library, and pulling in a
 * dependency (Guava) for one hash function felt like overkill for something
 * this small and well-specified. This is the Austin Appleby reference
 * algorithm, just ported to Java with unsigned-safe arithmetic.
 *
 * We only need the lower 64 bits because BloomFilter derives its k hash
 * functions from two 32-bit halves of a single 64-bit hash (Kirsch-Mitzenmacher
 * double hashing) rather than calling a hash function k separate times.
 */
public final class MurmurHash3 {

    private static final long C1 = 0x87c37b91114253d5L;
    private static final long C2 = 0x4cf5ad432745937fL;

    private MurmurHash3() {}

    public static long hash64(byte[] data) {
        return hash64(data, 0x9e3779b9L);
    }

    public static long hash64(byte[] data, long seed) {
        long h1 = seed;
        long h2 = seed;
        int length = data.length;
        int nblocks = length / 16;

        for (int i = 0; i < nblocks; i++) {
            int base = i * 16;
            long k1 = getLittleEndianLong(data, base);
            long k2 = getLittleEndianLong(data, base + 8);

            k1 *= C1; k1 = Long.rotateLeft(k1, 31); k1 *= C2;
            h1 ^= k1;
            h1 = Long.rotateLeft(h1, 27);
            h1 += h2;
            h1 = h1 * 5 + 0x52dce729;

            k2 *= C2; k2 = Long.rotateLeft(k2, 33); k2 *= C1;
            h2 ^= k2;
            h2 = Long.rotateLeft(h2, 31);
            h2 += h1;
            h2 = h2 * 5 + 0x38495ab5;
        }

        // Tail (remaining bytes that don't fill a full 16-byte block).
        long k1 = 0, k2 = 0;
        int tailStart = nblocks * 16;
        int remaining = length - tailStart;
        if (remaining > 8) {
            for (int i = remaining - 1; i >= 8; i--) {
                k2 ^= ((long) (data[tailStart + i] & 0xff)) << ((i - 8) * 8);
            }
            k2 *= C2; k2 = Long.rotateLeft(k2, 33); k2 *= C1;
            h2 ^= k2;
        }
        if (remaining > 0) {
            int limit = Math.min(remaining, 8);
            for (int i = limit - 1; i >= 0; i--) {
                k1 ^= ((long) (data[tailStart + i] & 0xff)) << (i * 8);
            }
            k1 *= C1; k1 = Long.rotateLeft(k1, 31); k1 *= C2;
            h1 ^= k1;
        }

        h1 ^= length;
        h2 ^= length;
        h1 += h2;
        h2 += h1;
        h1 = fmix64(h1);
        h2 = fmix64(h2);
        h1 += h2;

        return h1;
    }

    private static long fmix64(long k) {
        k ^= k >>> 33;
        k *= 0xff51afd7ed558ccdL;
        k ^= k >>> 33;
        k *= 0xc4ceb9fe1a85ec53L;
        k ^= k >>> 33;
        return k;
    }

    private static long getLittleEndianLong(byte[] data, int offset) {
        long result = 0;
        for (int i = 0; i < 8; i++) {
            result |= ((long) (data[offset + i] & 0xff)) << (i * 8);
        }
        return result;
    }
}
