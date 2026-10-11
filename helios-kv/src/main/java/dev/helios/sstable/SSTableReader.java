package dev.helios.sstable;

import dev.helios.bloom.BloomFilter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

/**
 * Reconstructs an SSTable handle from disk (used on startup, when we don't
 * have the in-memory objects from a fresh flush() anymore) and performs
 * point lookups against the data section.
 *
 * Lookup path: bloom filter check (in memory, no I/O) -> key range check
 * (in memory) -> sparse index binary search to find a starting offset (in
 * memory) -> sequential scan of the data file from that offset (the only
 * step that touches disk), stopping the instant we read a key greater than
 * the target, since the data section is sorted and there's no point
 * scanning past where the key would have been.
 */
public final class SSTableReader {

    private SSTableReader() {}

    /** A lookup can come back three ways: not in this table at all, found-but-deleted (tombstone), or found with a value. */
    public record LookupResult(boolean found, byte[] value) {
        public static final LookupResult NOT_FOUND = new LookupResult(false, null);
        public static LookupResult tombstone() { return new LookupResult(true, null); }
        public static LookupResult value(byte[] v) { return new LookupResult(true, v); }
    }

    /** Rebuilds an SSTable handle (index + bloom filter loaded into memory) by reading the footer of an existing .sst file. */
    public static SSTable open(Path dataFile, int level, long id) throws IOException {
        try (FileChannel ch = FileChannel.open(dataFile, StandardOpenOption.READ)) {
            long fileSize = ch.size();

            ByteBuffer footerLenBuf = ByteBuffer.allocate(4);
            readAt(ch, fileSize - 4, footerLenBuf);
            footerLenBuf.flip();
            int footerLength = footerLenBuf.getInt();

            long footerStart = fileSize - 4 - footerLength;
            ByteBuffer footer = ByteBuffer.allocate(footerLength);
            readAt(ch, footerStart, footer);
            footer.flip();

            long dataLength = footer.getLong();
            int indexLength = footer.getInt();
            int bloomLength = footer.getInt();
            int minKeyLen = footer.getInt();
            byte[] minKey = new byte[minKeyLen];
            footer.get(minKey);
            int maxKeyLen = footer.getInt();
            byte[] maxKey = new byte[maxKeyLen];
            footer.get(maxKey);
            int entryCount = footer.getInt();

            ByteBuffer indexBuf = ByteBuffer.allocate(indexLength);
            readAt(ch, dataLength, indexBuf);
            indexBuf.flip();
            SparseIndex sparseIndex = SparseIndex.fromBytes(indexBuf.array());

            ByteBuffer bloomBuf = ByteBuffer.allocate(bloomLength);
            readAt(ch, dataLength + indexLength, bloomBuf);
            bloomBuf.flip();
            BloomFilter bloom = BloomFilter.fromBytes(bloomBuf.array());

            return new SSTable(dataFile, bloom, sparseIndex, level, id, minKey, maxKey, entryCount);
        }
    }

    public static LookupResult get(SSTable table, byte[] key) throws IOException {
        if (!table.mayContainRange(key)) return LookupResult.NOT_FOUND;
        if (!table.bloomFilter().mightContain(key)) return LookupResult.NOT_FOUND;

        long startOffset = table.sparseIndex().floorOffset(key);

        try (FileChannel ch = FileChannel.open(table.dataFile(), StandardOpenOption.READ)) {
            long position = startOffset;
            long dataEnd = dataSectionEnd(ch);

            while (position < dataEnd) {
                ByteBuffer header = ByteBuffer.allocate(8);
                readAt(ch, position, header);
                header.flip();
                int keyLen = header.getInt();
                int valueLen = header.getInt();
                position += 8;

                ByteBuffer keyBuf = ByteBuffer.allocate(keyLen);
                readAt(ch, position, keyBuf);
                keyBuf.flip();
                byte[] candidateKey = keyBuf.array();
                position += keyLen;

                int cmp = java.util.Arrays.compareUnsigned(candidateKey, key);
                if (cmp == 0) {
                    if (valueLen == -1) return LookupResult.tombstone();
                    ByteBuffer valueBuf = ByteBuffer.allocate(valueLen);
                    readAt(ch, position, valueBuf);
                    valueBuf.flip();
                    return LookupResult.value(valueBuf.array());
                } else if (cmp > 0) {
                    // Sorted data section, and we've already passed the target key
                    // alphabetically -> it does not exist in this table.
                    return LookupResult.NOT_FOUND;
                }

                if (valueLen > 0) position += valueLen;
            }
        }
        return LookupResult.NOT_FOUND;
    }

    private static long dataSectionEnd(FileChannel ch) throws IOException {
        long fileSize = ch.size();
        ByteBuffer footerLenBuf = ByteBuffer.allocate(4);
        readAt(ch, fileSize - 4, footerLenBuf);
        footerLenBuf.flip();
        int footerLength = footerLenBuf.getInt();
        long footerStart = fileSize - 4 - footerLength;

        ByteBuffer footer = ByteBuffer.allocate(8);
        readAt(ch, footerStart, footer);
        footer.flip();
        return footer.getLong(); // dataLength is the first field of the footer
    }

    /** One record read back from a data section scan. value == null means a tombstone. */
    public record RawEntry(byte[] key, byte[] value) {}

    /**
     * Reads every record in a table's data section, in key order, for use
     * by compaction's merge step. Unlike get(), this doesn't touch the
     * bloom filter or sparse index at all — a compaction has to visit
     * every record anyway, so there's nothing to skip.
     *
     * Opens its own FileChannel and reads everything into memory as a
     * List rather than streaming lazily. For the table sizes this engine
     * actually produces (bounded by memTableMaxBytes per flush, and by
     * how aggressively compaction consolidates) that's a reasonable
     * trade — a true streaming iterator that holds the channel open across
     * calls would be the right move once SSTables get large enough that
     * "every table fits in memory at once during compaction" stops being true.
     */
    public static List<RawEntry> scanAll(SSTable table) throws IOException {
        List<RawEntry> entries = new java.util.ArrayList<>(Math.max(table.entryCount(), 16));
        try (FileChannel ch = FileChannel.open(table.dataFile(), StandardOpenOption.READ)) {
            long dataEnd = dataSectionEnd(ch);
            long position = 0;
            while (position < dataEnd) {
                ByteBuffer header = ByteBuffer.allocate(8);
                readAt(ch, position, header);
                header.flip();
                int keyLen = header.getInt();
                int valueLen = header.getInt();
                position += 8;

                ByteBuffer keyBuf = ByteBuffer.allocate(keyLen);
                readAt(ch, position, keyBuf);
                keyBuf.flip();
                byte[] key = keyBuf.array();
                position += keyLen;

                byte[] value = null;
                if (valueLen >= 0) {
                    ByteBuffer valueBuf = ByteBuffer.allocate(valueLen);
                    readAt(ch, position, valueBuf);
                    valueBuf.flip();
                    value = valueBuf.array();
                    position += valueLen;
                }
                entries.add(new RawEntry(key, value));
            }
        }
        return entries;
    }

    private static void readAt(FileChannel ch, long position, ByteBuffer buf) throws IOException {
        int total = 0;
        int target = buf.remaining();
        while (total < target) {
            int n = ch.read(buf, position + total);
            if (n < 0) throw new IOException("unexpected EOF reading " + ch + " at " + (position + total));
            total += n;
        }
    }
}
