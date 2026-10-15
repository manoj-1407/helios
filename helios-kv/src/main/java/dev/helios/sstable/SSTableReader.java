package dev.helios.sstable;

import dev.helios.bloom.BloomFilter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

/**
 * Reconstructs an SSTable handle from disk and performs point lookups.
 *
 * Lookup path: bloom filter (no I/O) → key range check (no I/O) →
 * sparse index binary search → sequential scan of data section.
 *
 * Checksums: format version 2 files carry a CRC32 per record in the header.
 * Any mismatch raises IOException immediately — a bad checksum in a compacted
 * SSTable is silent disk corruption, not a normal crash-recovery shape.
 * Format version 1 files (written before the checksum change) are read without
 * verification; they'll naturally disappear as compaction rewrites them as v2.
 */
public final class SSTableReader {

    private SSTableReader() {}

    public record LookupResult(boolean found, byte[] value) {
        public static final LookupResult NOT_FOUND = new LookupResult(false, null);
        public static LookupResult tombstone() { return new LookupResult(true, null); }
        public static LookupResult value(byte[] v) { return new LookupResult(true, v); }
    }

    /** Rebuilds an SSTable handle by reading the footer of an existing .sst file. */
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

            long dataLength    = footer.getLong();
            int  indexLength   = footer.getInt();
            int  bloomLength   = footer.getInt();
            int  minKeyLen     = footer.getInt();
            byte[] minKey      = new byte[minKeyLen]; footer.get(minKey);
            int  maxKeyLen     = footer.getInt();
            byte[] maxKey      = new byte[maxKeyLen]; footer.get(maxKey);
            int  entryCount    = footer.getInt();
            // formatVersion is present only in v2 footers; v1 footers end at entryCount.
            int  formatVersion = footer.hasRemaining() ? footer.getInt() : 1;

            ByteBuffer indexBuf = ByteBuffer.allocate(indexLength);
            readAt(ch, dataLength, indexBuf);
            indexBuf.flip();
            SparseIndex sparseIndex = SparseIndex.fromBytes(indexBuf.array());

            ByteBuffer bloomBuf = ByteBuffer.allocate(bloomLength);
            readAt(ch, dataLength + indexLength, bloomBuf);
            bloomBuf.flip();
            BloomFilter bloom = BloomFilter.fromBytes(bloomBuf.array());

            SSTable table = new SSTable(dataFile, bloom, sparseIndex, level, id, minKey, maxKey, entryCount);
            table.setFormatVersion(formatVersion);
            return table;
        }
    }

    public static LookupResult get(SSTable table, byte[] key) throws IOException {
        if (!table.mayContainRange(key)) return LookupResult.NOT_FOUND;
        if (!table.bloomFilter().mightContain(key)) return LookupResult.NOT_FOUND;

        long startOffset = table.sparseIndex().floorOffset(key);
        boolean verify   = table.formatVersion() >= 2;

        try (FileChannel ch = FileChannel.open(table.dataFile(), StandardOpenOption.READ)) {
            long position = startOffset;
            long dataEnd  = dataSectionEnd(ch);

            while (position < dataEnd) {
                int headerSize = verify ? SSTableWriter.RECORD_HEADER : 8;
                ByteBuffer header = ByteBuffer.allocate(headerSize);
                readAt(ch, position, header);
                header.flip();

                int keyLen   = header.getInt();
                int valueLen = header.getInt();
                int storedChecksum = verify ? header.getInt() : 0;
                position += headerSize;

                byte[] candidateKey = new byte[keyLen];
                readAt(ch, position, ByteBuffer.wrap(candidateKey));
                position += keyLen;

                int cmp = java.util.Arrays.compareUnsigned(candidateKey, key);
                if (cmp > 0) return LookupResult.NOT_FOUND; // sorted, passed the target

                boolean tombstone = (valueLen == -1);
                byte[] value = null;
                if (!tombstone && valueLen >= 0) {
                    value = new byte[valueLen];
                    readAt(ch, position, ByteBuffer.wrap(value));
                    position += valueLen;
                }

                if (cmp == 0) {
                    if (verify) {
                        int got = SSTableWriter.recordChecksum(candidateKey, value);
                        if (got != storedChecksum) {
                            throw new IOException(
                                "SSTable checksum mismatch in " + table.dataFile() +
                                " at key=" + new String(candidateKey, java.nio.charset.StandardCharsets.UTF_8) +
                                " (stored=" + Integer.toHexString(storedChecksum) +
                                " computed=" + Integer.toHexString(got) + ")");
                        }
                    }
                    return tombstone ? LookupResult.tombstone() : LookupResult.value(value);
                }
                // cmp < 0: keep scanning forward
            }
        }
        return LookupResult.NOT_FOUND;
    }

    public record RawEntry(byte[] key, byte[] value) {}

    /**
     * Reads every record in a table's data section for compaction.
     * Verifies checksums on v2 tables; raises IOException on mismatch.
     */
    public static List<RawEntry> scanAll(SSTable table) throws IOException {
        boolean verify = table.formatVersion() >= 2;
        List<RawEntry> entries = new java.util.ArrayList<>(Math.max(table.entryCount(), 16));
        try (FileChannel ch = FileChannel.open(table.dataFile(), StandardOpenOption.READ)) {
            long dataEnd = dataSectionEnd(ch);
            long position = 0;
            while (position < dataEnd) {
                int headerSize = verify ? SSTableWriter.RECORD_HEADER : 8;
                ByteBuffer header = ByteBuffer.allocate(headerSize);
                readAt(ch, position, header);
                header.flip();

                int keyLen   = header.getInt();
                int valueLen = header.getInt();
                int storedChecksum = verify ? header.getInt() : 0;
                position += headerSize;

                byte[] key = new byte[keyLen];
                readAt(ch, position, ByteBuffer.wrap(key));
                position += keyLen;

                byte[] value = null;
                if (valueLen >= 0) {
                    value = new byte[valueLen];
                    readAt(ch, position, ByteBuffer.wrap(value));
                    position += valueLen;
                }

                if (verify) {
                    int got = SSTableWriter.recordChecksum(key, value);
                    if (got != storedChecksum) {
                        throw new IOException(
                            "SSTable checksum mismatch during compaction scan in " + table.dataFile() +
                            " at offset=" + (position - keyLen - (value == null ? 0 : value.length) - headerSize));
                    }
                }

                entries.add(new RawEntry(key, value));
            }
        }
        return entries;
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
