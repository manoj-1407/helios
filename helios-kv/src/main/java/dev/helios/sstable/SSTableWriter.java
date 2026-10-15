package dev.helios.sstable;

import dev.helios.bloom.BloomFilter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.NavigableMap;
import java.util.zip.CRC32;

/**
 * Flushes a sorted MemTable snapshot to a single .sst file on disk.
 *
 * File layout:
 *   [data section]   sorted records: keyLen(4) valueLen(4, -1=tombstone) checksum(4) key value
 *   [index section]  SparseIndex.toBytes()
 *   [bloom section]  BloomFilter.toBytes()
 *   [footer]         self-describing trailer with format version
 *   [footerLen: int32]   <- last 4 bytes of the file
 *
 * The checksum field (CRC32 over key bytes || value bytes) is placed in the
 * header, before the key/value payload, so a reader can verify integrity as
 * it scans without needing to read the full record first. A tombstone record
 * checksums only the key (value is absent). Any mismatch is treated as silent
 * disk corruption and raises IOException immediately — unlike a WAL truncated
 * record (which is an expected crash shape), a bad checksum in a compacted
 * SSTable is never a normal recovery scenario.
 *
 * Format version 2 adds the per-record checksum field. The footer carries the
 * version so SSTableReader can handle both v1 (legacy, no checksum) and v2
 * files during a rolling upgrade. Once all files are compacted through v2,
 * v1 support can be dropped.
 */
public final class SSTableWriter {

    /** Sample one key into the sparse index every BLOCK_INTERVAL records. */
    private static final int BLOCK_INTERVAL = 128;
    private static final double BLOOM_FALSE_POSITIVE_RATE = 0.01;

    /** Record header size: keyLen(4) + valueLen(4) + checksum(4). */
    static final int RECORD_HEADER = 12;

    /** Format version written into the footer. */
    static final int FORMAT_VERSION = 2;

    private final Path dir;

    public SSTableWriter(Path dir) throws IOException {
        this.dir = dir;
        Files.createDirectories(dir);
    }

    public SSTable flush(NavigableMap<byte[], byte[]> snapshot, int level) throws IOException {
        if (snapshot.isEmpty()) {
            throw new IllegalArgumentException("refusing to flush an empty MemTable");
        }

        long id = System.nanoTime();
        Path dataFile = dir.resolve(level + "_" + id + ".sst");

        BloomFilter bloom = new BloomFilter(snapshot.size(), BLOOM_FALSE_POSITIVE_RATE);
        SparseIndex sparseIndex = new SparseIndex();
        byte[] minKey = null, maxKey = null;
        int count = 0;
        long dataLength;

        try (FileChannel dc = FileChannel.open(dataFile,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {

            long offset = 0;
            for (Map.Entry<byte[], byte[]> e : snapshot.entrySet()) {
                byte[] key = e.getKey();
                byte[] value = e.getValue(); // null == tombstone

                if (minKey == null) minKey = key;
                maxKey = key;

                bloom.add(key);
                if (count % BLOCK_INTERVAL == 0) {
                    sparseIndex.add(key, offset);
                }

                int valueLen = (value == null) ? -1 : value.length;
                int checksum = recordChecksum(key, value);
                int payloadLen = key.length + (value == null ? 0 : value.length);

                ByteBuffer record = ByteBuffer.allocate(RECORD_HEADER + payloadLen);
                record.putInt(key.length);
                record.putInt(valueLen);
                record.putInt(checksum);
                record.put(key);
                if (value != null) record.put(value);
                record.flip();

                offset += dc.write(record);
                count++;
            }
            dataLength = offset;

            byte[] indexBytes = sparseIndex.toBytes();
            dc.write(ByteBuffer.wrap(indexBytes));

            byte[] bloomBytes = bloom.toBytes();
            dc.write(ByteBuffer.wrap(bloomBytes));

            writeFooter(dc, dataLength, indexBytes.length, bloomBytes.length, minKey, maxKey, count);
        }

        return new SSTable(dataFile, bloom, sparseIndex, level, id, minKey, maxKey, count);
    }

    private void writeFooter(FileChannel dc, long dataLength, int indexLength, int bloomLength,
                              byte[] minKey, byte[] maxKey, int entryCount) throws IOException {
        ByteBuffer footer = ByteBuffer.allocate(
                8 + 4 + 4          // dataLength, indexLength, bloomLength
                        + 4 + minKey.length  // minKeyLen + minKey
                        + 4 + maxKey.length  // maxKeyLen + maxKey
                        + 4                  // entryCount
                        + 4                  // formatVersion (v2)
        );
        footer.putLong(dataLength);
        footer.putInt(indexLength);
        footer.putInt(bloomLength);
        footer.putInt(minKey.length);
        footer.put(minKey);
        footer.putInt(maxKey.length);
        footer.put(maxKey);
        footer.putInt(entryCount);
        footer.putInt(FORMAT_VERSION);
        footer.flip();

        int footerLength = footer.remaining();
        dc.write(footer);
        dc.write(ByteBuffer.allocate(4).putInt(footerLength).flip());
    }

    /** CRC32 over key bytes then value bytes (or just key bytes for a tombstone). */
    static int recordChecksum(byte[] key, byte[] value) {
        CRC32 crc = new CRC32();
        crc.update(key);
        if (value != null) crc.update(value);
        return (int) crc.getValue();
    }
}
