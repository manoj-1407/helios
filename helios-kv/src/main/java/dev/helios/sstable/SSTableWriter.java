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

/**
 * Flushes a sorted MemTable snapshot to a single .sst file on disk.
 *
 * Why one file instead of three (.sst/.idx/.bloom as separate files): a
 * compaction or a crash that deletes/renames files non-atomically is a much
 * scarier failure mode when "this SSTable" is actually three files that can
 * independently go missing or get out of sync. A single file with a footer
 * means an SSTable either exists, fully, or it doesn't — there's no
 * intermediate state where the data file is there but the bloom filter
 * isn't.
 *
 * File layout:
 *   [data section]   sorted records: keyLen(4) valueLen(4, -1=tombstone) key value
 *   [index section]  SparseIndex.toBytes()
 *   [bloom section]  BloomFilter.toBytes()
 *   [footer]         self-describing trailer, see writeFooter()
 *   [footerLen: int32]   <- last 4 bytes of the file, so the reader can
 *                            always find the footer by seeking from the end
 *                            without needing to know anything else first
 */
public final class SSTableWriter {

    /** Sample one key into the sparse index every BLOCK_INTERVAL records. */
    private static final int BLOCK_INTERVAL = 128;
    private static final double BLOOM_FALSE_POSITIVE_RATE = 0.01;

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
                byte[] value = e.getValue(); // null == tombstone, written through as-is

                if (minKey == null) minKey = key;
                maxKey = key;

                bloom.add(key);
                if (count % BLOCK_INTERVAL == 0) {
                    sparseIndex.add(key, offset);
                }

                int valueLen = (value == null) ? -1 : value.length;
                ByteBuffer record = ByteBuffer.allocate(8 + key.length + (value == null ? 0 : value.length));
                record.putInt(key.length);
                record.putInt(valueLen);
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
        );
        footer.putLong(dataLength);
        footer.putInt(indexLength);
        footer.putInt(bloomLength);
        footer.putInt(minKey.length);
        footer.put(minKey);
        footer.putInt(maxKey.length);
        footer.put(maxKey);
        footer.putInt(entryCount);
        footer.flip();

        int footerLength = footer.remaining();
        dc.write(footer);
        dc.write(ByteBuffer.allocate(4).putInt(footerLength).flip());
    }
}
