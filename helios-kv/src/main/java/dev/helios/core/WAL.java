package dev.helios.core;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;

/**
 * Write-ahead log. Every put/delete is appended here, fsync'd, before the
 * MemTable is touched — that ordering is the entire durability guarantee
 * of the engine, so it isn't optional and there's no "fast path" that
 * skips it.
 *
 * Record format on disk, back to back:
 *   [keyLen: int32][valueLen: int32][checksum: int32][key bytes][value bytes]
 * valueLen == -1 marks a tombstone (delete) and has no value bytes following.
 *
 * Durability: opened with StandardOpenOption.DSYNC rather than calling
 * FileChannel.force() after every write. DSYNC pushes the sync cost onto
 * the OS write() call itself, which on most platforms is cheaper than a
 * separate fsync syscall per record, and removes a whole class of bugs
 * where someone adds a write path that forgets to call force(). The
 * trade-off is it's slightly less portable (some platforms only honor
 * DSYNC loosely) — acceptable here since this isn't shipping to embedded
 * or exotic filesystems.
 *
 * Recovery deliberately treats a corrupt or truncated *trailing* record as
 * "this is where the crash happened, stop here" rather than an error. A
 * half-written record at the tail of the WAL is the expected shape of an
 * interrupted write, not a bug — throwing there would turn every ungraceful
 * shutdown into a permanently unrecoverable database, which defeats the
 * purpose of having a WAL at all.
 */
public final class WAL {

    private static final int HEADER_SIZE = 12; // keyLen + valueLen + checksum
    private static final int TOMBSTONE = -1;

    private final FileChannel channel;
    private final Path path;

    public WAL(Path walPath) throws IOException {
        this.path = walPath;
        this.channel = FileChannel.open(walPath,
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND,
                StandardOpenOption.DSYNC);
    }

    /** value == null is written as a tombstone. */
    public synchronized void append(byte[] key, byte[] value) throws IOException {
        boolean tombstone = (value == null);
        int valueLen = tombstone ? TOMBSTONE : value.length;

        ByteBuffer header = ByteBuffer.allocate(HEADER_SIZE);
        header.putInt(key.length);
        header.putInt(valueLen);
        header.putInt(checksum(key, tombstone ? null : value));
        header.flip();

        ByteBuffer body = ByteBuffer.allocate(key.length + (tombstone ? 0 : value.length));
        body.put(key);
        if (!tombstone) body.put(value);
        body.flip();

        // One vectored write so a concurrent reader (there shouldn't be one,
        // but belt-and-suspenders) never sees a header without its body.
        channel.write(new ByteBuffer[]{header, body});
    }

    public void close() throws IOException {
        channel.close();
    }

    public Path path() {
        return path;
    }

    /** A single recovered record: value == null means this record was a delete. */
    public record Record(byte[] key, byte[] value) {}

    /**
     * Replays a WAL file from scratch (this is a static method, not an
     * instance method, because recovery happens on startup before we've
     * decided whether to even keep writing to this WAL file or roll a new one).
     */
    public static List<Record> recover(Path walPath) throws IOException {
        List<Record> records = new ArrayList<>();
        if (!java.nio.file.Files.exists(walPath)) return records;

        try (FileChannel rc = FileChannel.open(walPath, StandardOpenOption.READ)) {
            long fileSize = rc.size();
            ByteBuffer header = ByteBuffer.allocate(HEADER_SIZE);

            while (rc.position() + HEADER_SIZE <= fileSize) {
                header.clear();
                int read = readFully(rc, header);
                if (read < HEADER_SIZE) break; // torn header at EOF -> stop, don't fail

                header.flip();
                int keyLen = header.getInt();
                int valueLen = header.getInt();
                int storedChecksum = header.getInt();

                if (keyLen < 0 || keyLen > 64 * 1024 * 1024) break; // garbage header, treat as end
                boolean tombstone = (valueLen == TOMBSTONE);
                int bodyLen = keyLen + (tombstone ? 0 : Math.max(valueLen, 0));

                if (rc.position() + bodyLen > fileSize) break; // truncated body -> crash happened here

                ByteBuffer body = ByteBuffer.allocate(bodyLen);
                int bodyRead = readFully(rc, body);
                if (bodyRead < bodyLen) break;
                body.flip();

                byte[] key = new byte[keyLen];
                body.get(key);
                byte[] value = null;
                if (!tombstone) {
                    value = new byte[valueLen];
                    body.get(value);
                }

                if (checksum(key, value) != storedChecksum) {
                    // Corrupt record, not just truncated (lengths looked sane but
                    // the bytes don't hash to what was claimed). Same treatment:
                    // stop replaying, keep everything recovered up to this point.
                    break;
                }

                records.add(new Record(key, value));
            }
        }
        return records;
    }

    private static int readFully(FileChannel ch, ByteBuffer buf) throws IOException {
        int total = 0;
        while (buf.hasRemaining()) {
            int n = ch.read(buf);
            if (n < 0) break;
            total += n;
        }
        return total;
    }

    private static int checksum(byte[] key, byte[] value) {
        CRC32 crc = new CRC32();
        crc.update(key);
        if (value != null) crc.update(value);
        return (int) crc.getValue();
    }
}
