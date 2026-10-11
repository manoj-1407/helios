package dev.helios.core;

import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

import static dev.helios.TestSupport.*;

public final class WALTest {

    public static void main(String[] args) throws Exception {
        testCleanRecovery();
        testTornTrailingRecordStopsCleanlyInsteadOfThrowing();
        System.out.println("WALTest: all assertions passed");
    }

    private static void testCleanRecovery() throws Exception {
        Path tmp = Files.createTempFile("helios-wal-test-clean", ".log");
        try {
            WAL wal = new WAL(tmp);
            for (int i = 0; i < 10; i++) {
                wal.append(("key" + i).getBytes(), ("value" + i).getBytes());
            }
            wal.append("deletedKey".getBytes(), null); // tombstone record
            wal.close();

            List<WAL.Record> records = WAL.recover(tmp);
            assertEquals(11, records.size(), "clean recovery should return every appended record");
            assertNull(records.get(10).value(), "the tombstone record must recover with a null value");
        } finally {
            Files.deleteIfExists(tmp);
        }
    }

    private static void testTornTrailingRecordStopsCleanlyInsteadOfThrowing() throws Exception {
        Path tmp = Files.createTempFile("helios-wal-test-crash", ".log");
        try {
            WAL wal = new WAL(tmp);
            for (int i = 0; i < 10; i++) {
                wal.append(("key" + i).getBytes(), ("value" + i).getBytes());
            }
            wal.append("deletedKey".getBytes(), null);
            wal.close();

            long fullSize = Files.size(tmp);
            try (FileChannel ch = FileChannel.open(tmp, StandardOpenOption.WRITE)) {
                ch.truncate(fullSize - 5); // simulate a crash mid-write on the last record
            }

            List<WAL.Record> afterCrash = WAL.recover(tmp);
            assertEquals(10, afterCrash.size(),
                    "recovery must return every complete record and silently drop only the torn trailing one");
        } finally {
            Files.deleteIfExists(tmp);
        }
    }
}
