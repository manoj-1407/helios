package dev.helios.core;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

import static dev.helios.TestSupport.*;

public final class HeliosDBTest {

    public static void main(String[] args) throws Exception {
        testMultiFlushDeletesAndOverwritesAcrossGenerations();
        testCleanCloseAndReopenRecoversFromSSTables();
        System.out.println("HeliosDBTest: all assertions passed");
    }

    private static void testMultiFlushDeletesAndOverwritesAcrossGenerations() throws Exception {
        Path dir = Path.of("/tmp/helios-test-multiflush");
        deleteRecursive(dir);

        // Tiny threshold so 20k entries forces several real SSTable flushes --
        // this is what actually exercises "newest generation wins" rather than
        // everything just sitting in one MemTable the whole test.
        HeliosDB db = new HeliosDB(dir, 64 * 1024);
        int n = 20_000;
        for (int i = 0; i < n; i++) {
            db.put(("k" + i).getBytes(), ("v" + i).getBytes());
        }
        assertTrue(db.sstableCount() >= 2, "20k puts at a 64KB threshold should have triggered multiple flushes, got " + db.sstableCount());

        for (int i = 100; i < 200; i++) {
            db.delete(("k" + i).getBytes());
        }
        for (int i = 5000; i < 5010; i++) {
            db.put(("k" + i).getBytes(), ("UPDATED-" + i).getBytes());
        }
        Thread.sleep(500); // let background flushes drain

        for (int i = 0; i < n; i++) {
            if (i >= 100 && i < 200) {
                assertTrue(!db.exists(("k" + i).getBytes()), "key " + i + " was deleted and must not exist, even across flush generations");
            } else if (i >= 5000 && i < 5010) {
                assertArrayEquals(("UPDATED-" + i).getBytes(), db.get(("k" + i).getBytes()),
                        "key " + i + " was overwritten in a newer generation -- newest must win");
            } else {
                assertArrayEquals(("v" + i).getBytes(), db.get(("k" + i).getBytes()), "key " + i + " untouched value");
            }
        }
        db.close();
    }

    private static void testCleanCloseAndReopenRecoversFromSSTables() throws Exception {
        Path dir = Path.of("/tmp/helios-test-clean-reopen");
        deleteRecursive(dir);

        HeliosDB db = new HeliosDB(dir, 64 * 1024);
        for (int i = 0; i < 5000; i++) db.put(("ck" + i).getBytes(), ("cv" + i).getBytes());
        db.delete("ck10".getBytes());
        db.close(); // no crash this time -- exercises recovering from SSTables on disk with an empty WAL, not WAL replay

        HeliosDB reopened = new HeliosDB(dir, 64 * 1024);
        assertArrayEquals("cv4000".getBytes(), reopened.get("ck4000".getBytes()), "value must survive a clean close + reopen");
        assertTrue(!reopened.exists("ck10".getBytes()), "tombstone must survive a clean close + reopen");
        reopened.close();
    }

    private static void deleteRecursive(Path p) throws Exception {
        if (!Files.exists(p)) return;
        Files.walk(p).sorted(Comparator.reverseOrder()).forEach(f -> f.toFile().delete());
    }
}
