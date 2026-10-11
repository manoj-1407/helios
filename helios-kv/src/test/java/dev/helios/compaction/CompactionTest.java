package dev.helios.compaction;

import dev.helios.core.HeliosDB;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

import static dev.helios.TestSupport.*;

public final class CompactionTest {

    public static void main(String[] args) throws Exception {
        testForcedLeveledReducesTableCountAndStaysCorrect();
        testTombstoneReclaimDuringTieredCompaction();
        testFifoEvictsOldestOnceOverBudget();
        testAdaptiveSwitchingActuallyChangesStrategy();
        System.out.println("CompactionTest: all assertions passed");
    }

    private static void testForcedLeveledReducesTableCountAndStaysCorrect() throws Exception {
        Path dir = Path.of("/tmp/helios-compaction-test-leveled");
        deleteRecursive(dir);
        HeliosDB db = new HeliosDB(dir, 32 * 1024, CompactionManager.Mode.FORCE_LEVELED);

        int n = 15_000;
        for (int i = 0; i < n; i++) db.put(("lk" + i).getBytes(), ("lv" + i).getBytes());
        Thread.sleep(3500); // let flush + a couple of 2s compaction ticks settle

        for (int i = 0; i < n; i++) {
            assertArrayEquals(("lv" + i).getBytes(), db.get(("lk" + i).getBytes()), "key " + i + " must survive leveled compaction");
        }
        assertTrue(db.sstableCount() < 10, "leveled compaction should have consolidated well below the raw flush count, got " + db.sstableCount());
        assertEquals("leveled", db.activeCompactionStrategy(), "forced mode must not switch strategies");
        db.close();
    }

    private static void testTombstoneReclaimDuringTieredCompaction() throws Exception {
        Path dir = Path.of("/tmp/helios-compaction-test-tombstone");
        deleteRecursive(dir);
        HeliosDB db = new HeliosDB(dir, 16 * 1024, CompactionManager.Mode.FORCE_TIERED);

        for (int i = 0; i < 2000; i++) db.put(("tk" + i).getBytes(), ("tv" + i).getBytes());
        db.delete("tk500".getBytes());
        for (int i = 2000; i < 8000; i++) db.put(("tk" + i).getBytes(), ("tv" + i).getBytes()); // force more flushes + a tiered pass
        Thread.sleep(3500);

        assertTrue(!db.exists("tk500".getBytes()), "deleted key must stay deleted after a full-pool tiered compaction (tombstone correctly reclaimed, not resurrected)");
        assertNull(db.get("tk500".getBytes()), "get() on the reclaimed tombstone must return null");
        db.close();
    }

    private static void testFifoEvictsOldestOnceOverBudget() throws Exception {
        Path dir = Path.of("/tmp/helios-compaction-test-fifo");
        deleteRecursive(dir);
        // fifoBudgetBytes inside HeliosDB defaults to memTableMaxBytes * 8 -- an
        // 8KB memtable gives a 64KB total budget, small enough to force real evictions here.
        HeliosDB db = new HeliosDB(dir, 8 * 1024, CompactionManager.Mode.FORCE_FIFO);

        int n = 6000;
        for (int i = 0; i < n; i++) db.put(("fk" + i).getBytes(), ("fv" + i).getBytes());
        Thread.sleep(3000);

        for (int i = n - 200; i < n; i++) {
            assertArrayEquals(("fv" + i).getBytes(), db.get(("fk" + i).getBytes()), "recently written key " + i + " must survive FIFO eviction");
        }
        boolean anyOldEvicted = false;
        for (int i = 0; i < 200; i++) {
            if (!db.exists(("fk" + i).getBytes())) { anyOldEvicted = true; break; }
        }
        assertTrue(anyOldEvicted, "FIFO is supposed to evict the oldest segments once over budget -- none were evicted");
        db.close();
    }

    private static void testAdaptiveSwitchingActuallyChangesStrategy() throws Exception {
        Path dir = Path.of("/tmp/helios-compaction-test-adaptive");
        deleteRecursive(dir);
        HeliosDB db = new HeliosDB(dir, 16 * 1024, CompactionManager.Mode.ADAPTIVE);

        for (int round = 0; round < 6; round++) {
            for (int i = 0; i < 500; i++) db.put(("ak" + (round * 500 + i)).getBytes(), ("av" + i).getBytes());
            Thread.sleep(2100); // straddle at least one classifier tick per round
        }
        String afterWrites = db.activeCompactionStrategy();

        for (int round = 0; round < 6; round++) {
            for (int i = 0; i < 2000; i++) db.get(("ak" + (i % 3000)).getBytes());
            Thread.sleep(2100);
        }
        String afterReads = db.activeCompactionStrategy();

        assertEquals("tiered", afterWrites, "a sustained write-heavy burst must drive the classifier to select tiered compaction");
        assertEquals("leveled", afterReads, "a sustained read-heavy burst afterward must drive the classifier to switch to leveled compaction");
        db.close();
    }

    private static void deleteRecursive(Path p) throws Exception {
        if (!Files.exists(p)) return;
        Files.walk(p).sorted(Comparator.reverseOrder()).forEach(f -> f.toFile().delete());
    }
}
