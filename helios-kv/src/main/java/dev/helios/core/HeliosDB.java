package dev.helios.core;

import dev.helios.classifier.WorkloadClassifier;
import dev.helios.compaction.CompactionManager;
import dev.helios.sstable.SSTable;
import dev.helios.sstable.SSTableReader;
import dev.helios.sstable.SSTableWriter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Entry point. Wires together WAL -> MemTable -> SSTableWriter, recovers
 * existing state on construction, flushes in the background so writes
 * never stall behind disk I/O, and — this is the actual thesis of the
 * project — runs a WorkloadClassifier + CompactionManager that watches
 * recent traffic shape and hot-swaps between leveled, tiered, and forced
 * compaction strategies live, with the engine under load.
 *
 * Write path:   put() -> WAL.append() (durable) -> active MemTable.put()
 *                      -> classifier.recordWrite()
 *                      -> if full, swap in a new active MemTable and hand
 *                         the old one to a background thread to flush
 * Read path:    get() -> classifier.recordRead()
 *                      -> active MemTable -> in-flight flushing MemTables
 *                      -> SSTables, newest first (bloom filter short-
 *                         circuits the ones that can't possibly have it)
 * Background:   CompactionManager ticks on a timer, asks the classifier
 *               what the workload looks like, and merges/evicts SSTables
 *               using whichever strategy that classification maps to.
 *
 * The MemTable swap is the one part of the write path that has to be
 * synchronized: "decide the table is full, swap it out, open a new WAL"
 * has to happen as one atomic step, or two writer threads could both
 * decide to flush at the same time and end up with two background flushes
 * racing to write SSTables for overlapping data. Everything else
 * (ConcurrentSkipListMap underneath MemTable, CopyOnWriteArrayList
 * underneath the SSTable list) is already lock-free.
 *
 * One known, accepted race: compaction publishes its new SSTable(s) and
 * retires the old references from the live list before deleting the old
 * files (see CompactionManager's class doc for why that ordering is
 * safe for *visibility*). It does NOT fully close the gap for a get()
 * that grabbed a reference to an old SSTable a moment before its file
 * gets deleted — that read can hit a vanished file. Rather than crash
 * the whole get() call over it, every per-table lookup below treats a
 * missing-file error as "this table is gone, move on to the next one"
 * — correct, because by definition a table can only disappear once a
 * newer compacted replacement covering the same keys already exists.
 * A real fix would be reference-counting open SSTables (don't delete
 * until every in-flight reader using one has finished); not implemented
 * here and called out as a known gap rather than silently worked around.
 */
public final class HeliosDB implements AutoCloseable {

    private final Path dataDir;
    private final Path walDir;
    private final long memTableMaxBytes;
    private final SSTableWriter sstableWriter;
    private final ExecutorService flushExecutor;

    private final Object flushLock = new Object();
    private volatile MemTable active;
    private volatile WAL wal;
    private final CopyOnWriteArrayList<MemTable> flushing = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<SSTable> sstables;

    private final WorkloadClassifier classifier;
    private final CompactionManager compactionManager;

    private final AtomicLong logicalBytesWritten = new AtomicLong();
    private final AtomicLong physicalBytesFlushed = new AtomicLong();

    public HeliosDB(Path dataDir) throws IOException {
        this(dataDir, 4 * 1024 * 1024, CompactionManager.Mode.ADAPTIVE);
    }

    public HeliosDB(Path dataDir, long memTableMaxBytes) throws IOException {
        this(dataDir, memTableMaxBytes, CompactionManager.Mode.ADAPTIVE);
    }

    /**
     * @param mode lets a benchmark or test force a single strategy (or
     *             disable compaction entirely) instead of letting the
     *             classifier decide — this is what makes "adaptive vs.
     *             static" an actual A/B comparison rather than a claim.
     */
    public HeliosDB(Path dataDir, long memTableMaxBytes, CompactionManager.Mode mode) throws IOException {
        this.dataDir = dataDir;
        this.walDir = dataDir.resolve("wal");
        this.memTableMaxBytes = memTableMaxBytes;
        this.sstableWriter = new SSTableWriter(dataDir.resolve("sstables"));
        this.flushExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "helios-flush");
            t.setDaemon(true);
            return t;
        });

        RecoveryManager.RecoveredState recovered = RecoveryManager.recover(dataDir, memTableMaxBytes);
        this.sstables = new CopyOnWriteArrayList<>(recovered.sstables());

        if (!recovered.memTable().isEmpty()) {
            // Don't leave recovered data sitting only in a MemTable that was
            // rebuilt from a WAL we're about to delete — flush it to a durable
            // SSTable synchronously, on this thread, before accepting any new
            // writes. This is the one flush in the engine's life that is NOT
            // backgrounded, on purpose: until it completes, the data only
            // exists in memory.
            SSTable recoveredFlush = sstableWriter.flush(recovered.memTable().snapshot(), 0);
            sstables.add(recoveredFlush);
        }
        for (Path oldWal : recovered.walFilesToCleanUp()) {
            Files.deleteIfExists(oldWal);
        }

        this.active = new MemTable(memTableMaxBytes);
        this.wal = new WAL(walDir.resolve(System.nanoTime() + ".log"));

        this.classifier = new WorkloadClassifier(5); // majority vote over the last 5 ticks
        long fifoBudgetBytes = Math.max(memTableMaxBytes * 8, 1); // arbitrary but proportional default for forced-FIFO benchmarking
        this.compactionManager = new CompactionManager(
                classifier, sstables, sstableWriter, fifoBudgetBytes, mode, 2000L);
        this.compactionManager.start();
    }

    public void put(byte[] key, byte[] value) throws IOException {
        writeInternal(key, value);
    }

    public void delete(byte[] key) throws IOException {
        writeInternal(key, null);
    }

    private void writeInternal(byte[] key, byte[] value) throws IOException {
        // WAL first, always. If we crash between these two lines, the WAL
        // has the record and recovery picks it up; if we crashed before
        // this line, the write never happened — both are fine. The one
        // ordering that would NOT be fine is MemTable-then-WAL, since a
        // crash in between would silently lose an acknowledged write.
        wal.append(key, value);
        active.put(key, value);
        classifier.recordWrite();
        logicalBytesWritten.addAndGet(key.length + (value == null ? 0 : value.length));

        if (active.shouldFlush()) {
            triggerFlush();
        }
    }

    public byte[] get(byte[] key) throws IOException {
        classifier.recordRead();

        MemTable currentActive = active;
        if (currentActive.containsKey(key)) {
            return currentActive.get(key); // null here legitimately means tombstone
        }

        // Newest-first: a MemTable that's mid-flush is newer than every
        // SSTable on disk, so it has to be checked before any of them.
        // toArray() takes a fixed-size snapshot -- iterating via size()+get(i)
        // instead is NOT safe here: flushing is a CopyOnWriteArrayList that
        // the flush-completion callback shrinks concurrently, and a get(i)
        // against an index computed from a now-stale size() throws
        // ArrayIndexOutOfBoundsException. This was a real bug, not a
        // hypothetical one -- it surfaced under concurrent benchmark load.
        for (MemTable mt : flushingSnapshot()) {
            if (mt.containsKey(key)) return mt.get(key);
        }

        for (SSTable table : newestFirstSnapshot()) {
            SSTableReader.LookupResult result = lookupTolerant(table, key);
            if (result != null && result.found()) {
                return result.value(); // null here means tombstone
            }
        }
        return null; // genuinely never written, or written-and-already-compacted-away
    }

    /** Distinguishes "key was deleted" from "key was never written" — get() alone can't, since both return null. */
    public boolean exists(byte[] key) throws IOException {
        classifier.recordRead();

        MemTable currentActive = active;
        if (currentActive.containsKey(key)) return currentActive.get(key) != null;
        for (MemTable mt : flushingSnapshot()) {
            if (mt.containsKey(key)) return mt.get(key) != null;
        }
        for (SSTable table : newestFirstSnapshot()) {
            SSTableReader.LookupResult result = lookupTolerant(table, key);
            if (result != null && result.found()) return result.value() != null;
        }
        return false;
    }

    private List<SSTable> newestFirstSnapshot() {
        List<SSTable> snapshot = new java.util.ArrayList<>(sstables);
        snapshot.sort(Comparator.comparingLong(SSTable::id).reversed());
        return snapshot;
    }

    /** Fixed-size snapshot of in-flight flushing MemTables, newest first. See get()'s comment for why this can't be a size()+indexed-get() loop. */
    private List<MemTable> flushingSnapshot() {
        MemTable[] arr = flushing.toArray(new MemTable[0]);
        List<MemTable> list = new java.util.ArrayList<>(arr.length);
        for (int i = arr.length - 1; i >= 0; i--) list.add(arr[i]);
        return list;
    }

    /** See class doc: a table whose file vanished mid-compaction is treated as a miss, not an error. */
    private SSTableReader.LookupResult lookupTolerant(SSTable table, byte[] key) throws IOException {
        try {
            return SSTableReader.get(table, key);
        } catch (NoSuchFileException e) {
            return null;
        }
    }

    private void triggerFlush() throws IOException {
        MemTable toFlush;
        WAL oldWal;
        synchronized (flushLock) {
            if (!active.shouldFlush()) return; // lost the race to another writer thread, nothing to do
            toFlush = active;
            oldWal = wal;
            active = new MemTable(memTableMaxBytes);
            wal = new WAL(walDir.resolve(System.nanoTime() + ".log"));
            flushing.add(toFlush);
        }

        flushExecutor.submit(() -> {
            try {
                SSTable flushed = sstableWriter.flush(toFlush.snapshot(), 0);
                physicalBytesFlushed.addAndGet(Files.size(flushed.dataFile()));
                sstables.add(flushed);
                flushing.remove(toFlush);
                oldWal.close();
                Files.deleteIfExists(oldWal.path());
            } catch (IOException e) {
                // A failed background flush is not survivable silently — the
                // data is still safe (it's in toFlush and in oldWal on disk),
                // but losing visibility into the failure would let writes pile
                // up in MemTables indefinitely. Surface it loudly.
                System.err.println("[helios-kv] FLUSH FAILED for WAL " + oldWal.path() + ": " + e);
                e.printStackTrace();
            }
        });
    }

    /**
     * Ratio of total bytes physically written to disk (every flush plus
     * every compaction rewrite) vs. logical bytes the caller asked us to
     * write. This is the actual headline metric of the project's thesis:
     * an adaptive strategy should produce a lower number here than a
     * mismatched static one, under the workload it wasn't designed for.
     */
    public double writeAmplification() {
        long logical = logicalBytesWritten.get();
        if (logical == 0) return 1.0;
        long physical = physicalBytesFlushed.get() + compactionManager.physicalBytesWrittenByCompaction();
        return (double) physical / logical;
    }

    public int sstableCount() {
        return sstables.size();
    }

    public String activeCompactionStrategy() {
        return compactionManager.activeStrategyName();
    }

    public List<String> compactionSwitchLog() {
        return compactionManager.switchLog();
    }

    public Path dataDir() {
        return dataDir;
    }

    /** Flushes whatever's left in memory, stops the compaction background thread, and shuts down cleanly. Call this — don't just let the process die — unless you're specifically testing crash recovery. */
    @Override
    public void close() throws IOException {
        compactionManager.stop();
        synchronized (flushLock) {
            if (!active.isEmpty()) {
                SSTable flushed = sstableWriter.flush(active.snapshot(), 0);
                physicalBytesFlushed.addAndGet(Files.size(flushed.dataFile()));
                sstables.add(flushed);
            }
        }
        flushExecutor.shutdown();
        try {
            flushExecutor.awaitTermination(30, java.util.concurrent.TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        wal.close();
    }
}
