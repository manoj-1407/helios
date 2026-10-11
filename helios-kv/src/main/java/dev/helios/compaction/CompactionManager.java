package dev.helios.compaction;

import dev.helios.classifier.WorkloadClassifier;
import dev.helios.classifier.WorkloadType;
import dev.helios.sstable.SSTable;
import dev.helios.sstable.SSTableWriter;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The background thread that turns "what does the classifier think the
 * workload is" into "which compaction strategy is actually running."
 *
 * Concurrency model: {@code sstables} is the *same* CopyOnWriteArrayList
 * instance HeliosDB hands out to readers and to the flush path. This
 * class never copies it permanently — it takes a one-shot snapshot
 * (`new ArrayList<>(sstables)`) to hand to the strategy's plan() method
 * (so the strategy itself doesn't have to deal with concurrent
 * modification), then applies whatever plan comes back directly against
 * the live list.
 *
 * Publish order is deliberate: new tables are added to the live list
 * BEFORE old ones are removed, and old ones are removed from the list
 * BEFORE their files are deleted from disk. That ordering means there is
 * a brief window where both the old and new SSTables are visible to
 * readers at once (harmless — redundant, not wrong, since the new table
 * has a newer id and gets checked first), but there is never a window
 * where neither is visible. Reversing either half of that ordering would
 * open a real data-loss gap for any get() that lands inside it.
 */
public final class CompactionManager {

    public enum Mode { ADAPTIVE, FORCE_LEVELED, FORCE_TIERED, FORCE_FIFO, DISABLED }

    private final WorkloadClassifier classifier;
    private final CopyOnWriteArrayList<SSTable> sstables;
    private final Map<WorkloadType, CompactionStrategy> adaptiveStrategies;
    private final ScheduledExecutorService scheduler;
    private final Mode mode;
    private final long tickIntervalMillis;

    private volatile CompactionStrategy active;
    private final AtomicLong physicalBytesWrittenByCompaction = new AtomicLong();
    private final List<String> switchLog = new CopyOnWriteArrayList<>();

    public CompactionManager(WorkloadClassifier classifier,
                              CopyOnWriteArrayList<SSTable> sstables,
                              SSTableWriter writer,
                              long fifoMaxBytes,
                              Mode mode,
                              long tickIntervalMillis) {
        this.classifier = classifier;
        this.sstables = sstables;
        this.mode = mode;
        this.tickIntervalMillis = tickIntervalMillis;
        this.adaptiveStrategies = Map.of(
                WorkloadType.WRITE_HEAVY, new TieredCompaction(writer),
                WorkloadType.READ_HEAVY, new LeveledCompaction(writer),
                WorkloadType.MIXED, new LeveledCompaction(writer)
        );
        this.active = switch (mode) {
            case FORCE_LEVELED -> new LeveledCompaction(writer);
            case FORCE_TIERED -> new TieredCompaction(writer);
            case FORCE_FIFO -> new FIFOCompaction(fifoMaxBytes);
            case DISABLED -> null;
            case ADAPTIVE -> adaptiveStrategies.get(WorkloadType.MIXED);
        };
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "helios-compaction");
            t.setDaemon(true);
            return t;
        });
    }

    public void start() {
        if (mode == Mode.DISABLED) return;
        scheduler.scheduleWithFixedDelay(this::tick, tickIntervalMillis, tickIntervalMillis, TimeUnit.MILLISECONDS);
    }

    private void tick() {
        try {
            if (mode == Mode.ADAPTIVE) {
                WorkloadType current = classifier.classify();
                CompactionStrategy next = adaptiveStrategies.get(current);
                if (next != active) {
                    switchLog.add("t=" + System.currentTimeMillis() + " workload=" + current
                            + " -> switched to " + next.name() + " compaction");
                    active = next;
                }
            }
            if (active == null) return;

            List<SSTable> snapshot = new ArrayList<>(sstables);
            if (snapshot.size() < 2) return;

            CompactionPlan plan = active.plan(snapshot);
            if (plan.isNoop()) return;

            sstables.addAll(plan.toAdd());     // publish new tables first (see class doc)
            sstables.removeAll(plan.toRemove()); // then retire old references
            for (SSTable added : plan.toAdd()) {
                physicalBytesWrittenByCompaction.addAndGet(Files.size(added.dataFile()));
            }
            for (SSTable removed : plan.toRemove()) {
                Files.deleteIfExists(removed.dataFile()); // only now is it safe to actually delete bytes
            }
        } catch (IOException e) {
            System.err.println("[helios-kv] compaction tick failed (will retry next tick): " + e);
        } catch (Throwable t) {
            System.err.println("[helios-kv] compaction tick threw unexpectedly: " + t);
            t.printStackTrace();
        }
    }

    public void stop() {
        scheduler.shutdown();
        try {
            scheduler.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public long physicalBytesWrittenByCompaction() {
        return physicalBytesWrittenByCompaction.get();
    }

    public List<String> switchLog() {
        return switchLog;
    }

    public String activeStrategyName() {
        CompactionStrategy a = active;
        return a == null ? "none" : a.name();
    }
}
