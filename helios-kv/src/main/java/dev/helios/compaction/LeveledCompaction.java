package dev.helios.compaction;

import dev.helios.sstable.SSTable;
import dev.helios.sstable.SSTableWriter;

import java.io.IOException;
import java.util.List;

/**
 * Optimizes for read latency: every SSTable a get() has to check costs a
 * bloom-filter probe at minimum, and a disk seek on a bloom false
 * positive or an actual hit. Fewer, larger tables means fewer checks per
 * read. So this strategy triggers a full merge as soon as the table count
 * crosses a small threshold — willing to pay merge cost (write
 * amplification) often, in exchange for keeping read fan-out low. That
 * trade only makes sense when the workload actually is read-heavy, which
 * is exactly when CompactionManager selects this strategy.
 */
public final class LeveledCompaction implements CompactionStrategy {

    private static final int BASE_TRIGGER_TABLE_COUNT = 3;

    private final SSTableWriter writer;

    public LeveledCompaction(SSTableWriter writer) {
        this.writer = writer;
    }

    @Override
    public CompactionPlan plan(List<SSTable> currentTables) throws IOException {
        // Scale trigger threshold by replication pressure.
        // Under high pressure, defer compaction to reduce disk I/O contention
        // with replication traffic. At 0.0 aggressiveness (CRITICAL), we
        // never trigger -- followers are so far behind that adding more I/O
        // would make it worse.
        double aggressiveness = dev.helios.classifier.ReplicationPressureReceiver
                .getInstance().compactionAggressiveness();
        if (aggressiveness == 0.0) return CompactionPlan.NONE;

        // Higher aggressiveness = lower threshold = compact sooner.
        // Lower aggressiveness = higher threshold = defer compaction.
        int trigger = (int) Math.ceil(BASE_TRIGGER_TABLE_COUNT / aggressiveness);
        if (currentTables.size() < trigger) return CompactionPlan.NONE;

        int nextLevel = currentTables.stream().mapToInt(SSTable::level).max().orElse(0) + 1;
        SSTable merged = CompactionMerge.mergeAll(currentTables, writer, nextLevel);

        List<SSTable> toAdd = (merged == null) ? List.of() : List.of(merged);
        return new CompactionPlan(List.copyOf(currentTables), toAdd);
    }

    @Override
    public String name() {
        return "leveled";
    }
}
