package dev.helios.compaction;

import dev.helios.sstable.SSTable;
import dev.helios.sstable.SSTableWriter;

import java.io.IOException;
import java.util.List;

/**
 * Optimizes for write throughput: every merge rewrites every byte that's
 * still live, which is exactly the "write amplification" this whole
 * project is about minimizing. Under a write-heavy load, paying that cost
 * frequently (the way LeveledCompaction does) actively works against the
 * goal — so this strategy lets considerably more SSTables accumulate
 * before it triggers, batching more data into each rewrite and merging
 * far less often overall. The trade is read fan-out: a get() may have to
 * check more tables before this fires. That's the right trade specifically
 * when traffic is write-heavy, which is when CompactionManager selects it.
 */
public final class TieredCompaction implements CompactionStrategy {

    private static final int BASE_TRIGGER_TABLE_COUNT = 6;

    private final SSTableWriter writer;

    public TieredCompaction(SSTableWriter writer) {
        this.writer = writer;
    }

    @Override
    public CompactionPlan plan(List<SSTable> currentTables) throws IOException {
        double aggressiveness = dev.helios.classifier.ReplicationPressureReceiver
                .getInstance().compactionAggressiveness();
        if (aggressiveness == 0.0) return CompactionPlan.NONE;
        int trigger = (int) Math.ceil(BASE_TRIGGER_TABLE_COUNT / aggressiveness);
        if (currentTables.size() < trigger) return CompactionPlan.NONE;

        int nextLevel = currentTables.stream().mapToInt(SSTable::level).max().orElse(0) + 1;
        SSTable merged = CompactionMerge.mergeAll(currentTables, writer, nextLevel);

        List<SSTable> toAdd = (merged == null) ? List.of() : List.of(merged);
        return new CompactionPlan(List.copyOf(currentTables), toAdd);
    }

    @Override
    public String name() {
        return "tiered";
    }
}
