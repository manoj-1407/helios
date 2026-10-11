package dev.helios.compaction;

import dev.helios.sstable.SSTable;

import java.io.IOException;
import java.util.List;

/**
 * A pluggable policy for deciding, given the current set of SSTables,
 * whether to merge/evict anything right now. CompactionManager calls
 * plan() on whichever strategy is currently active and applies the
 * result; the strategy itself never touches the live table list or the
 * filesystem directly, which is what makes it safe to hot-swap the active
 * strategy between ticks without any coordination beyond "the next tick
 * uses whichever strategy is current at that moment."
 */
public interface CompactionStrategy {
    CompactionPlan plan(List<SSTable> currentTables) throws IOException;

    String name();
}
