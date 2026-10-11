package dev.helios.compaction;

import dev.helios.sstable.SSTable;

import java.util.List;

/**
 * What a CompactionStrategy decided to do this tick: which existing
 * SSTables become obsolete (toRemove) and which new ones replace them
 * (toAdd — empty for pure eviction like FIFO, since nothing gets rewritten).
 *
 * Deliberately not "old SSTables get mutated" — SSTables are immutable
 * once flushed, full stop. Compaction always produces a *new* file (or
 * just deletes, for FIFO) and the swap from old to new is what
 * CompactionManager applies atomically against the live table list.
 */
public record CompactionPlan(List<SSTable> toRemove, List<SSTable> toAdd) {

    public static final CompactionPlan NONE = new CompactionPlan(List.of(), List.of());

    public boolean isNoop() {
        return toRemove.isEmpty();
    }
}
