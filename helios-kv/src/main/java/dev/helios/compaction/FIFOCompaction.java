package dev.helios.compaction;

import dev.helios.sstable.SSTable;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * The cheapest possible compaction strategy: never rewrite anything. Once
 * total on-disk size crosses a budget, the oldest SSTables are dropped
 * outright — not merged, just deleted along with everything they
 * contained. This is RocksDB's actual FIFOCompactionStyle, built for
 * time-series and TTL-style data where old records are expected to become
 * worthless and get discarded wholesale, not selectively garbage-collected.
 *
 * This is the one strategy in this engine where the workload classifier
 * matching it to "time-series-like patterns" (per the original design
 * thesis) doesn't map cleanly onto WRITE_HEAVY/READ_HEAVY/MIXED — true
 * FIFO selection would need a fourth signal ("is this access pattern
 * append-only with a recency-biased read pattern"), which the classifier
 * doesn't currently detect. FIFOCompaction is implemented and fully
 * functional, but CompactionManager's adaptive mode doesn't select it
 * automatically yet; it's reachable via an explicit forced mode for
 * benchmarking, and wiring up real TTL/time-series detection is called
 * out as future work rather than faked.
 */
public final class FIFOCompaction implements CompactionStrategy {

    private final long maxTotalBytes;

    public FIFOCompaction(long maxTotalBytes) {
        this.maxTotalBytes = maxTotalBytes;
    }

    @Override
    public CompactionPlan plan(List<SSTable> currentTables) throws IOException {
        long total = 0;
        for (SSTable t : currentTables) total += Files.size(t.dataFile());
        if (total <= maxTotalBytes) return CompactionPlan.NONE;

        List<SSTable> oldestFirst = new ArrayList<>(currentTables);
        oldestFirst.sort(Comparator.comparingLong(SSTable::id));

        List<SSTable> toEvict = new ArrayList<>();
        long remaining = total;
        for (SSTable t : oldestFirst) {
            if (remaining <= maxTotalBytes) break;
            remaining -= Files.size(t.dataFile());
            toEvict.add(t);
        }
        return new CompactionPlan(toEvict, List.of());
    }

    @Override
    public String name() {
        return "fifo";
    }
}
