package dev.helios.compaction;

import dev.helios.sstable.SSTable;
import dev.helios.sstable.SSTableReader;
import dev.helios.sstable.SSTableWriter;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * Shared merge logic for any strategy that consolidates multiple SSTables
 * into one. Leveled and Tiered compaction differ only in *when* they
 * trigger and *how many* tables they fold together — the actual merge
 * mechanics are identical, so they live here instead of being duplicated.
 *
 * Correctness depends on one invariant that both callers must uphold:
 * every merge here has to include the *entire* current table pool, not
 * some arbitrary subset. The reason is tombstones — dropping a delete
 * marker is only safe once you know there's no older value for that key
 * left anywhere else. If a merge only ever touched a subset of tables,
 * an older un-merged table could still be holding the value the
 * tombstone was supposed to have erased, and dropping the tombstone here
 * would let that old value resurface on the next read. Always merging
 * the full pool sidesteps the problem entirely: nothing is left behind
 * to resurrect.
 */
final class CompactionMerge {

    private CompactionMerge() {}

    static SSTable mergeAll(List<SSTable> tables, SSTableWriter writer, int targetLevel) throws IOException {
        List<SSTable> oldestFirst = new java.util.ArrayList<>(tables);
        oldestFirst.sort(Comparator.comparingLong(SSTable::id));

        NavigableMap<byte[], byte[]> merged = new TreeMap<>(Arrays::compareUnsigned);
        for (SSTable table : oldestFirst) {
            for (SSTableReader.RawEntry entry : SSTableReader.scanAll(table)) {
                // Processing oldest -> newest means a later table's entry for
                // the same key naturally overwrites an earlier one here,
                // which is exactly the "newest write wins" rule reads use.
                merged.put(entry.key(), entry.value());
            }
        }

        // Full-pool merge (see class doc) -> safe to drop tombstones now.
        merged.entrySet().removeIf(e -> e.getValue() == null);

        if (merged.isEmpty()) {
            return null; // every surviving key in this pool had already been deleted
        }
        return writer.flush(merged, targetLevel);
    }
}
