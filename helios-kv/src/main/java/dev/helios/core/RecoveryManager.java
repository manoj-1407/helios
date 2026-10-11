package dev.helios.core;

import dev.helios.sstable.SSTable;
import dev.helios.sstable.SSTableReader;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

/**
 * Everything that has to happen between "process started" and "the engine
 * is safe to accept writes":
 *
 *   1. Find every WAL segment on disk (there's normally exactly one, but a
 *      crash between "flush finished" and "delete the old WAL" can leave
 *      more than one behind — that's expected, not corruption, so we
 *      replay all of them in file-id order rather than assuming there's
 *      only one).
 *   2. Replay them into a fresh MemTable. Later records for the same key
 *      naturally overwrite earlier ones because we replay in order, which
 *      is exactly the semantics we want.
 *   3. Reopen every existing .sst file by reading its footer (cheap — we
 *      never re-scan the data section, just the trailer with the index
 *      and bloom filter already serialized inside it).
 *
 * WAL filenames and SSTable filenames both encode their creation order
 * (System.nanoTime()), which is what lets us sort them correctly without
 * needing a separate manifest file. A real production engine would add a
 * manifest for crash-safety around the rename/delete steps themselves —
 * called out explicitly in the README as a known limitation rather than
 * pretending this is bulletproof.
 */
public final class RecoveryManager {

    private RecoveryManager() {}

    public record RecoveredState(MemTable memTable, List<SSTable> sstables, List<Path> walFilesToCleanUp) {}

    public static RecoveredState recover(Path dataDir, long memTableMaxBytes) throws IOException {
        Path walDir = dataDir.resolve("wal");
        Path sstDir = dataDir.resolve("sstables");
        Files.createDirectories(walDir);
        Files.createDirectories(sstDir);

        List<Path> walFiles = listSorted(walDir, ".log");
        MemTable recovered = new MemTable(memTableMaxBytes);
        for (Path walFile : walFiles) {
            for (WAL.Record record : WAL.recover(walFile)) {
                recovered.put(record.key(), record.value());
            }
        }

        List<SSTable> sstables = new ArrayList<>();
        for (Path sstFile : listSorted(sstDir, ".sst")) {
            LevelId levelAndId = parseLevelAndId(sstFile);
            sstables.add(SSTableReader.open(sstFile, levelAndId.level(), levelAndId.id()));
        }
        sstables.sort(Comparator.comparingLong(SSTable::id));

        return new RecoveredState(recovered, sstables, walFiles);
    }

    private static List<Path> listSorted(Path dir, String suffix) throws IOException {
        try (Stream<Path> stream = Files.list(dir)) {
            List<Path> files = stream
                    .filter(p -> p.toString().endsWith(suffix))
                    .sorted(Comparator.comparing(p -> p.getFileName().toString()))
                    .toList();
            return new ArrayList<>(files);
        }
    }

    private record LevelId(int level, long id) {}

    /** Filenames are "{level}_{id}.sst" — see SSTableWriter. id is System.nanoTime(), so it must stay a long all the way through. */
    private static LevelId parseLevelAndId(Path sstFile) {
        String name = sstFile.getFileName().toString();
        String withoutExt = name.substring(0, name.length() - ".sst".length());
        int sep = withoutExt.indexOf('_');
        int level = Integer.parseInt(withoutExt.substring(0, sep));
        long id = Long.parseLong(withoutExt.substring(sep + 1));
        return new LevelId(level, id);
    }
}
