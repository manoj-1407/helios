package dev.helios.bench;

import dev.helios.compaction.CompactionManager;
import dev.helios.core.HeliosDB;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.function.IntToDoubleFunction;

/**
 * The actual test of this project's thesis. Three fixed profiles
 * (write-heavy, read-heavy, mixed) plus a fourth — "shifting" — that
 * spends its first half write-heavy and its second half read-heavy
 * inside a single run. The fixed profiles mostly tell you that adaptive
 * settles into roughly whatever the matching static strategy already
 * does, which is expected and not very interesting on its own. The
 * shifting profile is where the actual claim gets tested: a static
 * choice is necessarily wrong for at least half of that run, while
 * adaptive should track the change and switch underneath it. The write
 * amplification and sstable-count numbers for "shifting" are the ones
 * that matter most when reading this output.
 *
 * Scale: op counts default small enough to finish in a couple of minutes
 * in a constrained sandbox — this engine's WAL opens with DSYNC, so
 * every single put() pays a real fsync, and throughput sits in the
 * low thousands of ops/sec rather than the hundreds of thousands a
 * batched/group-commit WAL would get. That's a real, known
 * characteristic of this engine (see README "What I'd do next"), not a
 * benchmark artifact — scale args[0] up for a more statistically
 * meaningful run on real hardware; at 1,000,000 ops per config this
 * would take on the order of hours here, which is why the default is
 * not 1,000,000.
 *
 * Usage: java -cp out dev.helios.bench.BenchmarkRunner [ops] [preload] [outputDir]
 */
public final class BenchmarkRunner {

    private record Profile(String name, IntToDoubleFunction writeRatioAt) {}
    private record Config(String name, CompactionManager.Mode mode) {}

    private record Result(String profile, String config, long opsPerSec, double writeAmpX,
                           long p50Micros, long p99Micros, int finalSstableCount, long elapsedMillis) {}

    public static void main(String[] args) throws Exception {
        int ops = args.length > 0 ? Integer.parseInt(args[0]) : 25_000;
        int preload = args.length > 1 ? Integer.parseInt(args[1]) : 2_000;
        Path baseDir = Path.of(args.length > 2 ? args[2] : "bench/data");

        List<Profile> profiles = List.of(
                new Profile("write_heavy", i -> 0.9),
                new Profile("read_heavy", i -> 0.1),
                new Profile("mixed", i -> 0.5),
                new Profile("shifting", i -> i < ops / 2 ? 0.9 : 0.1) // the profile that actually tests the thesis
        );
        List<Config> configs = List.of(
                new Config("no_compaction", CompactionManager.Mode.DISABLED),
                new Config("static_leveled", CompactionManager.Mode.FORCE_LEVELED),
                new Config("static_tiered", CompactionManager.Mode.FORCE_TIERED),
                new Config("adaptive", CompactionManager.Mode.ADAPTIVE)
        );

        System.out.printf("Running %d ops x %d configs x %d profiles (preload=%d each)...%n%n",
                ops, configs.size(), profiles.size(), preload);

        List<Result> results = new ArrayList<>();
        for (Profile profile : profiles) {
            for (Config config : configs) {
                Path dir = baseDir.resolve(profile.name() + "_" + config.name());
                deleteRecursive(dir);
                Result r = runOne(dir, profile, config, ops, preload);
                results.add(r);
                System.out.printf("  [%-12s / %-14s] %,7d ops/sec | writeAmp %5.2fx | p50 %4dus | p99 %5dus | %2d sstables%n",
                        r.profile(), r.config(), r.opsPerSec(), r.writeAmpX(), r.p50Micros(), r.p99Micros(), r.finalSstableCount());
            }
        }

        System.out.println();
        printSummaryTable(results);
        printShiftingHighlight(results);

        deleteRecursive(baseDir); // scratch data, not a deliverable
    }

    private static Result runOne(Path dir, Profile profile, Config config, int ops, int preload) throws Exception {
        // Small memTable threshold relative to the data volume here is intentional:
        // production defaults (MB-scale) would never accumulate enough SSTables
        // for compaction to do anything within a 25k-op benchmark run. This is
        // tuned for the benchmark, not a production setting -- see HeliosDB's
        // own default constructor for that.
        HeliosDB db = new HeliosDB(dir, 48 * 1024, config.mode());
        Random rng = new Random(42);

        for (int i = 0; i < preload; i++) {
            db.put(key(i), value(i));
        }

        long[] latenciesNanos = new long[ops];
        int maxKeySeen = preload - 1;
        long start = System.nanoTime();
        for (int i = 0; i < ops; i++) {
            long opStart = System.nanoTime();
            if (rng.nextDouble() < profile.writeRatioAt().applyAsDouble(i)) {
                int k = preload + i; // writes always target a fresh key
                db.put(key(k), value(k));
                maxKeySeen = k;
            } else {
                int k = rng.nextInt(maxKeySeen + 1); // reads always target something that exists
                db.get(key(k));
            }
            latenciesNanos[i] = System.nanoTime() - opStart;
        }
        long elapsedNanos = System.nanoTime() - start;

        db.close(); // flush whatever's left in memory before reading final stats

        Arrays.sort(latenciesNanos);
        long p50 = latenciesNanos[ops / 2] / 1_000;
        long p99 = latenciesNanos[(int) (ops * 0.99)] / 1_000;
        long opsPerSec = (long) (ops / (elapsedNanos / 1e9));

        return new Result(profile.name(), config.name(), opsPerSec, db.writeAmplification(),
                p50, p99, db.sstableCount(), elapsedNanos / 1_000_000);
    }

    private static void printSummaryTable(List<Result> results) {
        System.out.println("=".repeat(82));
        System.out.printf("%-12s %-16s %10s %10s %9s %9s %9s%n",
                "profile", "config", "ops/sec", "writeAmpX", "p50(us)", "p99(us)", "sstables");
        System.out.println("-".repeat(82));
        String lastProfile = "";
        for (Result r : results) {
            if (!r.profile().equals(lastProfile)) {
                if (!lastProfile.isEmpty()) System.out.println();
                lastProfile = r.profile();
            }
            System.out.printf("%-12s %-16s %10d %10.2f %9d %9d %9d%n",
                    r.profile(), r.config(), r.opsPerSec(), r.writeAmpX(), r.p50Micros(), r.p99Micros(), r.finalSstableCount());
        }
        System.out.println("=".repeat(82));
    }

    private static void printShiftingHighlight(List<Result> results) {
        System.out.println();
        System.out.println("The thesis test (\"shifting\" profile -- write-heavy first half, read-heavy second half):");
        for (Result r : results) {
            if (!r.profile().equals("shifting")) continue;
            System.out.printf("  %-16s write amplification = %.2fx, final sstables = %d%n",
                    r.config(), r.writeAmpX(), r.finalSstableCount());
        }
        System.out.println("  A static strategy is necessarily wrong for half of this run.");
        System.out.println("  Whether 'adaptive' actually beats both static rows above on write");
        System.out.println("  amplification is the real result -- read it from the numbers, not this sentence.");
    }

    private static byte[] key(int i) {
        return ("bench-key-" + i).getBytes();
    }

    private static byte[] value(int i) {
        return ("bench-value-" + i + "-payload").getBytes();
    }

    private static void deleteRecursive(Path p) throws IOException {
        if (!Files.exists(p)) return;
        Files.walk(p).sorted(Comparator.reverseOrder()).forEach(f -> f.toFile().delete());
    }
}
