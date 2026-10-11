package dev.helios;

import dev.helios.compaction.CompactionTest;
import dev.helios.core.HeliosDBTest;
import dev.helios.core.MemTableTest;
import dev.helios.core.WALTest;

/**
 * Runs every test class's main() in sequence and reports a summary.
 * Stands in for a JUnit suite for the reason explained in TestSupport:
 * Maven Central wasn't reachable from this sandbox to actually pull in
 * a real test framework.
 */
public final class AllTests {

    private record TestClass(String name, ThrowingRunnable run) {}

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    public static void main(String[] args) {
        TestClass[] tests = {
                new TestClass("MemTableTest", () -> MemTableTest.main(new String[0])),
                new TestClass("WALTest", () -> WALTest.main(new String[0])),
                new TestClass("HeliosDBTest", () -> HeliosDBTest.main(new String[0])),
                new TestClass("CompactionTest", () -> CompactionTest.main(new String[0])),
        };

        int passed = 0, failed = 0;
        for (TestClass t : tests) {
            System.out.println("--- " + t.name() + " ---");
            try {
                t.run().run();
                passed++;
            } catch (Throwable e) {
                failed++;
                System.out.println(t.name() + " FAILED: " + e);
            }
            System.out.println();
        }

        System.out.printf("AllTests: %d passed, %d failed (out of %d)%n", passed, failed, tests.length);
        if (failed > 0) System.exit(1);
    }
}
