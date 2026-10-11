package dev.helios;

import dev.helios.compaction.CompactionManager;
import dev.helios.core.HeliosDB;
import dev.helios.server.HeliosServer;

import java.nio.file.Path;

public final class Main {

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            usage();
            System.exit(1);
        }
        String mode = args[0];
        switch (mode) {
            case "server" -> {
                if (args.length < 3) { usage(); System.exit(1); }
                Path dataDir = Path.of(args[1]);
                int port = Integer.parseInt(args[2]);
                CompactionManager.Mode cMode = args.length > 3
                    ? CompactionManager.Mode.valueOf(args[3].toUpperCase())
                    : CompactionManager.Mode.ADAPTIVE;
                HeliosServer srv = new HeliosServer(dataDir, port, cMode);
                srv.start();
                System.out.printf("[helios-kv] :%d  data=%s  mode=%s%n", port, dataDir, cMode);
                Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                    try { srv.stop(); } catch (Exception e) { e.printStackTrace(); }
                }));
                Thread.currentThread().join();
            }
            case "load" -> {
                Path dataDir = Path.of(args[1]);
                int count = Integer.parseInt(args[2]);
                load(dataDir, count);
            }
            case "verify" -> {
                Path dataDir = Path.of(args[1]);
                int count = Integer.parseInt(args[2]);
                verify(dataDir, count);
            }
            case "loadcontinuous" -> {
                Path dataDir = Path.of(args[1]);
                int startIndex = Integer.parseInt(args[2]);
                loadContinuous(dataDir, startIndex);
            }
            case "chaos" -> {
                Path dataDir = Path.of(args[1]);
                int iterations = Integer.parseInt(args[2]);
                chaos(dataDir, iterations);
            }
            default -> throw new IllegalArgumentException("unknown mode: " + mode);
        }
    }

    private static void usage() {
        System.err.println("Usage:");
        System.err.println("  Main server          <dataDir> <port> [ADAPTIVE|LEVELED|TIERED]");
        System.err.println("  Main load            <dataDir> <count>");
        System.err.println("  Main verify          <dataDir> <count>");
        System.err.println("  Main loadcontinuous  <dataDir> <startIndex>");
        System.err.println("  Main chaos           <dataDir> <iterations>");
    }

    private static void load(Path dataDir, int count) throws Exception {
        HeliosDB db = new HeliosDB(dataDir, 2 * 1024 * 1024);
        long start = System.nanoTime();
        for (int i = 0; i < count; i++) {
            db.put(key(i), value(i));
            if (i % 100_000 == 0 && i > 0)
                System.out.println("loaded " + i + " keys...");
        }
        double seconds = (System.nanoTime() - start) / 1e9;
        System.out.printf("loaded %,d keys in %.2fs (%,.0f ops/sec)%n", count, seconds, count / seconds);
        System.out.println("sstables: " + db.sstableCount());
        System.out.flush();
        Runtime.getRuntime().halt(0);
    }

    private static void verify(Path dataDir, int count) throws Exception {
        long start = System.nanoTime();
        HeliosDB db = new HeliosDB(dataDir, 2 * 1024 * 1024);
        System.out.printf("reopened in %.2fs%n", (System.nanoTime() - start) / 1e9);
        int correct = 0, wrong = 0, missing = 0;
        for (int i = 0; i < count; i++) {
            byte[] actual = db.get(key(i));
            if (actual == null) missing++;
            else if (!java.util.Arrays.equals(actual, value(i))) wrong++;
            else correct++;
        }
        System.out.printf("correct=%,d wrong=%,d missing=%,d%n", correct, wrong, missing);
        System.out.printf("write_amplification=%.2fx%n", db.writeAmplification());
        System.out.println(missing == 0 && wrong == 0 ? "PASS" : "FAIL");
        db.close();
    }

    private static void loadContinuous(Path dataDir, int startIndex) throws Exception {
        HeliosDB db = new HeliosDB(dataDir, 256 * 1024);
        int i = startIndex;
        while (true) {
            db.put(key(i), value(i));
            System.out.println(i);
            System.out.flush();
            i++;
        }
    }

    private static void chaos(Path dataDir, int iterations) throws Exception {
        String javaBin = System.getProperty("java.home") + "/bin/java";
        String classpath = System.getProperty("java.class.path");
        int nextIndex = 0, totalConfirmed = 0, failures = 0;
        java.util.Random rng = new java.util.Random(7);

        for (int iter = 1; iter <= iterations; iter++) {
            ProcessBuilder pb = new ProcessBuilder(javaBin, "-cp", classpath,
                "dev.helios.Main", "loadcontinuous", dataDir.toString(), String.valueOf(nextIndex));
            pb.redirectErrorStream(false);
            Process child = pb.start();
            int[] confirmed = {-1};
            Thread reader = new Thread(() -> {
                try (var br = new java.io.BufferedReader(
                        new java.io.InputStreamReader(child.getInputStream()))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        try { confirmed[0] = Integer.parseInt(line.trim()); }
                        catch (NumberFormatException ignored) {}
                    }
                } catch (java.io.IOException ignored) {}
            });
            reader.start();
            long delay = 50 + rng.nextInt(1450);
            Thread.sleep(delay);
            child.destroyForcibly();
            child.waitFor();
            reader.join(2000);

            if (confirmed[0] < 0) {
                try (HeliosDB probe = new HeliosDB(dataDir, 256 * 1024)) {
                    probe.sstableCount();
                    System.out.printf("iter %2d/%d: killed@%dms before first ack — recovery ok%n", iter, iterations, delay);
                } catch (Exception e) {
                    failures++;
                    System.out.printf("iter %2d/%d: FAIL reopen threw: %s%n", iter, iterations, e);
                }
                continue;
            }

            boolean ok = true;
            try (HeliosDB db = new HeliosDB(dataDir, 256 * 1024)) {
                for (int i = nextIndex; i <= confirmed[0]; i++) {
                    byte[] actual = db.get(key(i));
                    if (!java.util.Arrays.equals(actual, value(i))) {
                        ok = false;
                        System.out.printf("  MISMATCH key=%d expected=%s actual=%s%n",
                            i, new String(value(i)), actual == null ? "null" : new String(actual));
                    }
                }
            } catch (Exception e) {
                ok = false;
                System.out.printf("iter %2d/%d: FAIL exception: %s%n", iter, iterations, e);
            }

            int n = confirmed[0] - nextIndex + 1;
            totalConfirmed += n;
            if (!ok) failures++;
            System.out.printf("iter %2d/%d: killed@%4dms keys[%d..%d] (%d) — %s%n",
                iter, iterations, delay, nextIndex, confirmed[0], n, ok ? "PASS" : "FAIL");
            nextIndex = confirmed[0] + 1;
        }

        System.out.printf("%nchaos: %d iters, %,d keys verified, %d failures%n",
            iterations, totalConfirmed, failures);
        System.out.println(failures == 0 ? "PASS" : "FAIL");
        if (failures > 0) System.exit(1);
    }

    private static byte[] key(int i)   { return ("key" + i).getBytes(); }
    private static byte[] value(int i) { return ("value-" + i + "-" + (i * 31)).getBytes(); }
}
