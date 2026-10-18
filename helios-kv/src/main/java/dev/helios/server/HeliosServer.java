package dev.helios.server;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import dev.helios.compaction.CompactionManager;
import dev.helios.core.HeliosDB;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public final class HeliosServer {

    private final HeliosDB db;
    private final Path dataDir;
    private final HttpServer server;
    private final AtomicLong compactionEvents = new AtomicLong(0);
    private final AtomicReference<String> lastStrategy = new AtomicReference<>("none");
    /** Shared secret for X-Helios-Token auth; null means auth disabled. */
    private final String authToken;

    public HeliosServer(Path dataDir, int port, CompactionManager.Mode mode) throws IOException {
        this(dataDir, port, mode, null, 4 * 1024 * 1024);
    }

    public HeliosServer(Path dataDir, int port, CompactionManager.Mode mode, String authToken) throws IOException {
        this(dataDir, port, mode, authToken, 4 * 1024 * 1024);
    }

    public HeliosServer(Path dataDir, int port, CompactionManager.Mode mode, String authToken,
                        long memTableMaxBytes) throws IOException {
        this.dataDir = dataDir;
        this.authToken = authToken;
        this.db = new HeliosDB(dataDir, memTableMaxBytes, mode);
        this.server = HttpServer.create(new InetSocketAddress(port), 128);
        this.server.setExecutor(Executors.newFixedThreadPool(8));
        server.createContext("/put",      this::handlePut);
        server.createContext("/get",      this::handleGet);
        server.createContext("/delete",   this::handleDelete);
        server.createContext("/stats",    this::handleStats);
        server.createContext("/pressure", this::handlePressure);
        server.createContext("/snapshot", this::handleSnapshot);
        server.createContext("/restore",  this::handleRestore);
        server.createContext("/scan",     this::handleScan);
        server.createContext("/metrics",  this::handleMetrics);
        // /health is intentionally unauthenticated so the Go bridge can poll it before auth is set up
        server.createContext("/health",   ex -> respond(ex, 200, "ok"));
    }

    /**
     * Returns true if the request carries a valid auth token, or if auth is disabled.
     * Sends a 401 and returns false otherwise — callers just return immediately on false.
     */
    private boolean authorized(HttpExchange ex) throws IOException {
        if (authToken == null || authToken.isEmpty()) return true;
        String header = ex.getRequestHeaders().getFirst("X-Helios-Token");
        if (authToken.equals(header)) return true;
        respond(ex, 401, "unauthorized");
        return false;
    }

    public void start() { server.start(); }

    public void stop() throws IOException {
        server.stop(1);
        db.close();
    }

    private void handlePut(HttpExchange ex) throws IOException {
        if (!authorized(ex)) return;
        if (!ex.getRequestMethod().equals("POST")) { respond(ex, 405, "method not allowed"); return; }
        byte[] body = ex.getRequestBody().readAllBytes();
        String raw = new String(body, StandardCharsets.UTF_8);
        int nl = raw.indexOf('\n');
        if (nl < 0) { respond(ex, 400, "expected key\\nvalue"); return; }
        String key = raw.substring(0, nl);
        String val = raw.substring(nl + 1);
        String prevStrategy = db.activeCompactionStrategy();
        db.put(key.getBytes(StandardCharsets.UTF_8), val.getBytes(StandardCharsets.UTF_8));
        String newStrategy = db.activeCompactionStrategy();
        if (!prevStrategy.equals(newStrategy)) compactionEvents.incrementAndGet();
        respond(ex, 200, "ok");
    }

    private void handleGet(HttpExchange ex) throws IOException {
        if (!authorized(ex)) return;
        if (!ex.getRequestMethod().equals("POST")) { respond(ex, 405, "method not allowed"); return; }
        byte[] key = ex.getRequestBody().readAllBytes();
        byte[] value = db.get(key);
        if (value == null) { respond(ex, 404, ""); return; }
        respond(ex, 200, new String(value, StandardCharsets.UTF_8));
    }

    private void handleDelete(HttpExchange ex) throws IOException {
        if (!authorized(ex)) return;
        if (!ex.getRequestMethod().equals("POST")) { respond(ex, 405, "method not allowed"); return; }
        byte[] key = ex.getRequestBody().readAllBytes();
        db.delete(key);
        respond(ex, 200, "ok");
    }

    /**
     * POST /scan — body is a UTF-8 prefix string; returns a JSON object
     * {"key":"value",...} of all live keys that start with that prefix.
     * Empty body means "scan everything". Keys and values are returned as
     * UTF-8 strings (matching what PUT/GET use on the wire).
     */
    private void handleScan(HttpExchange ex) throws IOException {
        if (!authorized(ex)) return;
        if (!ex.getRequestMethod().equals("POST")) { respond(ex, 405, "method not allowed"); return; }
        byte[] prefixBytes = ex.getRequestBody().readAllBytes();
        java.util.Map<byte[], byte[]> results = db.scan(prefixBytes);

        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (java.util.Map.Entry<byte[], byte[]> e : results.entrySet()) {
            if (!first) sb.append(",");
            first = false;
            String k = new String(e.getKey(),   java.nio.charset.StandardCharsets.UTF_8);
            String v = new String(e.getValue(), java.nio.charset.StandardCharsets.UTF_8);
            sb.append(jsonQuote(k)).append(":").append(jsonQuote(v));
        }
        sb.append("}");
        respond(ex, 200, sb.toString());
    }

    /** Minimal JSON string quoting — escapes backslash, double-quote, and control chars. */
    private static String jsonQuote(String s) {
        StringBuilder out = new StringBuilder("\"");
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if      (c == '"')  out.append("\\\"");
            else if (c == '\\') out.append("\\\\");
            else if (c < 0x20)  out.append(String.format("\\u%04x", (int) c));
            else                out.append(c);
        }
        return out.append("\"").toString();
    }

    private void handleStats(HttpExchange ex) throws IOException {
        if (!authorized(ex)) return;
        String strategy = db.activeCompactionStrategy();
        String prev = lastStrategy.getAndSet(strategy);
        boolean changed = !strategy.equals(prev);
        String body = String.format(
            "{\"write_amplification\":%.4f,\"sstable_count\":%d,\"compaction_events\":%d,\"strategy\":\"%s\",\"strategy_changed\":%b}",
            db.writeAmplification(), db.sstableCount(), compactionEvents.get(), strategy, changed
        );
        respond(ex, 200, body);
    }

    /**
     * GET /snapshot — streams a tar.gz of the dataDir so a Raft leader can
     * ship the full storage state to a lagging follower via InstallSnapshot.
     * The Go bridge reads this body and passes it as InstallSnapshotRequest.Data.
     */
    private void handleSnapshot(HttpExchange ex) throws IOException {
        if (!authorized(ex)) return;
        if (!ex.getRequestMethod().equals("GET")) { respond(ex, 405, "method not allowed"); return; }
        db.flushMemTable(); // make sure all in-memory state is on disk before we tar it
        byte[] tarGz = tarGzDir(dataDir);
        ex.getResponseHeaders().set("Content-Type", "application/octet-stream");
        ex.sendResponseHeaders(200, tarGz.length);
        try (OutputStream os = ex.getResponseBody()) { os.write(tarGz); }
    }

    /**
     * POST /restore — receives a tar.gz snapshot, stops the DB, unpacks over
     * dataDir, and reopens the storage engine. Called by the Go bridge on a
     * follower after HandleInstallSnapshot.
     */
    private void handleRestore(HttpExchange ex) throws IOException {
        if (!authorized(ex)) return;
        if (!ex.getRequestMethod().equals("POST")) { respond(ex, 405, "method not allowed"); return; }
        try (InputStream body = ex.getRequestBody()) {
            byte[] tarGz = body.readAllBytes();
            db.close();
            deleteDirContents(dataDir);
            untarGzDir(tarGz, dataDir);
            db.reopen(dataDir);
        }
        respond(ex, 200, "ok");
    }

    /** Creates a gzip-compressed tar archive of a directory in memory. */
    private static byte[] tarGzDir(Path dir) throws IOException {
        java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
        try (var gz = new java.util.zip.GZIPOutputStream(baos);
             var tar = new java.io.DataOutputStream(gz)) {
            Files.walk(dir)
                 .filter(Files::isRegularFile)
                 .forEach(p -> {
                     try {
                         String name = dir.relativize(p).toString();
                         byte[] data = Files.readAllBytes(p);
                         // Simple header: nameLen(4) name(n) dataLen(4) data(n)
                         tar.writeInt(name.length());
                         tar.write(name.getBytes(StandardCharsets.UTF_8));
                         tar.writeInt(data.length);
                         tar.write(data);
                     } catch (IOException e) { throw new RuntimeException(e); }
                 });
        }
        return baos.toByteArray();
    }

    private static void untarGzDir(byte[] tarGz, Path dir) throws IOException {
        Files.createDirectories(dir);
        try (var gz = new java.util.zip.GZIPInputStream(new java.io.ByteArrayInputStream(tarGz));
             var din = new java.io.DataInputStream(gz)) {
            try {
                while (true) {
                    int nameLen = din.readInt();
                    byte[] nameBytes = din.readNBytes(nameLen);
                    String name = new String(nameBytes, StandardCharsets.UTF_8);
                    int dataLen = din.readInt();
                    byte[] data = din.readNBytes(dataLen);
                    Path target = dir.resolve(name).normalize();
                    if (!target.startsWith(dir)) continue; // path traversal guard
                    Files.createDirectories(target.getParent());
                    Files.write(target, data);
                }
            } catch (java.io.EOFException ignored) { /* end of archive */ }
        }
    }

    private static void deleteDirContents(Path dir) throws IOException {
        if (!Files.exists(dir)) return;
        try (var s = Files.walk(dir)) {
            s.sorted(java.util.Comparator.reverseOrder())
             .filter(p -> !p.equals(dir))
             .forEach(p -> { try { Files.delete(p); } catch (IOException ignored) {} });
        }
    }

    private void handlePressure(HttpExchange ex) throws IOException {
        if (!authorized(ex)) return;
        if (!ex.getRequestMethod().equals("POST")) { respond(ex, 405, "method not allowed"); return; }
        String body = new String(ex.getRequestBody().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8).trim();
        try {
            double pressure = Double.parseDouble(body);
            dev.helios.classifier.ReplicationPressureReceiver.getInstance().update(pressure);
            respond(ex, 200, "ok");
        } catch (NumberFormatException e) {
            respond(ex, 400, "expected a float in [0.0, 1.0]");
        }
    }

    /**
     * GET /metrics — Prometheus text format (no library required).
     * Intentionally unauthenticated: Prometheus scrapers don't carry tokens,
     * and all values here are operational metrics, not user data. If you need
     * to lock this down, put the scraper behind the same network boundary as
     * the node and firewall /metrics from the outside.
     *
     * Metrics exposed:
     *   helios_write_amplification          gauge   physical/logical bytes ratio
     *   helios_sstable_count                gauge   live SSTables on disk
     *   helios_compaction_events_total      counter strategy-switch events seen
     *   helios_logical_bytes_written_total  counter raw bytes the caller asked us to store
     *   helios_physical_bytes_flushed_total counter bytes actually written to disk (flush+compaction)
     *   helios_active_strategy              info    label=strategy, value always 1
     */
    private void handleMetrics(HttpExchange ex) throws IOException {
        if (!ex.getRequestMethod().equals("GET")) { respond(ex, 405, "method not allowed"); return; }

        String strategy   = db.activeCompactionStrategy();
        double writeAmp   = db.writeAmplification();
        int    sstCount   = db.sstableCount();
        long   cmpEvents  = compactionEvents.get();
        long   logical    = db.logicalBytesWritten();
        long   physical   = db.physicalBytesFlushed();

        // Prometheus text format: one line per sample, TYPE and HELP before first sample.
        StringBuilder sb = new StringBuilder(512);
        metric(sb, "helios_write_amplification",
               "Physical-to-logical write ratio (1.0 = no amplification)",
               "gauge", String.format("%.6f", writeAmp), null, null);
        metric(sb, "helios_sstable_count",
               "Number of live SSTables on disk",
               "gauge", String.valueOf(sstCount), null, null);
        metric(sb, "helios_compaction_events_total",
               "Cumulative compaction strategy-switch events",
               "counter", String.valueOf(cmpEvents), null, null);
        metric(sb, "helios_logical_bytes_written_total",
               "Raw bytes the caller asked to store (pre-amplification)",
               "counter", String.valueOf(logical), null, null);
        metric(sb, "helios_physical_bytes_flushed_total",
               "Bytes written to disk including flush and compaction rewrites",
               "counter", String.valueOf(physical), null, null);
        // info-style metric: always value=1, carries the strategy as a label
        metric(sb, "helios_active_strategy_info",
               "Active compaction strategy (leveled / tiered / adaptive)",
               "gauge", "1", "strategy", sanitizeLabel(strategy));

        byte[] out = sb.toString().getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
        ex.sendResponseHeaders(200, out.length);
        try (OutputStream os = ex.getResponseBody()) { os.write(out); }
    }

    /** Appends one Prometheus metric family (HELP + TYPE + one sample line) to sb. */
    private static void metric(StringBuilder sb, String name, String help,
                                String type, String value,
                                String labelKey, String labelVal) {
        sb.append("# HELP ").append(name).append(' ').append(help).append('\n');
        sb.append("# TYPE ").append(name).append(' ').append(type).append('\n');
        sb.append(name);
        if (labelKey != null) {
            sb.append('{').append(labelKey).append("=\"").append(labelVal).append("\"}");
        }
        sb.append(' ').append(value).append('\n');
    }

    /** Strips characters Prometheus label values can't contain. */
    private static String sanitizeLabel(String s) {
        return s == null ? "unknown" : s.replaceAll("[^A-Za-z0-9_\\-]", "_");
    }

    private static void respond(HttpExchange ex, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", "application/json");
        ex.sendResponseHeaders(status, bytes.length);
        try (OutputStream os = ex.getResponseBody()) { os.write(bytes); }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("usage: HeliosServer <dataDir> <port> [ADAPTIVE|LEVELED|TIERED] [authToken]");
            System.exit(1);
        }
        Path dataDir = Path.of(args[0]);
        int port = Integer.parseInt(args[1]);
        CompactionManager.Mode mode = args.length > 2
            ? CompactionManager.Mode.valueOf(args[2].toUpperCase())
            : CompactionManager.Mode.ADAPTIVE;
        String authToken = args.length > 3 ? args[3] : null;
        HeliosServer srv = new HeliosServer(dataDir, port, mode, authToken);
        srv.start();
        System.out.printf("[helios-kv] listening on :%d  data=%s  mode=%s  auth=%s%n",
            port, dataDir, mode, authToken != null ? "enabled" : "disabled");
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try { srv.stop(); } catch (Exception e) { e.printStackTrace(); }
        }));
        Thread.currentThread().join();
    }
}
