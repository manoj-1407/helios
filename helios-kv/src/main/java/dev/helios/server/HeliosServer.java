package dev.helios.server;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import dev.helios.compaction.CompactionManager;
import dev.helios.core.HeliosDB;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public final class HeliosServer {

    private final HeliosDB db;
    private final HttpServer server;
    private final AtomicLong compactionEvents = new AtomicLong(0);
    private final AtomicReference<String> lastStrategy = new AtomicReference<>("none");

    public HeliosServer(Path dataDir, int port, CompactionManager.Mode mode) throws IOException {
        this.db = new HeliosDB(dataDir, 4 * 1024 * 1024, mode);
        this.server = HttpServer.create(new InetSocketAddress(port), 128);
        this.server.setExecutor(Executors.newFixedThreadPool(8));
        server.createContext("/put",      this::handlePut);
        server.createContext("/get",      this::handleGet);
        server.createContext("/delete",   this::handleDelete);
        server.createContext("/stats",    this::handleStats);
        server.createContext("/pressure", this::handlePressure);
        server.createContext("/health",   ex -> respond(ex, 200, "ok"));
    }

    public void start() { server.start(); }

    public void stop() throws IOException {
        server.stop(1);
        db.close();
    }

    private void handlePut(HttpExchange ex) throws IOException {
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
        if (!ex.getRequestMethod().equals("POST")) { respond(ex, 405, "method not allowed"); return; }
        byte[] key = ex.getRequestBody().readAllBytes();
        byte[] value = db.get(key);
        if (value == null) { respond(ex, 404, ""); return; }
        respond(ex, 200, new String(value, StandardCharsets.UTF_8));
    }

    private void handleDelete(HttpExchange ex) throws IOException {
        if (!ex.getRequestMethod().equals("POST")) { respond(ex, 405, "method not allowed"); return; }
        byte[] key = ex.getRequestBody().readAllBytes();
        db.delete(key);
        respond(ex, 200, "ok");
    }

    private void handleStats(HttpExchange ex) throws IOException {
        String strategy = db.activeCompactionStrategy();
        String prev = lastStrategy.getAndSet(strategy);
        boolean changed = !strategy.equals(prev);
        String body = String.format(
            "{\"write_amplification\":%.4f,\"sstable_count\":%d,\"compaction_events\":%d,\"strategy\":\"%s\",\"strategy_changed\":%b}",
            db.writeAmplification(), db.sstableCount(), compactionEvents.get(), strategy, changed
        );
        respond(ex, 200, body);
    }

    private void handlePressure(HttpExchange ex) throws IOException {
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

    private static void respond(HttpExchange ex, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", "application/json");
        ex.sendResponseHeaders(status, bytes.length);
        try (OutputStream os = ex.getResponseBody()) { os.write(bytes); }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("usage: HeliosServer <dataDir> <port> [ADAPTIVE|LEVELED|TIERED]");
            System.exit(1);
        }
        Path dataDir = Path.of(args[0]);
        int port = Integer.parseInt(args[1]);
        CompactionManager.Mode mode = args.length > 2
            ? CompactionManager.Mode.valueOf(args[2].toUpperCase())
            : CompactionManager.Mode.ADAPTIVE;
        HeliosServer srv = new HeliosServer(dataDir, port, mode);
        srv.start();
        System.out.printf("[helios-kv] listening on :%d  data=%s  mode=%s%n", port, dataDir, mode);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try { srv.stop(); } catch (Exception e) { e.printStackTrace(); }
        }));
        Thread.currentThread().join();
    }
}
