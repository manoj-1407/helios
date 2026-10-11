package dev.helios.classifier;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Receives replication pressure signals from the Go Raft layer via the bridge
 * and makes them available to the CompactionManager.
 *
 * This is the core of what makes helios novel: a storage engine that knows
 * about the replication health of the cluster it's part of, and backs off on
 * compaction I/O when followers are falling behind. RocksDB, LevelDB, TiKV,
 * and Cassandra all compact based purely on local storage metrics. None of
 * them have a feedback loop from the replication layer.
 *
 * The pressure level (0.0-1.0) comes from the Go bridge, which reads it from
 * RaftNode.RepMetrics.PressureLevel(). The bridge calls /pressure on this
 * JVM's HTTP server whenever the pressure level changes significantly (>0.1
 * delta), avoiding a polling overhead while still keeping the storage layer
 * informed within one heartbeat interval of any real change.
 */
public final class ReplicationPressureReceiver {

    public enum PressureState {
        NORMAL,      // < 0.3 — compact freely
        ELEVATED,    // 0.3-0.6 — compact at reduced aggressiveness
        HIGH,        // 0.6-0.85 — defer non-urgent compaction
        CRITICAL     // > 0.85 — suspend compaction entirely
    }

    private static final ReplicationPressureReceiver INSTANCE =
            new ReplicationPressureReceiver();

    private final AtomicReference<Double> currentPressure =
            new AtomicReference<>(0.0);

    private ReplicationPressureReceiver() {}

    public static ReplicationPressureReceiver getInstance() {
        return INSTANCE;
    }

    /** Called by HeliosServer when it receives a POST /pressure from the bridge. */
    public void update(double pressure) {
        currentPressure.set(Math.max(0.0, Math.min(1.0, pressure)));
    }

    public double get() {
        return currentPressure.get();
    }

    public PressureState state() {
        double p = currentPressure.get();
        if (p < 0.3)  return PressureState.NORMAL;
        if (p < 0.6)  return PressureState.ELEVATED;
        if (p < 0.85) return PressureState.HIGH;
        return PressureState.CRITICAL;
    }

    /**
     * Returns a multiplier (0.0-1.0) for how aggressively to compact.
     * 1.0 = full aggressiveness (normal state).
     * 0.0 = suspend compaction entirely (critical state).
     *
     * CompactionManager multiplies its trigger thresholds by this value:
     * if leveled normally triggers at 3 SSTables, at 0.5 aggressiveness
     * it waits until 6 SSTables accumulate before merging, halving the
     * disk I/O pressure on the shared disk that replication also uses.
     */
    public double compactionAggressiveness() {
        switch (state()) {
            case NORMAL:    return 1.0;
            case ELEVATED:  return 0.7;
            case HIGH:      return 0.3;
            case CRITICAL:  return 0.0;
            default:        return 1.0;
        }
    }
}
