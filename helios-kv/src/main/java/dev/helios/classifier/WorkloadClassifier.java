package dev.helios.classifier;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Classifies recent traffic as write-heavy, read-heavy, or mixed.
 *
 * Two layers of smoothing, both deliberate:
 *
 *   1. classify() drains the write/read counters and computes one
 *      instantaneous ratio per call. A single call to classify() right
 *      after a 50-put burst followed by nothing shouldn't flip the whole
 *      engine's compaction strategy on that alone — bursts are normal,
 *      noise shouldn't trigger a strategy switch.
 *   2. The instantaneous classifications go into a fixed-size sliding
 *      window, and the *dominant* type across that window is what gets
 *      returned and acted on. This is what actually prevents flapping:
 *      CompactionManager calls classify() on a timer (every few seconds),
 *      and only a sustained shift in traffic shape — not one noisy tick —
 *      changes which compaction strategy is active.
 *
 * If there's been no traffic at all since the last tick (total == 0),
 * this reports the previous dominant type rather than forcing MIXED —
 * an idle database isn't evidence the workload shape changed, it's just
 * idle, so the compaction strategy shouldn't get yanked back to a
 * default for no reason.
 */
public final class WorkloadClassifier {

    private final int windowSize;
    private final AtomicLong writes = new AtomicLong();
    private final AtomicLong reads = new AtomicLong();
    private final Queue<WorkloadType> window = new ConcurrentLinkedQueue<>();
    private volatile WorkloadType lastDominant = WorkloadType.MIXED;

    public WorkloadClassifier(int windowSize) {
        this.windowSize = windowSize;
    }

    public void recordWrite() { writes.incrementAndGet(); }
    public void recordRead() { reads.incrementAndGet(); }

    public WorkloadType classify() {
        long w = writes.getAndSet(0);
        long r = reads.getAndSet(0);
        long total = w + r;

        if (total == 0) {
            return lastDominant; // idle tick, don't manufacture a signal
        }

        double writeRatio = (double) w / total;
        WorkloadType instantaneous = writeRatio > 0.75 ? WorkloadType.WRITE_HEAVY
                : writeRatio < 0.25 ? WorkloadType.READ_HEAVY
                : WorkloadType.MIXED;

        synchronized (window) {
            if (window.size() >= windowSize) window.poll();
            window.offer(instantaneous);
            lastDominant = dominantType();
        }
        return lastDominant;
    }

    private WorkloadType dominantType() {
        Map<WorkloadType, Long> freq = window.stream()
                .collect(Collectors.groupingBy(t -> t, Collectors.counting()));
        return freq.entrySet().stream()
                .max(Map.Entry.comparingByValue())
                .map(Map.Entry::getKey)
                .orElse(WorkloadType.MIXED);
    }

    /** For observability/demo output — what the window currently looks like, oldest first. */
    public String windowSnapshot() {
        return window.toString();
    }
}
