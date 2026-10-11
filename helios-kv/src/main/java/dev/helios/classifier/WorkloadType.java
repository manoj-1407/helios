package dev.helios.classifier;

/**
 * What the engine thinks is happening right now, based on recent
 * get()/put() call volume. Not a property of the data — purely a
 * property of how the caller has been hitting the API lately.
 */
public enum WorkloadType {
    WRITE_HEAVY,
    READ_HEAVY,
    MIXED
}
