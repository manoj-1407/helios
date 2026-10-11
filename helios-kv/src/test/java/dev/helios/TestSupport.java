package dev.helios;

/**
 * Stand-in for JUnit. This sandbox's network egress is restricted to a
 * fixed allowlist that doesn't include Maven Central, so `org.junit:*`
 * couldn't actually be resolved as a dependency here — pinning it in
 * pom.xml without being able to verify it builds felt worse than just
 * writing the five assertions this project actually needs. Swapping
 * these tests over to real JUnit on a machine with normal internet
 * access is a mechanical change (annotate methods, replace these calls
 * with org.junit.jupiter.api.Assertions), not a redesign.
 */
public final class TestSupport {

    private TestSupport() {}

    public static void assertTrue(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }

    public static void assertEquals(Object expected, Object actual, String message) {
        if (!java.util.Objects.equals(expected, actual)) {
            throw new AssertionError(message + " (expected=" + expected + ", actual=" + actual + ")");
        }
    }

    public static void assertArrayEquals(byte[] expected, byte[] actual, String message) {
        if (!java.util.Arrays.equals(expected, actual)) {
            throw new AssertionError(message + " (expected=" + bytesToString(expected) + ", actual=" + bytesToString(actual) + ")");
        }
    }

    public static void assertNull(Object value, String message) {
        if (value != null) throw new AssertionError(message + " (was=" + value + ")");
    }

    private static String bytesToString(byte[] b) {
        return b == null ? "null" : new String(b);
    }
}
