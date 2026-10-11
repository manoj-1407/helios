package dev.helios.core;

import java.util.NavigableMap;

import static dev.helios.TestSupport.*;

public final class MemTableTest {

    public static void main(String[] args) {
        testInsertAndRead();
        testTombstoneAndSnapshotTranslation();
        System.out.println("MemTableTest: all assertions passed");
    }

    private static void testInsertAndRead() {
        MemTable mt = new MemTable(1024 * 1024);
        for (int i = 0; i < 1000; i++) {
            mt.put(("key" + i).getBytes(), ("value" + i).getBytes());
        }
        assertArrayEquals("value500".getBytes(), mt.get("key500".getBytes()), "basic put/get round trip");
        assertEquals(1000, mt.entryCount(), "entry count after 1000 distinct puts");
    }

    private static void testTombstoneAndSnapshotTranslation() {
        MemTable mt = new MemTable(1024 * 1024);
        mt.put("k".getBytes(), "v".getBytes());
        mt.delete("k".getBytes());

        assertTrue(mt.containsKey("k".getBytes()), "deleted key must still be present as a tombstone, not removed");
        assertNull(mt.get("k".getBytes()), "get() on a tombstoned key must return null");

        // This is the bug that the first version of this class had: storing
        // the internal TOMBSTONE sentinel directly in a ConcurrentSkipListMap
        // value is fine, but if snapshot() ever leaked that sentinel instead
        // of translating it back to null, SSTableWriter would silently write
        // an empty-value record instead of a delete marker -- permanently
        // undoing the delete the moment this MemTable got flushed.
        NavigableMap<byte[], byte[]> snap = mt.snapshot();
        assertTrue(snap.containsKey("k".getBytes()), "snapshot must still contain the key");
        assertNull(snap.get("k".getBytes()), "snapshot must translate the internal sentinel back to real null");
    }
}
