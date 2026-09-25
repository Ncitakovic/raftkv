package io.github.ncitakovic.raftkv.store;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DurableKvStoreTest {

    @TempDir
    Path dir;

    @Test
    void stateSurvivesRestart() throws IOException {
        try (DurableKvStore store = DurableKvStore.open(dir)) {
            store.put("a", bytes("1"));
            store.put("b", bytes("2"));
            store.put("a", bytes("3"));
            store.delete("b");
        }
        try (DurableKvStore store = DurableKvStore.open(dir)) {
            assertArrayEquals(bytes("3"), store.get("a").orElseThrow());
            assertTrue(store.get("b").isEmpty());
            assertEquals(1, store.size());
            assertEquals(4, store.lastIndex());
        }
    }

    @Test
    void writeResultsReportIndexAndExistence() throws IOException {
        try (DurableKvStore store = DurableKvStore.open(dir)) {
            assertEquals(new DurableKvStore.WriteResult(1, false), store.put("k", bytes("v")));
            assertEquals(new DurableKvStore.WriteResult(2, true), store.delete("k"));
            assertEquals(new DurableKvStore.WriteResult(3, false), store.delete("k"));
        }
    }

    @Test
    void concurrentWritersGetUniqueContiguousIndexes() throws Exception {
        int writers = 8;
        int writesEach = 50;
        try (DurableKvStore store = DurableKvStore.open(dir)) {
            ExecutorService pool = Executors.newFixedThreadPool(writers);
            List<Future<List<Long>>> futures = new ArrayList<>();
            for (int w = 0; w < writers; w++) {
                int id = w;
                futures.add(pool.submit(() -> {
                    List<Long> indexes = new ArrayList<>();
                    for (int i = 0; i < writesEach; i++) {
                        indexes.add(store.put("w" + id + "-" + i, bytes(Integer.toString(i))).index());
                    }
                    return indexes;
                }));
            }
            boolean[] seen = new boolean[writers * writesEach + 1];
            for (Future<List<Long>> f : futures) {
                for (long index : f.get()) {
                    assertFalse(seen[(int) index], "duplicate index " + index);
                    seen[(int) index] = true;
                }
            }
            pool.shutdown();
            assertEquals(writers * writesEach, store.lastIndex());
            assertEquals(writers * writesEach, store.size());
        }
        try (DurableKvStore store = DurableKvStore.open(dir)) {
            assertEquals(writers * writesEach, store.size());
        }
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
