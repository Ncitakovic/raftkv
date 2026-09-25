package io.github.ncitakovic.raftkv.store;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.ncitakovic.raftkv.log.Command;
import io.github.ncitakovic.raftkv.log.LogEntry;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;

class KvStateMachineTest {

    @Test
    void putGetDelete() {
        KvStateMachine sm = new KvStateMachine();
        assertFalse(sm.apply(new LogEntry(1, Command.put("k", bytes("v1")))));
        assertTrue(sm.apply(new LogEntry(2, Command.put("k", bytes("v2")))));
        assertArrayEquals(bytes("v2"), sm.get("k").orElseThrow());

        assertTrue(sm.apply(new LogEntry(3, Command.delete("k"))));
        assertTrue(sm.get("k").isEmpty());
        assertFalse(sm.apply(new LogEntry(4, Command.delete("k"))));
        assertEquals(4, sm.lastApplied());
    }

    @Test
    void rejectsOutOfOrderOrDuplicateEntries() {
        KvStateMachine sm = new KvStateMachine();
        sm.apply(new LogEntry(1, Command.put("a", bytes("1"))));
        assertThrows(IllegalStateException.class, () -> sm.apply(new LogEntry(1, Command.put("a", bytes("1")))));
        assertThrows(IllegalStateException.class, () -> sm.apply(new LogEntry(3, Command.put("a", bytes("1")))));
    }

    @Test
    void sameEntriesProduceSameStateOnEveryReplica() {
        List<LogEntry> log = List.of(
                new LogEntry(1, Command.put("x", bytes("1"))),
                new LogEntry(2, Command.put("y", bytes("2"))),
                new LogEntry(3, Command.delete("x")),
                new LogEntry(4, Command.put("z", bytes("3"))));
        KvStateMachine a = new KvStateMachine();
        KvStateMachine b = new KvStateMachine();
        log.forEach(a::apply);
        log.forEach(b::apply);

        for (String key : List.of("x", "y", "z")) {
            assertEquals(a.get(key).isPresent(), b.get(key).isPresent());
            a.get(key).ifPresent(v -> assertArrayEquals(v, b.get(key).orElseThrow()));
        }
        assertEquals(a.size(), b.size());
    }

    @Test
    void returnedValuesCannotMutateState() {
        KvStateMachine sm = new KvStateMachine();
        byte[] original = bytes("value");
        sm.apply(new LogEntry(1, Command.put("k", original)));
        original[0] = 'X';
        sm.get("k").orElseThrow()[1] = 'Y';
        assertArrayEquals(bytes("value"), sm.get("k").orElseThrow());
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
