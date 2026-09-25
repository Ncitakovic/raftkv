package io.github.ncitakovic.raftkv.store;

import io.github.ncitakovic.raftkv.log.Command;
import io.github.ncitakovic.raftkv.log.LogEntry;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Deterministic key-value state machine.
 *
 * <p>Given the same sequence of log entries, every replica ends up in the same state. That property
 * is what Raft relies on: consensus agrees on the order of commands, and each node applies them to
 * its own copy of this class.
 */
public final class KvStateMachine {

    private final ConcurrentHashMap<String, byte[]> data = new ConcurrentHashMap<>();
    private volatile long lastApplied;

    /**
     * Applies {@code entry}. Entries must be applied in index order, exactly once.
     *
     * @return true if the entry changed an existing key (for DELETE: the key existed)
     */
    public synchronized boolean apply(LogEntry entry) {
        if (entry.index() != lastApplied + 1) {
            throw new IllegalStateException(
                    "Out-of-order apply: last applied " + lastApplied + ", got " + entry.index());
        }
        Command c = entry.command();
        boolean existed = switch (c.type()) {
            case PUT -> data.put(c.key(), c.value()) != null;
            case DELETE -> data.remove(c.key()) != null;
        };
        lastApplied = entry.index();
        return existed;
    }

    public Optional<byte[]> get(String key) {
        byte[] value = data.get(key);
        return value == null ? Optional.empty() : Optional.of(value.clone());
    }

    public long lastApplied() {
        return lastApplied;
    }

    public int size() {
        return data.size();
    }
}
