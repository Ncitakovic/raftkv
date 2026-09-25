package io.github.ncitakovic.raftkv.log;

/** A command together with its position in the log. */
public record LogEntry(long index, Command command) {

    public LogEntry {
        if (index < 1) {
            throw new IllegalArgumentException("Log indexes start at 1, got " + index);
        }
    }
}
