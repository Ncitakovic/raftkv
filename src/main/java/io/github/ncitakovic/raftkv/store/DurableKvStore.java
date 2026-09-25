package io.github.ncitakovic.raftkv.store;

import io.github.ncitakovic.raftkv.log.Command;
import io.github.ncitakovic.raftkv.log.LogEntry;
import io.github.ncitakovic.raftkv.log.WriteAheadLog;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;

/**
 * Single-node durable key-value store (milestone 1).
 *
 * <p>Every write follows the same path a Raft leader will use: append to the log and fsync, then
 * apply to the state machine, then acknowledge. Reads are served from the state machine. On
 * restart, the log is replayed to rebuild state.
 *
 * <p>In milestone 3, {@link #write(Command)} is replaced by "append locally, replicate to a
 * majority, advance the commit index, apply" — the state machine and log format stay the same.
 */
public final class DurableKvStore implements Closeable {

    /** Result of a write: its log index and, for deletes, whether the key existed. */
    public record WriteResult(long index, boolean existed) {}

    private final WriteAheadLog wal;
    private final KvStateMachine stateMachine = new KvStateMachine();

    private DurableKvStore(WriteAheadLog wal) {
        this.wal = wal;
        for (LogEntry entry : wal.recoveredEntries()) {
            stateMachine.apply(entry);
        }
    }

    public static DurableKvStore open(Path dataDir) throws IOException {
        return new DurableKvStore(WriteAheadLog.open(dataDir.resolve("raftkv.log")));
    }

    public WriteResult put(String key, byte[] value) throws IOException {
        return write(Command.put(key, value));
    }

    public WriteResult delete(String key) throws IOException {
        return write(Command.delete(key));
    }

    public Optional<byte[]> get(String key) {
        return stateMachine.get(key);
    }

    public long lastIndex() {
        return wal.lastIndex();
    }

    public int size() {
        return stateMachine.size();
    }

    private synchronized WriteResult write(Command command) throws IOException {
        long index = wal.append(command);
        boolean existed = stateMachine.apply(new LogEntry(index, command));
        return new WriteResult(index, existed);
    }

    @Override
    public synchronized void close() throws IOException {
        wal.close();
    }
}
