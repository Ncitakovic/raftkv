package io.github.ncitakovic.raftkv.log;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.zip.CRC32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Append-only, checksummed, fsync'd log of commands on local disk.
 *
 * <p>On-disk record format (big-endian):
 *
 * <pre>
 * +-------------+-------------+-----------------------------------------------------+
 * | length: int | crc32: int  | payload: index(long) type(byte) keyLen(int) key     |
 * |             | of payload  |          valueLen(int) value                         |
 * +-------------+-------------+-----------------------------------------------------+
 * </pre>
 *
 * <p>A crash can leave a partially written ("torn") record at the end of the file. On open, the
 * log replays records until the first one that is incomplete or fails its checksum, then truncates
 * the file at that point. Anything before it was fsync'd and acknowledged; anything after it was
 * never acknowledged to a client, so dropping it is safe.
 *
 * <p>This class is not thread-safe; callers serialize appends.
 */
public final class WriteAheadLog implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(WriteAheadLog.class);
    private static final int HEADER_BYTES = Integer.BYTES * 2;
    /** Upper bound on a single record, to reject garbage lengths from a corrupted file. */
    static final int MAX_RECORD_BYTES = 16 * 1024 * 1024;

    private final Path file;
    private final FileChannel channel;
    private final List<LogEntry> recovered;
    private long lastIndex;

    private WriteAheadLog(Path file, FileChannel channel, List<LogEntry> recovered) {
        this.file = file;
        this.channel = channel;
        this.recovered = Collections.unmodifiableList(recovered);
        this.lastIndex = recovered.isEmpty() ? 0 : recovered.get(recovered.size() - 1).index();
    }

    /** Opens (or creates) the log at {@code file}, recovering every intact record. */
    public static WriteAheadLog open(Path file) throws IOException {
        Path parent = file.toAbsolutePath().getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        FileChannel channel = FileChannel.open(file,
                StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);

        List<LogEntry> entries = new ArrayList<>();
        long validEnd = replay(channel, entries);
        long size = channel.size();
        if (validEnd < size) {
            log.warn("Truncating {} bytes of torn or corrupt data at the end of {}", size - validEnd, file);
            channel.truncate(validEnd);
            channel.force(true);
        }
        channel.position(validEnd);
        log.info("Opened log {} with {} entries", file, entries.size());
        return new WriteAheadLog(file, channel, entries);
    }

    /** Entries recovered from disk when the log was opened, in index order. */
    public List<LogEntry> recoveredEntries() {
        return recovered;
    }

    /** Index of the last entry in the log, or 0 if the log is empty. */
    public long lastIndex() {
        return lastIndex;
    }

    /**
     * Appends {@code command} as the next entry and forces it to disk before returning.
     *
     * @return the new entry's index
     */
    public long append(Command command) throws IOException {
        long index = lastIndex + 1;
        byte[] payload = encode(new LogEntry(index, command));
        if (payload.length > MAX_RECORD_BYTES) {
            throw new IllegalArgumentException("Record too large: " + payload.length + " bytes");
        }
        CRC32 crc = new CRC32();
        crc.update(payload);

        ByteBuffer record = ByteBuffer.allocate(HEADER_BYTES + payload.length);
        record.putInt(payload.length).putInt((int) crc.getValue()).put(payload).flip();
        while (record.hasRemaining()) {
            channel.write(record);
        }
        channel.force(false);
        lastIndex = index;
        return index;
    }

    public Path file() {
        return file;
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }

    /** Reads records into {@code out}; returns the byte offset just past the last intact record. */
    private static long replay(FileChannel channel, List<LogEntry> out) throws IOException {
        long position = 0;
        long size = channel.size();
        ByteBuffer header = ByteBuffer.allocate(HEADER_BYTES);
        while (position + HEADER_BYTES <= size) {
            header.clear();
            readFully(channel, header, position);
            header.flip();
            int length = header.getInt();
            int expectedCrc = header.getInt();
            if (length <= 0 || length > MAX_RECORD_BYTES || position + HEADER_BYTES + length > size) {
                break;
            }
            ByteBuffer payload = ByteBuffer.allocate(length);
            readFully(channel, payload, position + HEADER_BYTES);
            CRC32 crc = new CRC32();
            crc.update(payload.array());
            if ((int) crc.getValue() != expectedCrc) {
                break;
            }
            LogEntry entry = decode(payload.array());
            long expectedIndex = out.isEmpty() ? 1 : out.get(out.size() - 1).index() + 1;
            if (entry.index() != expectedIndex) {
                throw new IOException("Log index gap: expected " + expectedIndex + " but found " + entry.index());
            }
            out.add(entry);
            position += HEADER_BYTES + length;
        }
        return position;
    }

    private static void readFully(FileChannel channel, ByteBuffer buffer, long position) throws IOException {
        long p = position;
        while (buffer.hasRemaining()) {
            int n = channel.read(buffer, p);
            if (n < 0) {
                throw new IOException("Unexpected end of log file");
            }
            p += n;
        }
    }

    static byte[] encode(LogEntry entry) {
        Command c = entry.command();
        byte[] key = c.key().getBytes(StandardCharsets.UTF_8);
        byte[] value = c.value();
        ByteBuffer buf = ByteBuffer.allocate(Long.BYTES + 1 + Integer.BYTES + key.length + Integer.BYTES + value.length);
        buf.putLong(entry.index()).put(c.type().code()).putInt(key.length).put(key).putInt(value.length).put(value);
        return buf.array();
    }

    static LogEntry decode(byte[] payload) {
        ByteBuffer buf = ByteBuffer.wrap(payload);
        long index = buf.getLong();
        Command.Type type = Command.Type.fromCode(buf.get());
        byte[] key = new byte[buf.getInt()];
        buf.get(key);
        byte[] value = new byte[buf.getInt()];
        buf.get(value);
        return new LogEntry(index, new Command(type, new String(key, StandardCharsets.UTF_8), value));
    }
}
