package io.github.ncitakovic.raftkv.log;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class WriteAheadLogTest {

    @TempDir
    Path dir;

    private Path file() {
        return dir.resolve("test.log");
    }

    @Test
    void emptyLogStartsAtIndexZero() throws IOException {
        try (WriteAheadLog wal = WriteAheadLog.open(file())) {
            assertEquals(0, wal.lastIndex());
            assertTrue(wal.recoveredEntries().isEmpty());
        }
    }

    @Test
    void appendAssignsConsecutiveIndexes() throws IOException {
        try (WriteAheadLog wal = WriteAheadLog.open(file())) {
            assertEquals(1, wal.append(Command.put("a", bytes("1"))));
            assertEquals(2, wal.append(Command.put("b", bytes("2"))));
            assertEquals(3, wal.append(Command.delete("a")));
            assertEquals(3, wal.lastIndex());
        }
    }

    @Test
    void reopenRecoversAllEntriesInOrder() throws IOException {
        try (WriteAheadLog wal = WriteAheadLog.open(file())) {
            wal.append(Command.put("a", bytes("1")));
            wal.append(Command.put("b", bytes("2")));
            wal.append(Command.delete("a"));
        }
        try (WriteAheadLog wal = WriteAheadLog.open(file())) {
            List<LogEntry> entries = wal.recoveredEntries();
            assertEquals(3, entries.size());
            assertEquals(new LogEntry(1, Command.put("a", bytes("1"))), entries.get(0));
            assertEquals(new LogEntry(2, Command.put("b", bytes("2"))), entries.get(1));
            assertEquals(new LogEntry(3, Command.delete("a")), entries.get(2));
            assertEquals(4, wal.append(Command.put("c", bytes("3"))));
        }
    }

    @Test
    void tornTailIsTruncatedAndLogStaysUsable() throws IOException {
        try (WriteAheadLog wal = WriteAheadLog.open(file())) {
            wal.append(Command.put("a", bytes("1")));
            wal.append(Command.put("b", bytes("2")));
        }
        long intactSize = Files.size(file());
        // Simulate a crash mid-write: a header promising 100 bytes, followed by only 3.
        try (RandomAccessFile raf = new RandomAccessFile(file().toFile(), "rw")) {
            raf.seek(raf.length());
            raf.writeInt(100);
            raf.writeInt(0);
            raf.write(new byte[] {1, 2, 3});
        }

        try (WriteAheadLog wal = WriteAheadLog.open(file())) {
            assertEquals(2, wal.recoveredEntries().size());
            assertEquals(intactSize, Files.size(file()));
            assertEquals(3, wal.append(Command.put("c", bytes("3"))));
        }
        try (WriteAheadLog wal = WriteAheadLog.open(file())) {
            assertEquals(3, wal.recoveredEntries().size());
        }
    }

    @Test
    void corruptedRecordAndEverythingAfterItIsDropped() throws IOException {
        try (WriteAheadLog wal = WriteAheadLog.open(file())) {
            wal.append(Command.put("a", bytes("1")));
        }
        long firstRecordEnd = Files.size(file());
        try (WriteAheadLog wal = WriteAheadLog.open(file())) {
            wal.append(Command.put("b", bytes("2")));
        }
        // Flip the last byte of the second record's payload so its checksum no longer matches.
        try (RandomAccessFile raf = new RandomAccessFile(file().toFile(), "rw")) {
            raf.seek(raf.length() - 1);
            int b = raf.read();
            raf.seek(raf.length() - 1);
            raf.write(b ^ 0xFF);
        }

        try (WriteAheadLog wal = WriteAheadLog.open(file())) {
            assertEquals(1, wal.recoveredEntries().size());
            assertEquals(firstRecordEnd, Files.size(file()));
        }
    }

    @Test
    void encodeDecodeRoundTripsBinaryValuesAndUnicodeKeys() {
        byte[] value = {0, -1, 127, -128, 42};
        LogEntry entry = new LogEntry(7, Command.put("ključ-🔑", value));
        LogEntry decoded = WriteAheadLog.decode(WriteAheadLog.encode(entry));
        assertEquals(entry, decoded);
        assertArrayEquals(value, decoded.command().value());
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
