package io.github.ncitakovic.raftkv.raft;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import java.util.Optional;
import java.util.zip.CRC32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Raft state a node must never forget: {@code currentTerm} and {@code votedFor}.
 *
 * <p><b>Why it must be on disk before replying.</b> A node may grant at most one vote per term.
 * If {@code votedFor} lived only in memory, a node could vote for B, restart, forget, and then vote
 * for C in the same term. Both B and C would reach a majority and the cluster would have two
 * leaders. So every change is written and fsync'd <i>before</i> the method returns, and callers
 * must not send an RPC reply until it has.
 *
 * <p><b>How it is written.</b> Unlike the write-ahead log, this file is overwritten, not appended
 * to. Overwriting in place is unsafe: a crash halfway through would destroy both the old and the
 * new value. Instead each update:
 *
 * <ol>
 *   <li>writes the new state to {@code raft-state.tmp} and fsyncs it,
 *   <li>atomically renames it over {@code raft-state} (all-or-nothing at the file-system level),
 *   <li>fsyncs the directory so the rename itself survives a crash (where the OS supports it).
 * </ol>
 *
 * <p>At any moment the disk holds either the complete old state or the complete new state.
 *
 * <p><b>Corruption fails loudly.</b> The write-ahead log may drop a torn record at its tail,
 * because that record was never acknowledged. Here the opposite is true: silently resetting to
 * "term 0, no vote" could let the node vote twice. A file that fails its checksum therefore stops
 * the node with an exception and requires an operator to look at it.
 *
 * <p>Thread-safe: all methods are synchronized.
 */
public final class PersistentState implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(PersistentState.class);

    static final String FILE_NAME = "raft-state";
    static final String TMP_FILE_NAME = "raft-state.tmp";
    private static final int MAGIC = 0x52414654; // "RAFT"
    private static final byte VERSION = 1;

    private final Path file;
    private final Path tmpFile;
    private long currentTerm;
    private String votedFor; // null = has not voted in currentTerm

    private PersistentState(Path dir, long currentTerm, String votedFor) {
        this.file = dir.resolve(FILE_NAME);
        this.tmpFile = dir.resolve(TMP_FILE_NAME);
        this.currentTerm = currentTerm;
        this.votedFor = votedFor;
    }

    /**
     * Loads the state from {@code dir}, or starts at term 0 with no vote if the node has never run.
     *
     * @throws IOException if the state file exists but is corrupt or unreadable
     */
    public static PersistentState open(Path dir) throws IOException {
        Files.createDirectories(dir);
        Path file = dir.resolve(FILE_NAME);
        Path tmp = dir.resolve(TMP_FILE_NAME);

        // A leftover temp file means a crash happened before the rename. The rename never took
        // effect, so the old file is still the truth and the temp file can be discarded.
        if (Files.deleteIfExists(tmp)) {
            log.warn("Discarded unfinished state write {}", tmp);
        }

        if (!Files.exists(file)) {
            log.info("No Raft state in {}, starting at term 0", dir);
            return new PersistentState(dir, 0, null);
        }
        PersistentState state = decode(dir, Files.readAllBytes(file));
        log.info("Loaded Raft state: term {}, votedFor {}", state.currentTerm, state.votedFor);
        return state;
    }

    public synchronized long currentTerm() {
        return currentTerm;
    }

    public synchronized Optional<String> votedFor() {
        return Optional.ofNullable(votedFor);
    }

    /**
     * Moves to a newer term, for example after seeing a higher term in an RPC. The vote is cleared,
     * because a vote only counts within its own term.
     *
     * @throws IllegalArgumentException if {@code newTerm} is not greater than the current term
     */
    public synchronized void advanceTerm(long newTerm) throws IOException {
        if (newTerm <= currentTerm) {
            throw new IllegalArgumentException(
                    "Term must increase: current " + currentTerm + ", requested " + newTerm);
        }
        persist(newTerm, null);
    }

    /**
     * Records a vote for {@code candidateId} in the current term. Voting again for the same
     * candidate is allowed (a retried RPC); voting for a different one is not.
     *
     * @throws IllegalStateException if this node already voted for someone else in this term
     */
    public synchronized void voteFor(String candidateId) throws IOException {
        Objects.requireNonNull(candidateId, "candidateId");
        if (candidateId.equals(votedFor)) {
            return;
        }
        if (votedFor != null) {
            throw new IllegalStateException("Already voted for " + votedFor + " in term " + currentTerm
                    + ", cannot vote for " + candidateId);
        }
        persist(currentTerm, candidateId);
    }

    /**
     * Starts an election: increments the term and votes for {@code selfId}, in a single atomic
     * write, so a crash can never leave the node in the new term without its own vote recorded.
     *
     * @return the new term
     */
    public synchronized long startElection(String selfId) throws IOException {
        Objects.requireNonNull(selfId, "selfId");
        persist(currentTerm + 1, selfId);
        return currentTerm;
    }

    @Override
    public void close() {
        // Nothing to release: files are opened and closed on every write.
    }

    // ---- writing ----

    private void persist(long term, String vote) throws IOException {
        byte[] bytes = encode(term, vote);
        try (FileChannel ch = FileChannel.open(tmpFile,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
            ByteBuffer buf = ByteBuffer.wrap(bytes);
            while (buf.hasRemaining()) {
                ch.write(buf);
            }
            ch.force(true);
        }
        try {
            Files.move(tmpFile, file, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException e) {
            throw new IOException("File system does not support atomic rename: " + file.getParent(), e);
        }
        fsyncDirectory(file.getParent());
        // Update memory only after the disk write succeeded, so memory never runs ahead of disk.
        currentTerm = term;
        votedFor = vote;
    }

    /**
     * Makes the rename durable. On Linux a rename lives in the directory entry, which has its own
     * fsync. Windows does not allow opening a directory this way; there NTFS journals the rename.
     */
    private static void fsyncDirectory(Path dir) {
        try (FileChannel ch = FileChannel.open(dir, StandardOpenOption.READ)) {
            ch.force(true);
        } catch (IOException e) {
            log.debug("Directory fsync not supported on this platform: {}", e.toString());
        }
    }

    // ---- file format ----
    // [magic:int][version:byte][term:long][voteLen:int, -1 = none][vote:UTF-8 bytes][crc32:int]
    // The CRC covers every byte before it.

    static byte[] encode(long term, String vote) {
        byte[] voteBytes = vote == null ? new byte[0] : vote.getBytes(StandardCharsets.UTF_8);
        int voteLen = vote == null ? -1 : voteBytes.length;
        ByteBuffer buf = ByteBuffer.allocate(Integer.BYTES + 1 + Long.BYTES + Integer.BYTES
                + voteBytes.length + Integer.BYTES);
        buf.putInt(MAGIC).put(VERSION).putLong(term).putInt(voteLen).put(voteBytes);
        CRC32 crc = new CRC32();
        crc.update(buf.array(), 0, buf.position());
        buf.putInt((int) crc.getValue());
        return buf.array();
    }

    private static PersistentState decode(Path dir, byte[] bytes) throws IOException {
        Path file = dir.resolve(FILE_NAME);
        int minSize = Integer.BYTES + 1 + Long.BYTES + Integer.BYTES + Integer.BYTES;
        if (bytes.length < minSize) {
            throw corrupt(file, "file is too short (" + bytes.length + " bytes)");
        }
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        CRC32 crc = new CRC32();
        crc.update(bytes, 0, bytes.length - Integer.BYTES);
        int storedCrc = ByteBuffer.wrap(bytes, bytes.length - Integer.BYTES, Integer.BYTES).getInt();
        if ((int) crc.getValue() != storedCrc) {
            throw corrupt(file, "checksum mismatch");
        }
        if (buf.getInt() != MAGIC) {
            throw corrupt(file, "not a RaftKV state file");
        }
        byte version = buf.get();
        if (version != VERSION) {
            throw corrupt(file, "unsupported version " + version);
        }
        long term = buf.getLong();
        int voteLen = buf.getInt();
        int expectedVoteBytes = voteLen == -1 ? 0 : voteLen;
        if (voteLen < -1 || expectedVoteBytes != bytes.length - minSize) {
            throw corrupt(file, "vote length does not match file size");
        }
        String vote = null;
        if (voteLen >= 0) {
            byte[] voteBytes = new byte[voteLen];
            buf.get(voteBytes);
            vote = new String(voteBytes, StandardCharsets.UTF_8);
        }
        if (term < 0) {
            throw corrupt(file, "negative term " + term);
        }
        return new PersistentState(dir, term, vote);
    }

    private static IOException corrupt(Path file, String reason) {
        return new IOException("Raft state file " + file + " is corrupt (" + reason + "). Refusing to start: "
                + "resetting it could let this node vote twice in the same term.");
    }
}
