package io.github.ncitakovic.raftkv.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PersistentStateTest {

    @TempDir
    Path dir;

    @Test
    void freshNodeStartsAtTermZeroWithoutVote() throws IOException {
        PersistentState state = PersistentState.open(dir);
        assertEquals(0, state.currentTerm());
        assertTrue(state.votedFor().isEmpty());
    }

    @Test
    void voteSurvivesRestart() throws IOException {
        PersistentState state = PersistentState.open(dir);
        state.advanceTerm(5);
        state.voteFor("B");

        PersistentState restarted = PersistentState.open(dir);
        assertEquals(5, restarted.currentTerm());
        assertEquals(Optional.of("B"), restarted.votedFor());
    }

    /** The scenario from the design discussion: A votes for B, restarts, then C asks for A's vote. */
    @Test
    void restartedNodeCannotVoteTwiceInTheSameTerm() throws IOException {
        PersistentState a = PersistentState.open(dir);
        a.advanceTerm(5);
        a.voteFor("B");

        PersistentState aAfterRestart = PersistentState.open(dir);
        assertThrows(IllegalStateException.class, () -> aAfterRestart.voteFor("C"));
        assertEquals(Optional.of("B"), aAfterRestart.votedFor());
    }

    @Test
    void votingAgainForTheSameCandidateIsAllowed() throws IOException {
        PersistentState state = PersistentState.open(dir);
        state.advanceTerm(1);
        state.voteFor("B");
        state.voteFor("B"); // a retried RequestVote must get the same answer
        assertEquals(Optional.of("B"), state.votedFor());
    }

    @Test
    void newTermClearsTheVote() throws IOException {
        PersistentState state = PersistentState.open(dir);
        state.advanceTerm(1);
        state.voteFor("B");
        state.advanceTerm(2);
        assertTrue(state.votedFor().isEmpty());

        state.voteFor("C"); // a fresh term allows a fresh vote
        assertEquals(Optional.of("C"), PersistentState.open(dir).votedFor());
    }

    @Test
    void termNeverGoesBackwards() throws IOException {
        PersistentState state = PersistentState.open(dir);
        state.advanceTerm(3);
        assertThrows(IllegalArgumentException.class, () -> state.advanceTerm(3));
        assertThrows(IllegalArgumentException.class, () -> state.advanceTerm(2));
        assertEquals(3, state.currentTerm());
    }

    @Test
    void startElectionIncrementsTermAndVotesForSelfAtomically() throws IOException {
        PersistentState state = PersistentState.open(dir);
        state.advanceTerm(4);
        assertEquals(5, state.startElection("A"));

        PersistentState restarted = PersistentState.open(dir);
        assertEquals(5, restarted.currentTerm());
        assertEquals(Optional.of("A"), restarted.votedFor());
    }

    @Test
    void noTempFileIsLeftBehindAfterAWrite() throws IOException {
        PersistentState state = PersistentState.open(dir);
        state.startElection("A");
        assertTrue(Files.exists(dir.resolve(PersistentState.FILE_NAME)));
        assertFalse(Files.exists(dir.resolve(PersistentState.TMP_FILE_NAME)));
    }

    /** Simulates a crash after writing the temp file but before the rename. */
    @Test
    void unfinishedWriteIsDiscardedAndOldStateWins() throws IOException {
        PersistentState state = PersistentState.open(dir);
        state.advanceTerm(7);
        state.voteFor("B");
        Files.write(dir.resolve(PersistentState.TMP_FILE_NAME), PersistentState.encode(8, "C"));

        PersistentState restarted = PersistentState.open(dir);
        assertEquals(7, restarted.currentTerm());
        assertEquals(Optional.of("B"), restarted.votedFor());
        assertFalse(Files.exists(dir.resolve(PersistentState.TMP_FILE_NAME)));
    }

    @Test
    void corruptedStateFileStopsTheNodeInsteadOfResetting() throws IOException {
        PersistentState state = PersistentState.open(dir);
        state.advanceTerm(9);
        state.voteFor("B");
        Path file = dir.resolve(PersistentState.FILE_NAME);
        byte[] bytes = Files.readAllBytes(file);
        bytes[6] = (byte) ~bytes[6]; // flip a byte inside the term
        Files.write(file, bytes);

        assertThrows(IOException.class, () -> PersistentState.open(dir));
    }

    @Test
    void truncatedStateFileStopsTheNode() throws IOException {
        PersistentState.open(dir).advanceTerm(2);
        Path file = dir.resolve(PersistentState.FILE_NAME);
        Files.write(file, new byte[] {1, 2, 3});

        assertThrows(IOException.class, () -> PersistentState.open(dir));
    }

    @Test
    void candidateIdsWithNonAsciiCharactersRoundTrip() throws IOException {
        PersistentState state = PersistentState.open(dir);
        state.startElection("čvor-Č");
        assertEquals(Optional.of("čvor-Č"), PersistentState.open(dir).votedFor());
    }
}
