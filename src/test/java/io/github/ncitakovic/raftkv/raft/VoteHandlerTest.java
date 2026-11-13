package io.github.ncitakovic.raftkv.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.ncitakovic.raftkv.api.v1.RequestVoteRequest;
import io.github.ncitakovic.raftkv.api.v1.RequestVoteResponse;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class VoteHandlerTest {

    @TempDir
    Path dir;

    private PersistentState state;

    @BeforeEach
    void setUp() throws IOException {
        state = PersistentState.open(dir);
    }

    /** A handler whose own log ends at {@code myLastIndex} with an entry from {@code myLastTerm}. */
    private VoteHandler handler(long myLastIndex, long myLastTerm) {
        return new VoteHandler(state, () -> myLastIndex, () -> myLastTerm);
    }

    private static RequestVoteRequest request(long term, String candidate, long lastLogIndex, long lastLogTerm) {
        return RequestVoteRequest.newBuilder()
                .setTerm(term)
                .setCandidateId(candidate)
                .setLastLogIndex(lastLogIndex)
                .setLastLogTerm(lastLogTerm)
                .build();
    }

    // Rule 1
    @Test
    void rejectsCandidateWithLowerTermAndRepliesWithMyTerm() throws IOException {
        state.advanceTerm(5);

        RequestVoteResponse resp = handler(0, 0).handle(request(3, "B", 0, 0));

        assertFalse(resp.getVoteGranted());
        assertEquals(5, resp.getTerm());
        assertEquals(5, state.currentTerm());
        assertTrue(state.votedFor().isEmpty());
    }

    // Rule 2
    @Test
    void higherTermIsAdoptedAndVoteGrantedWhenLogIsUpToDate() throws IOException {
        state.advanceTerm(1);

        RequestVoteResponse resp = handler(0, 0).handle(request(4, "B", 0, 0));

        assertTrue(resp.getVoteGranted());
        assertEquals(4, resp.getTerm());
        assertEquals(4, state.currentTerm());
        assertEquals(Optional.of("B"), state.votedFor());
    }

    // Rule 3
    @Test
    void rejectsSecondCandidateInTheSameTerm() throws IOException {
        state.advanceTerm(2);
        state.voteFor("C");

        RequestVoteResponse resp = handler(0, 0).handle(request(2, "B", 0, 0));

        assertFalse(resp.getVoteGranted());
        assertEquals(2, resp.getTerm());
        assertEquals(Optional.of("C"), state.votedFor());
    }

    // Rule 4
    @Test
    void repeatedRequestFromSameCandidateIsGrantedAgain() throws IOException {
        state.advanceTerm(2);
        state.voteFor("B");

        RequestVoteResponse resp = handler(0, 0).handle(request(2, "B", 0, 0));

        assertTrue(resp.getVoteGranted());
        assertEquals(2, resp.getTerm());
        assertEquals(Optional.of("B"), state.votedFor());
    }

    // Rule 5
    @Test
    void rejectsCandidateWhoseLastLogTermIsLowerEvenWithLongerLog() throws IOException {
        state.advanceTerm(3);
        VoteHandler handler = handler(5, 3); // my log: 5 entries, last one from term 3

        RequestVoteResponse resp = handler.handle(request(4, "B", 10, 2));

        assertFalse(resp.getVoteGranted());
        // The higher term is still adopted (rule 2), even though the vote is refused.
        assertEquals(4, resp.getTerm());
        assertEquals(4, state.currentTerm());
        assertTrue(state.votedFor().isEmpty());
    }

    // Rule 6
    @Test
    void rejectsCandidateWithSameLastLogTermButShorterLog() throws IOException {
        state.advanceTerm(3);

        RequestVoteResponse resp = handler(5, 3).handle(request(4, "B", 4, 3));

        assertFalse(resp.getVoteGranted());
        assertTrue(state.votedFor().isEmpty());
    }

    // Rule 7
    @Test
    void grantsCandidateWithSameLastLogTermAndEqualIndex() throws IOException {
        state.advanceTerm(3);

        RequestVoteResponse resp = handler(5, 3).handle(request(4, "B", 5, 3));

        assertTrue(resp.getVoteGranted());
        assertEquals(Optional.of("B"), state.votedFor());
    }

    // Rule 8
    @Test
    void grantedVoteSurvivesRestart() throws IOException {
        state.advanceTerm(1);
        assertTrue(handler(0, 0).handle(request(2, "B", 0, 0)).getVoteGranted());

        PersistentState restarted = PersistentState.open(dir);

        assertEquals(2, restarted.currentTerm());
        assertEquals(Optional.of("B"), restarted.votedFor());
    }

    // Input validation: proto3 turns a missing candidate_id into "", which must not consume our vote.
    @Test
    void rejectsEmptyCandidateIdWithoutChangingState() throws IOException {
        state.advanceTerm(2);

        // A higher term would normally be adopted, so this also checks that validation runs first.
        RequestVoteResponse resp = handler(0, 0).handle(request(5, "", 0, 0));

        assertFalse(resp.getVoteGranted());
        assertEquals(2, resp.getTerm());
        assertEquals(2, state.currentTerm());
        assertTrue(state.votedFor().isEmpty());
    }

    // Input validation: term 0 is never an election term (the first election is term 1).
    @Test
    void rejectsTermZero() throws IOException {
        RequestVoteResponse resp = handler(0, 0).handle(request(0, "B", 0, 0));

        assertFalse(resp.getVoteGranted());
        assertEquals(0, state.currentTerm());
        assertTrue(state.votedFor().isEmpty());
    }
}
