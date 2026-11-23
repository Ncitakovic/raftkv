package io.github.ncitakovic.raftkv.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.ncitakovic.raftkv.api.v1.AppendEntriesRequest;
import io.github.ncitakovic.raftkv.api.v1.AppendEntriesResponse;
import io.github.ncitakovic.raftkv.api.v1.RequestVoteRequest;
import io.github.ncitakovic.raftkv.api.v1.RequestVoteResponse;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Leader election on one node, driven by a fake clock. Every election timeout is exactly
 * {@link #TIMEOUT} ms (see {@link FixedRandom}), so "199 ms: still a follower, 200 ms: candidate"
 * can be asserted precisely. Peers are played by the test through {@link InMemoryTransport}.
 */
class RaftNodeTest {

    private static final long TIMEOUT = 200;
    private static final long HEARTBEAT = RaftConfig.defaults().heartbeatInterval().toMillis(); // 50

    @TempDir
    Path dir;

    private Path stateDir;
    private PersistentState state;
    private FakeScheduler scheduler;
    private InMemoryTransport transport;
    private RaftNode node;

    @BeforeEach
    void setUp() throws IOException {
        stateDir = dir.resolve("n1");
        state = PersistentState.open(stateDir);
        scheduler = new FakeScheduler();
        transport = new InMemoryTransport();
    }

    // ---- helpers ----

    /** Node "n1" with an empty log, started at time 0. */
    private void startNode(List<String> peers) {
        startNode(peers, 0, 0);
    }

    private void startNode(List<String> peers, long lastIndex, long lastTerm) {
        VoteHandler votes = new VoteHandler(state, () -> lastIndex, () -> lastTerm);
        node = new RaftNode("n1", peers, state, votes, () -> lastIndex, () -> lastTerm, transport, scheduler,
                new FixedRandom(TIMEOUT), RaftConfig.defaults());
        node.start();
        scheduler.runPending();
    }

    /** Starts a 3-node cluster member and lets it time out into its first election (term 1). */
    private void startCandidate() {
        startNode(List.of("n2", "n3"));
        scheduler.advance(TIMEOUT);
        assertEquals(Role.CANDIDATE, node.status().role());
    }

    /** Starts a 3-node cluster member and makes it leader of term 1 with n2's vote. */
    private void startLeader() {
        startCandidate();
        vote("n2", 1, true);
        assertEquals(Role.LEADER, node.status().role());
    }

    /** Peer {@code peer} answers this node's latest RequestVote to it. */
    private void vote(String peer, long term, boolean granted) {
        transport.lastRequestVoteTo(peer).reply()
                .complete(RequestVoteResponse.newBuilder().setTerm(term).setVoteGranted(granted).build());
        scheduler.runPending();
    }

    private AppendEntriesResponse heartbeatFrom(String leader, long term) throws Exception {
        CompletableFuture<AppendEntriesResponse> reply = node.onAppendEntries(
                AppendEntriesRequest.newBuilder().setTerm(term).setLeaderId(leader).build());
        scheduler.runPending();
        return reply.get();
    }

    private RequestVoteResponse requestVoteFrom(String candidate, long term, long lastIndex, long lastTerm)
            throws Exception {
        CompletableFuture<RequestVoteResponse> reply = node.onRequestVote(RequestVoteRequest.newBuilder()
                .setTerm(term).setCandidateId(candidate).setLastLogIndex(lastIndex).setLastLogTerm(lastTerm).build());
        scheduler.runPending();
        return reply.get();
    }

    private long requestVotesInTerm(long term) {
        return transport.requestVotes().stream().filter(s -> s.request().getTerm() == term).count();
    }

    // ---- follower → candidate ----

    @Test
    void followerBecomesCandidateAfterElectionTimeoutAndTermGrowsByOne() {
        startNode(List.of("n2", "n3"));
        assertEquals(new RaftStatus(Role.FOLLOWER, 0, Optional.empty()), node.status());

        scheduler.advance(TIMEOUT - 1);
        assertEquals(Role.FOLLOWER, node.status().role());
        assertTrue(transport.requestVotes().isEmpty());

        scheduler.advance(1);
        assertEquals(new RaftStatus(Role.CANDIDATE, 1, Optional.empty()), node.status());
        assertEquals(1, state.currentTerm());
        assertEquals(Optional.of("n1"), state.votedFor(), "a candidate votes for itself");
        assertEquals(List.of("n2", "n3"), transport.requestVotes().stream().map(s -> s.peerId()).toList());
        for (var sent : transport.requestVotes()) {
            assertEquals(1, sent.request().getTerm());
            assertEquals("n1", sent.request().getCandidateId());
        }
    }

    @Test
    void requestVoteCarriesLastLogIndexAndTermFromTheSuppliers() {
        startNode(List.of("n2", "n3"), 7, 3);
        scheduler.advance(TIMEOUT);

        RequestVoteRequest sent = transport.lastRequestVoteTo("n2").request();
        assertEquals(7, sent.getLastLogIndex());
        assertEquals(3, sent.getLastLogTerm());
    }

    // ---- candidate ----

    @Test
    void candidateWithMajorityBecomesLeaderAndSendsHeartbeats() {
        startCandidate();
        vote("n2", 1, true); // 2 of 3, counting its own vote

        assertEquals(new RaftStatus(Role.LEADER, 1, Optional.of("n1")), node.status());
        // Heartbeats to every peer at once...
        assertEquals(List.of("n2", "n3"), transport.appendEntries().stream().map(s -> s.peerId()).toList());
        for (var sent : transport.appendEntries()) {
            assertEquals(1, sent.request().getTerm());
            assertEquals("n1", sent.request().getLeaderId());
        }
        // ...then every 50 ms.
        scheduler.advance(HEARTBEAT - 1);
        assertEquals(2, transport.appendEntries().size());
        scheduler.advance(1);
        assertEquals(4, transport.appendEntries().size());
        scheduler.advance(HEARTBEAT);
        assertEquals(6, transport.appendEntries().size());

        // A leader does not time out into a new election.
        scheduler.advance(10 * TIMEOUT);
        assertEquals(Role.LEADER, node.status().role());
        assertEquals(1, state.currentTerm());
    }

    @Test
    void candidateWithoutMajorityStartsANewRoundWithTermPlusOne() {
        startCandidate();
        vote("n2", 1, false);
        assertEquals(Role.CANDIDATE, node.status().role());

        scheduler.advance(TIMEOUT - 1);
        assertEquals(1, state.currentTerm());

        scheduler.advance(1); // election times out
        assertEquals(new RaftStatus(Role.CANDIDATE, 2, Optional.empty()), node.status());
        assertEquals(Optional.of("n1"), state.votedFor());
        assertEquals(2, requestVotesInTerm(2), "a new RequestVote to every peer");
    }

    @Test
    void voteFromAnEarlierRoundDoesNotCountInTheNextRound() {
        // Five nodes: a majority is 3.
        startNode(List.of("n2", "n3", "n4", "n5"));
        scheduler.advance(TIMEOUT); // round 1, term 1
        var lateRound1Vote = transport.lastRequestVoteTo("n2");
        scheduler.advance(TIMEOUT); // nobody answered: round 2, term 2
        assertEquals(2, state.currentTerm());

        // n2's grant for term 1 arrives late. Counted, it would make 3 with n3's below.
        lateRound1Vote.reply().complete(RequestVoteResponse.newBuilder().setTerm(1).setVoteGranted(true).build());
        scheduler.runPending();
        vote("n3", 2, true);
        assertEquals(Role.CANDIDATE, node.status().role(), "only n1 and n3 voted in term 2");

        vote("n4", 2, true);
        assertEquals(new RaftStatus(Role.LEADER, 2, Optional.of("n1")), node.status());
    }

    @Test
    void lateVoteAfterBecomingLeaderIsIgnored() {
        startLeader();
        int heartbeats = transport.appendEntries().size();

        vote("n3", 1, true);

        assertEquals(Role.LEADER, node.status().role());
        assertEquals(heartbeats, transport.appendEntries().size(), "no second round of 'becoming leader'");
    }

    @Test
    void candidateReceivingAppendEntriesWithItsOwnTermBecomesFollower() throws Exception {
        startCandidate();

        AppendEntriesResponse reply = heartbeatFrom("n2", 1);

        assertTrue(reply.getSuccess());
        assertEquals(1, reply.getTerm());
        assertEquals(new RaftStatus(Role.FOLLOWER, 1, Optional.of("n2")), node.status());
    }

    @Test
    void candidateReceivingAppendEntriesWithAHigherTermBecomesFollowerInThatTerm() throws Exception {
        startCandidate();

        AppendEntriesResponse reply = heartbeatFrom("n3", 4);

        assertTrue(reply.getSuccess());
        assertEquals(4, reply.getTerm());
        assertEquals(new RaftStatus(Role.FOLLOWER, 4, Optional.of("n3")), node.status());
        assertEquals(Optional.empty(), state.votedFor());
    }

    // ---- leader steps down ----

    @Test
    void leaderSeeingAHigherTermInARequestBecomesFollower() throws Exception {
        startLeader();

        RequestVoteResponse reply = requestVoteFrom("n3", 3, 0, 0);

        assertTrue(reply.getVoteGranted());
        assertEquals(new RaftStatus(Role.FOLLOWER, 3, Optional.empty()), node.status());
    }

    @Test
    void leaderSeeingAHigherTermInAResponseBecomesFollowerAndStopsHeartbeats() {
        startLeader();
        transport.appendEntries().getFirst().reply()
                .complete(AppendEntriesResponse.newBuilder().setTerm(5).setSuccess(false).build());
        scheduler.runPending();

        assertEquals(new RaftStatus(Role.FOLLOWER, 5, Optional.empty()), node.status());
        assertEquals(5, state.currentTerm());

        int sent = transport.appendEntries().size();
        scheduler.advance(TIMEOUT);
        assertEquals(sent, transport.appendEntries().size(), "no heartbeat after stepping down");
        // As a follower it got a new election timer, so it is now campaigning in term 6.
        assertEquals(new RaftStatus(Role.CANDIDATE, 6, Optional.empty()), node.status());
    }

    @Test
    void candidateSeeingAHigherTermInAVoteResponseBecomesFollower() {
        startCandidate();

        vote("n2", 3, false);

        assertEquals(new RaftStatus(Role.FOLLOWER, 3, Optional.empty()), node.status());
        assertEquals(Optional.empty(), state.votedFor());
    }

    // ---- election timer ----

    @Test
    void heartbeatsKeepAFollowerFromStartingAnElection() throws Exception {
        startNode(List.of("n2", "n3"));

        for (int i = 0; i < 10; i++) { // 1 s of heartbeats, every 100 ms (< the 200 ms timeout)
            scheduler.advance(100);
            assertTrue(heartbeatFrom("n2", 1).getSuccess());
        }
        assertEquals(new RaftStatus(Role.FOLLOWER, 1, Optional.of("n2")), node.status());
        assertTrue(transport.requestVotes().isEmpty());

        // The leader goes quiet: one timeout later this node campaigns.
        scheduler.advance(TIMEOUT);
        assertEquals(new RaftStatus(Role.CANDIDATE, 2, Optional.empty()), node.status());
    }

    @Test
    void grantingAVoteResetsTheElectionTimer() throws Exception {
        startNode(List.of("n2", "n3"));
        scheduler.advance(100);

        assertTrue(requestVoteFrom("n2", 1, 0, 0).getVoteGranted());

        scheduler.advance(150); // t = 250: the original timer (t = 200) would have fired
        assertEquals(Role.FOLLOWER, node.status().role());
        scheduler.advance(50); // t = 300: 200 ms after the vote
        assertEquals(new RaftStatus(Role.CANDIDATE, 2, Optional.empty()), node.status());
    }

    @Test
    void appendEntriesFromAStaleLeaderIsRejectedAndDoesNotResetTheTimer() throws Exception {
        startNode(List.of("n2", "n3"));
        scheduler.advance(100);
        requestVoteFrom("n2", 3, 0, 0); // now term 3; the timer fires at t = 300

        scheduler.advance(150); // t = 250
        AppendEntriesResponse reply = heartbeatFrom("n3", 2);
        assertFalse(reply.getSuccess());
        assertEquals(3, reply.getTerm(), "the reply tells the stale leader our term");
        assertEquals(new RaftStatus(Role.FOLLOWER, 3, Optional.empty()), node.status());

        scheduler.advance(50); // t = 300: not reset by the stale AppendEntries
        assertEquals(new RaftStatus(Role.CANDIDATE, 4, Optional.empty()), node.status());
    }

    @Test
    void rejectedRequestVoteWithAHigherTermStillMovesTheNodeToThatTermAsFollower() throws Exception {
        startNode(List.of("n2", "n3"), 5, 2); // our log ends at index 5, term 2
        scheduler.advance(TIMEOUT); // candidate in term 1; its next timeout is at t = 400
        scheduler.advance(100); // t = 300

        // n3 is in a newer term but its log is behind ours: no vote.
        RequestVoteResponse reply = requestVoteFrom("n3", 4, 9, 1);

        assertFalse(reply.getVoteGranted());
        assertEquals(4, reply.getTerm());
        assertEquals(new RaftStatus(Role.FOLLOWER, 4, Optional.empty()), node.status());
        assertEquals(Optional.empty(), state.votedFor());

        // Refusing is not granting: the timer was not reset and still fires at t = 400.
        scheduler.advance(100);
        assertEquals(new RaftStatus(Role.CANDIDATE, 5, Optional.empty()), node.status());
    }

    // ---- edge cases ----

    @Test
    void singleNodeClusterBecomesLeaderOnItsOwnVote() {
        startNode(List.of());
        scheduler.advance(TIMEOUT);

        assertEquals(new RaftStatus(Role.LEADER, 1, Optional.of("n1")), node.status());
        assertTrue(transport.requestVotes().isEmpty());
    }

    @Test
    void nodeStopsIfItCannotPersistAnElection() throws Exception {
        startNode(List.of("n2", "n3"));
        Files.delete(stateDir); // nothing written yet, so the directory is empty; startElection will fail

        scheduler.advance(TIMEOUT);

        assertTrue(transport.requestVotes().isEmpty(), "no RequestVote without a durable term and vote");
        assertEquals(Role.FOLLOWER, node.status().role());
        CompletableFuture<AppendEntriesResponse> reply =
                node.onAppendEntries(AppendEntriesRequest.newBuilder().setTerm(1).setLeaderId("n2").build());
        scheduler.runPending();
        assertThrows(ExecutionException.class, reply::get, "a stopped node does not reply");
    }

    @Test
    void rejectsAnInvalidPeerList() {
        VoteHandler votes = new VoteHandler(state, () -> 0);
        assertThrows(IllegalArgumentException.class, () -> new RaftNode("n1", List.of("n2", "n1"), state, votes,
                () -> 0, () -> 0, transport, scheduler, new FixedRandom(TIMEOUT), RaftConfig.defaults()));
        assertThrows(IllegalArgumentException.class, () -> new RaftNode("n1", List.of("n2", "n2"), state, votes,
                () -> 0, () -> 0, transport, scheduler, new FixedRandom(TIMEOUT), RaftConfig.defaults()));
    }
}
