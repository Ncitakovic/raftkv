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
 * {@link #TIMEOUT} ms (see {@link FixedRandom}), so "199 ms: still a follower, 200 ms: PreVote"
 * can be asserted precisely. Peers are played by the test through {@link InMemoryTransport}.
 */
class RaftNodeTest {

    private static final long TIMEOUT = 200;
    private static final long HEARTBEAT = RaftConfig.defaults().heartbeatInterval().toMillis(); // 50
    private static final long ELECTION_TIMEOUT_MIN = RaftConfig.defaults().electionTimeoutMin().toMillis(); // 150

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

    /** Starts a 3-node cluster member, lets it time out, and grants n2's PreVote: candidate of term 1. */
    private void startCandidate() {
        startNode(List.of("n2", "n3"));
        scheduler.advance(TIMEOUT);
        assertEquals(Role.PRE_CANDIDATE, node.status().role());
        preVote("n2", true);
        assertEquals(Role.CANDIDATE, node.status().role());
    }

    /** Starts a 3-node cluster member and makes it leader of term 1 with n2's vote. */
    private void startLeader() {
        startCandidate();
        vote("n2", 1, true);
        assertEquals(Role.LEADER, node.status().role());
    }

    private List<InMemoryTransport.Sent<RequestVoteRequest, RequestVoteResponse>> sent(boolean preVote) {
        return transport.requestVotes().stream().filter(s -> s.request().getPreVote() == preVote).toList();
    }

    private InMemoryTransport.Sent<RequestVoteRequest, RequestVoteResponse> lastSent(String peer, boolean preVote) {
        return sent(preVote).reversed().stream()
                .filter(s -> s.peerId().equals(peer))
                .findFirst()
                .orElseThrow(() -> new AssertionError("No " + (preVote ? "PreVote" : "RequestVote") + " to " + peer));
    }

    /** Peer {@code peer} answers this node's latest PreVote to it, in its own (unchanged) term. */
    private void preVote(String peer, boolean granted) {
        preVote(peer, state.currentTerm(), granted);
    }

    private void preVote(String peer, long peerTerm, boolean granted) {
        lastSent(peer, true).reply()
                .complete(RequestVoteResponse.newBuilder().setTerm(peerTerm).setVoteGranted(granted).build());
        scheduler.runPending();
    }

    /** Peer {@code peer} answers this node's latest real RequestVote to it. */
    private void vote(String peer, long term, boolean granted) {
        lastSent(peer, false).reply()
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
        return incomingVote(false, candidate, term, lastIndex, lastTerm);
    }

    private RequestVoteResponse preVoteFrom(String candidate, long term, long lastIndex, long lastTerm)
            throws Exception {
        return incomingVote(true, candidate, term, lastIndex, lastTerm);
    }

    private RequestVoteResponse incomingVote(boolean preVote, String candidate, long term, long lastIndex,
            long lastTerm) throws Exception {
        CompletableFuture<RequestVoteResponse> reply = node.onRequestVote(RequestVoteRequest.newBuilder()
                .setPreVote(preVote).setTerm(term).setCandidateId(candidate)
                .setLastLogIndex(lastIndex).setLastLogTerm(lastTerm).build());
        scheduler.runPending();
        return reply.get();
    }

    private long realRequestVotesInTerm(long term) {
        return sent(false).stream().filter(s -> s.request().getTerm() == term).count();
    }

    // ---- follower → pre-candidate → candidate ----

    @Test
    void electionTimeoutStartsAPreVoteThatChangesNeitherTermNorVote() {
        startNode(List.of("n2", "n3"));
        assertEquals(new RaftStatus(Role.FOLLOWER, 0, Optional.empty()), node.status());

        scheduler.advance(TIMEOUT - 1);
        assertEquals(Role.FOLLOWER, node.status().role());
        assertTrue(transport.requestVotes().isEmpty());

        scheduler.advance(1);
        assertEquals(new RaftStatus(Role.PRE_CANDIDATE, 0, Optional.empty()), node.status());
        assertEquals(0, state.currentTerm(), "no startElection before a PreVote majority");
        assertTrue(state.votedFor().isEmpty());
        assertEquals(List.of("n2", "n3"), sent(true).stream().map(s -> s.peerId()).toList());
        for (var s : sent(true)) {
            assertEquals(1, s.request().getTerm(), "PreVote asks about currentTerm + 1");
            assertEquals("n1", s.request().getCandidateId());
        }
        assertTrue(sent(false).isEmpty(), "no real RequestVote yet");
    }

    @Test
    void preVoteMajorityStartsTheRealElection() {
        startNode(List.of("n2", "n3"));
        scheduler.advance(TIMEOUT);

        preVote("n2", true); // n1 + n2 = 2 of 3

        assertEquals(new RaftStatus(Role.CANDIDATE, 1, Optional.empty()), node.status());
        assertEquals(1, state.currentTerm());
        assertEquals(Optional.of("n1"), state.votedFor(), "a candidate votes for itself");
        assertEquals(List.of("n2", "n3"), sent(false).stream().map(s -> s.peerId()).toList());
        for (var s : sent(false)) {
            assertEquals(1, s.request().getTerm());
            assertFalse(s.request().getPreVote());
        }
    }

    @Test
    void withoutAPreVoteMajorityTheTermNeverGrows() {
        startNode(List.of("n2", "n3"));
        scheduler.advance(TIMEOUT);

        // Both refuse, and their replies carry a much higher term: a PreVote reply changes nothing.
        preVote("n2", 7, false);
        preVote("n3", 7, false);
        assertEquals(new RaftStatus(Role.PRE_CANDIDATE, 0, Optional.empty()), node.status());

        scheduler.advance(TIMEOUT); // a new PreVote round, still about term 1
        scheduler.advance(TIMEOUT); // and another
        assertEquals(new RaftStatus(Role.PRE_CANDIDATE, 0, Optional.empty()), node.status());
        assertEquals(0, state.currentTerm());
        assertEquals(6, sent(true).size());
        assertTrue(sent(true).stream().allMatch(s -> s.request().getTerm() == 1));
        assertTrue(sent(false).isEmpty());
    }

    @Test
    void preVoteFromAnEarlierRoundDoesNotCount() {
        // Five nodes: a majority is 3.
        startNode(List.of("n2", "n3", "n4", "n5"));
        scheduler.advance(TIMEOUT); // PreVote round 1
        var lateRound1Yes = lastSent("n2", true);
        scheduler.advance(TIMEOUT); // nobody answered: round 2, same term

        lateRound1Yes.reply().complete(RequestVoteResponse.newBuilder().setTerm(0).setVoteGranted(true).build());
        scheduler.runPending();
        preVote("n3", true);
        assertEquals(Role.PRE_CANDIDATE, node.status().role(), "only n1 and n3 said yes in round 2");

        preVote("n4", true);
        assertEquals(new RaftStatus(Role.CANDIDATE, 1, Optional.empty()), node.status());
    }

    @Test
    void preVoteAndRequestVoteCarryLastLogIndexAndTermFromTheSuppliers() {
        startNode(List.of("n2", "n3"), 7, 3);
        scheduler.advance(TIMEOUT);
        RequestVoteRequest pre = lastSent("n2", true).request();
        assertEquals(7, pre.getLastLogIndex());
        assertEquals(3, pre.getLastLogTerm());

        preVote("n2", true);
        RequestVoteRequest real = lastSent("n2", false).request();
        assertEquals(7, real.getLastLogIndex());
        assertEquals(3, real.getLastLogTerm());
    }

    // ---- candidate ----

    @Test
    void candidateWithMajorityBecomesLeaderAndSendsHeartbeats() {
        startCandidate();
        vote("n2", 1, true); // 2 of 3, counting its own vote

        assertEquals(new RaftStatus(Role.LEADER, 1, Optional.of("n1")), node.status());
        // Heartbeats to every peer at once...
        assertEquals(List.of("n2", "n3"), transport.appendEntries().stream().map(s -> s.peerId()).toList());
        for (var s : transport.appendEntries()) {
            assertEquals(1, s.request().getTerm());
            assertEquals("n1", s.request().getLeaderId());
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
    void splitVoteGoesBackToAPreVoteInTheSameTerm() {
        startCandidate(); // term 1
        vote("n2", 1, false);
        assertEquals(Role.CANDIDATE, node.status().role());

        scheduler.advance(TIMEOUT); // the election times out
        assertEquals(new RaftStatus(Role.PRE_CANDIDATE, 1, Optional.empty()), node.status());
        assertEquals(1, state.currentTerm());
        assertEquals(2, lastSent("n2", true).request().getTerm());
        assertEquals(0, realRequestVotesInTerm(2), "no real election before a PreVote majority");

        preVote("n3", true);
        assertEquals(new RaftStatus(Role.CANDIDATE, 2, Optional.empty()), node.status());
        assertEquals(2, realRequestVotesInTerm(2));
    }

    @Test
    void voteFromAnEarlierTermDoesNotCountInTheNextElection() {
        startNode(List.of("n2", "n3", "n4", "n5"));
        scheduler.advance(TIMEOUT);
        preVote("n2", true);
        preVote("n3", true); // 3 of 5: candidate of term 1
        var lateTerm1Vote = lastSent("n2", false);
        scheduler.advance(TIMEOUT); // nobody voted: PreVote again
        preVote("n2", true);
        preVote("n3", true); // candidate of term 2
        assertEquals(2, state.currentTerm());

        // n2's grant for term 1 arrives late. Counted, it would make 3 with n3's below.
        lateTerm1Vote.reply().complete(RequestVoteResponse.newBuilder().setTerm(1).setVoteGranted(true).build());
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

    @Test
    void preCandidateReceivingAHeartbeatBecomesFollower() throws Exception {
        startNode(List.of("n2", "n3"));
        scheduler.advance(TIMEOUT);
        assertEquals(Role.PRE_CANDIDATE, node.status().role());

        assertTrue(heartbeatFrom("n2", 1).getSuccess());

        assertEquals(new RaftStatus(Role.FOLLOWER, 1, Optional.of("n2")), node.status());
        preVote("n3", true); // a late yes from the abandoned round
        assertEquals(Role.FOLLOWER, node.status().role());
        assertEquals(1, state.currentTerm());
    }

    // ---- answering PreVotes ----

    @Test
    void followerThatHeardTheLeaderRecentlyRefusesAPreVoteAndChangesNothing() throws Exception {
        startNode(List.of("n2", "n3"));
        scheduler.advance(100);
        heartbeatFrom("n2", 1); // the timer now fires at t = 300

        scheduler.advance(ELECTION_TIMEOUT_MIN - 1); // t = 249: still within the minimum timeout
        RequestVoteResponse reply = preVoteFrom("n3", 2, 0, 0);

        assertFalse(reply.getVoteGranted());
        assertEquals(1, reply.getTerm());
        assertEquals(new RaftStatus(Role.FOLLOWER, 1, Optional.of("n2")), node.status());
        assertTrue(state.votedFor().isEmpty());
        scheduler.advance(51); // t = 300: the PreVote did not reset the timer
        assertEquals(Role.PRE_CANDIDATE, node.status().role());
    }

    @Test
    void followerWithoutARecentLeaderGrantsAPreVoteWithoutChangingTermVoteOrTimer() throws Exception {
        startNode(List.of("n2", "n3"));
        scheduler.advance(10);
        heartbeatFrom("n2", 1); // the timer now fires at t = 210

        scheduler.advance(ELECTION_TIMEOUT_MIN); // t = 160: 150 ms of silence
        RequestVoteResponse reply = preVoteFrom("n3", 2, 0, 0);

        assertTrue(reply.getVoteGranted());
        assertEquals(1, reply.getTerm());
        assertEquals(new RaftStatus(Role.FOLLOWER, 1, Optional.of("n2")), node.status());
        assertEquals(1, state.currentTerm());
        assertTrue(state.votedFor().isEmpty(), "a PreVote yes is not a vote");
        scheduler.advance(50); // t = 210: the timer was not reset
        assertEquals(Role.PRE_CANDIDATE, node.status().role());
    }

    @Test
    void leaderAlwaysRefusesAPreVote() throws Exception {
        startLeader();
        scheduler.advance(10 * TIMEOUT); // a leader hears no heartbeats, but it is the active leader

        RequestVoteResponse reply = preVoteFrom("n3", 2, 0, 0);

        assertFalse(reply.getVoteGranted());
        assertEquals(new RaftStatus(Role.LEADER, 1, Optional.of("n1")), node.status());
    }

    @Test
    void preVoteFromANodeWithAnOlderLogIsRefused() throws Exception {
        startNode(List.of("n2", "n3"), 5, 2);

        assertFalse(preVoteFrom("n3", 1, 9, 1).getVoteGranted());
        assertTrue(preVoteFrom("n3", 1, 5, 2).getVoteGranted());
        assertEquals(0, state.currentTerm());
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

        int sentHeartbeats = transport.appendEntries().size();
        scheduler.advance(TIMEOUT);
        assertEquals(sentHeartbeats, transport.appendEntries().size(), "no heartbeat after stepping down");
        // As a follower it got a new election timer, so it is now asking for PreVotes about term 6.
        assertEquals(new RaftStatus(Role.PRE_CANDIDATE, 5, Optional.empty()), node.status());
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

        // The leader goes quiet: one timeout later this node asks for PreVotes.
        scheduler.advance(TIMEOUT);
        assertEquals(new RaftStatus(Role.PRE_CANDIDATE, 1, Optional.empty()), node.status());
    }

    @Test
    void grantingAVoteResetsTheElectionTimer() throws Exception {
        startNode(List.of("n2", "n3"));
        scheduler.advance(100);

        assertTrue(requestVoteFrom("n2", 1, 0, 0).getVoteGranted());

        scheduler.advance(150); // t = 250: the original timer (t = 200) would have fired
        assertEquals(Role.FOLLOWER, node.status().role());
        scheduler.advance(50); // t = 300: 200 ms after the vote
        assertEquals(new RaftStatus(Role.PRE_CANDIDATE, 1, Optional.empty()), node.status());
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
        assertEquals(new RaftStatus(Role.PRE_CANDIDATE, 3, Optional.empty()), node.status());
    }

    @Test
    void rejectedRequestVoteWithAHigherTermStillMovesTheNodeToThatTermAsFollower() throws Exception {
        startNode(List.of("n2", "n3"), 5, 2); // our log ends at index 5, term 2
        scheduler.advance(TIMEOUT); // PreVote in term 0; its next timeout is at t = 400
        scheduler.advance(100); // t = 300

        // n3 is in a newer term but its log is behind ours: no vote.
        RequestVoteResponse reply = requestVoteFrom("n3", 4, 9, 1);

        assertFalse(reply.getVoteGranted());
        assertEquals(4, reply.getTerm());
        assertEquals(new RaftStatus(Role.FOLLOWER, 4, Optional.empty()), node.status());
        assertEquals(Optional.empty(), state.votedFor());

        // Refusing is not granting: the timer was not reset and still fires at t = 400.
        scheduler.advance(100);
        assertEquals(new RaftStatus(Role.PRE_CANDIDATE, 4, Optional.empty()), node.status());
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
        scheduler.advance(TIMEOUT); // the PreVote writes nothing, so it still goes out

        preVote("n2", true); // majority: now startElection must write, and fails

        assertTrue(sent(false).isEmpty(), "no RequestVote without a durable term and vote");
        assertEquals(0, node.status().term());
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
