package io.github.ncitakovic.raftkv.raft;

import io.github.ncitakovic.raftkv.api.v1.AppendEntriesRequest;
import io.github.ncitakovic.raftkv.api.v1.AppendEntriesResponse;
import io.github.ncitakovic.raftkv.api.v1.RequestVoteRequest;
import io.github.ncitakovic.raftkv.api.v1.RequestVoteResponse;
import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.random.RandomGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Leader election: the FOLLOWER → CANDIDATE → LEADER state machine (Raft paper §5.2, Figure 4).
 * Log replication is not part of it yet; {@code AppendEntries} is only a heartbeat.
 *
 * <ul>
 *   <li><b>Follower.</b> Waits a random election timeout (150–300 ms by default). The timer is
 *       reset only when a valid {@code AppendEntries} arrives from the current leader, or when this
 *       node grants its vote. If it expires, the node becomes a candidate.
 *   <li><b>Candidate.</b> Increments its term and votes for itself in one durable write
 *       ({@link PersistentState#startElection}), asks every peer for a vote, and starts a new
 *       random timeout. With votes from a majority (its own included) it becomes leader; if the
 *       timeout expires first, it starts another round in the next term.
 *   <li><b>Leader.</b> Sends a heartbeat to every peer at once, then every 50 ms by default.
 *   <li><b>Any role.</b> A request or response carrying a higher term moves the node to that term
 *       and makes it a follower.
 * </ul>
 *
 * <p><b>One thread.</b> All state is read and changed only by tasks on the {@link Scheduler}, which
 * runs them one at a time. Incoming RPCs ({@link #onRequestVote}, {@link #onAppendEntries}) and
 * responses from the {@link RaftTransport} are handed to that thread; nothing here needs a lock.
 * {@link #status()} is a snapshot published for other threads.
 *
 * <p><b>Durability.</b> A term or vote is fsync'd by {@link PersistentState} before any reply is
 * sent or any RequestVote is sent. If that write fails, the node stops (fail-stop): carrying on
 * without a durable term or vote could let it vote twice in one term.
 */
public final class RaftNode {

    private static final Logger log = LoggerFactory.getLogger(RaftNode.class);

    private final String selfId;
    private final List<String> peerIds;
    private final PersistentState state;
    private final VoteHandler voteHandler;
    private final LongSupplier lastLogIndex;
    private final LongSupplier lastLogTerm;
    private final RaftTransport transport;
    private final Scheduler scheduler;
    private final RandomGenerator random;
    private final RaftConfig config;
    private final int majority;

    // ---- Raft-thread state: read and written only by tasks on the scheduler ----
    private boolean started;
    private boolean stopped;
    private Role role = Role.FOLLOWER;
    private String leaderId; // null = unknown in the current term
    private final Set<String> votesReceived = new HashSet<>(); // candidate only, for the current term
    private Cancellable electionTimer; // null = not running (a leader has none)
    private Cancellable heartbeatTimer; // leader only

    private volatile RaftStatus status;

    /**
     * @param selfId this node's ID, as stored in {@code votedFor} and sent as {@code candidate_id}
     * @param peerIds the other members of the cluster (not including this node)
     * @param voteHandler decides incoming RequestVotes; must use the same {@code state} and log
     *     suppliers as this node
     * @param lastLogIndex index of the last entry in this node's log, 0 if empty
     * @param lastLogTerm term of the last entry in this node's log, 0 if empty
     * @param random source of election timeouts; used only on the Raft thread
     */
    public RaftNode(String selfId, Collection<String> peerIds, PersistentState state, VoteHandler voteHandler,
            LongSupplier lastLogIndex, LongSupplier lastLogTerm, RaftTransport transport, Scheduler scheduler,
            RandomGenerator random, RaftConfig config) {
        this.selfId = Objects.requireNonNull(selfId, "selfId");
        if (selfId.isBlank()) {
            throw new IllegalArgumentException("selfId must not be blank");
        }
        Set<String> peers = new LinkedHashSet<>(Objects.requireNonNull(peerIds, "peerIds"));
        if (peers.size() != peerIds.size()) {
            throw new IllegalArgumentException("Duplicate peer IDs: " + peerIds);
        }
        if (peers.contains(selfId)) {
            throw new IllegalArgumentException("peerIds must not contain this node's own ID " + selfId);
        }
        this.peerIds = List.copyOf(peers);
        this.state = Objects.requireNonNull(state, "state");
        this.voteHandler = Objects.requireNonNull(voteHandler, "voteHandler");
        this.lastLogIndex = Objects.requireNonNull(lastLogIndex, "lastLogIndex");
        this.lastLogTerm = Objects.requireNonNull(lastLogTerm, "lastLogTerm");
        this.transport = Objects.requireNonNull(transport, "transport");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
        this.random = Objects.requireNonNull(random, "random");
        this.config = Objects.requireNonNull(config, "config");
        this.majority = (this.peerIds.size() + 1) / 2 + 1;
        this.status = new RaftStatus(Role.FOLLOWER, state.currentTerm(), Optional.empty());
    }

    /** Starts as a follower in the persisted term, with a fresh election timeout. */
    public void start() {
        scheduler.execute(() -> {
            if (started || stopped) {
                return;
            }
            started = true;
            role = Role.FOLLOWER;
            publishStatus();
            logRoleChange();
            resetElectionTimer();
        });
    }

    /** Cancels all timers. Afterwards responses are ignored and incoming RPCs fail. */
    public void stop() {
        scheduler.execute(this::stopNow);
    }

    public RaftStatus status() {
        return status;
    }

    // ---- incoming RPCs ----

    /**
     * Handles a RequestVote from a candidate on the Raft thread. The future fails, and no reply
     * should be sent, if the node is not running or could not persist its new term or vote.
     */
    public CompletableFuture<RequestVoteResponse> onRequestVote(RequestVoteRequest request) {
        Objects.requireNonNull(request, "request");
        CompletableFuture<RequestVoteResponse> reply = new CompletableFuture<>();
        scheduler.execute(() -> {
            if (!running(reply)) {
                return;
            }
            try {
                reply.complete(handleRequestVote(request));
            } catch (IOException e) {
                failStop(e);
                reply.completeExceptionally(e);
            }
        });
        return reply;
    }

    /** Handles an AppendEntries (a heartbeat, for now) on the Raft thread; fails like {@link #onRequestVote}. */
    public CompletableFuture<AppendEntriesResponse> onAppendEntries(AppendEntriesRequest request) {
        Objects.requireNonNull(request, "request");
        CompletableFuture<AppendEntriesResponse> reply = new CompletableFuture<>();
        scheduler.execute(() -> {
            if (!running(reply)) {
                return;
            }
            try {
                reply.complete(handleAppendEntries(request));
            } catch (IOException e) {
                failStop(e);
                reply.completeExceptionally(e);
            }
        });
        return reply;
    }

    private RequestVoteResponse handleRequestVote(RequestVoteRequest request) throws IOException {
        // VoteHandler applies the vote rules and may move us to a newer term. It cannot change our
        // role, so compare the term before and after to know whether to step down.
        long termBefore = state.currentTerm();
        RequestVoteResponse response = voteHandler.handle(request);
        // Reset before stepping down: becomeFollower arms a timer only if none is running, so a
        // leader that steps down and grants its vote draws one timeout, not two.
        if (response.getVoteGranted()) {
            resetElectionTimer();
        }
        if (state.currentTerm() > termBefore) {
            becomeFollower(null);
        }
        return response;
    }

    private AppendEntriesResponse handleAppendEntries(AppendEntriesRequest request) throws IOException {
        // Malformed (proto3 defaults for missing fields) or from a deposed leader: refuse, and send our
        // term so a stale leader steps down. Nothing changes here, and the timer keeps running.
        if (request.getTerm() < 1 || request.getLeaderId().isBlank() || request.getTerm() < state.currentTerm()) {
            return appendEntriesReply(false);
        }
        if (request.getTerm() > state.currentTerm()) {
            state.advanceTerm(request.getTerm());
        }
        // A valid AppendEntries from the leader of our term: follow it. Reset first (see handleRequestVote).
        resetElectionTimer();
        becomeFollower(request.getLeaderId());
        return appendEntriesReply(true);
    }

    private AppendEntriesResponse appendEntriesReply(boolean success) {
        return AppendEntriesResponse.newBuilder().setTerm(state.currentTerm()).setSuccess(success).build();
    }

    // ---- role changes ----

    /**
     * Follows {@code newLeaderId} (null if not yet known). Stops sending heartbeats. A running
     * election timer keeps running, because only a valid AppendEntries or a granted vote may reset
     * it; a leader has none, so one is started.
     */
    private void becomeFollower(String newLeaderId) {
        Role previous = role;
        role = Role.FOLLOWER;
        leaderId = newLeaderId;
        votesReceived.clear();
        cancelHeartbeatTimer();
        if (electionTimer == null) {
            resetElectionTimer();
        }
        publishStatus();
        if (previous != Role.FOLLOWER) {
            logRoleChange();
        }
    }

    private void becomeCandidate() throws IOException {
        // Term + 1 and a vote for ourselves, in one fsync'd write, before any RequestVote goes out.
        long electionTerm = state.startElection(selfId);
        role = Role.CANDIDATE;
        leaderId = null;
        votesReceived.clear();
        votesReceived.add(selfId);
        publishStatus();
        logRoleChange();
        // "Election times out": if this round gets no majority, the timer starts the next one.
        resetElectionTimer();

        if (votesReceived.size() >= majority) { // a single-node cluster
            becomeLeader();
            return;
        }
        RequestVoteRequest request = RequestVoteRequest.newBuilder()
                .setTerm(electionTerm)
                .setCandidateId(selfId)
                .setLastLogIndex(lastLogIndex.getAsLong())
                .setLastLogTerm(lastLogTerm.getAsLong())
                .build();
        for (String peer : peerIds) {
            send(() -> transport.requestVote(peer, request),
                    (response, error) -> onVoteResponse(peer, electionTerm, response, error));
        }
    }

    private void becomeLeader() {
        role = Role.LEADER;
        leaderId = selfId;
        votesReceived.clear();
        cancelElectionTimer();
        publishStatus();
        logRoleChange();
        sendHeartbeats();
        heartbeatTimer = scheduler.schedule(config.heartbeatInterval(), this::onHeartbeatTimer);
    }

    // ---- timers ----

    private void resetElectionTimer() {
        cancelElectionTimer();
        long min = config.electionTimeoutMin().toMillis();
        long max = config.electionTimeoutMax().toMillis();
        Duration timeout = Duration.ofMillis(random.nextLong(min, max + 1));
        electionTimer = scheduler.schedule(timeout, this::onElectionTimeout);
    }

    private void onElectionTimeout() {
        electionTimer = null;
        if (stopped || role == Role.LEADER) {
            return;
        }
        try {
            becomeCandidate();
        } catch (IOException e) {
            failStop(e);
        }
    }

    private void onHeartbeatTimer() {
        heartbeatTimer = null;
        if (stopped || role != Role.LEADER) {
            return;
        }
        sendHeartbeats();
        heartbeatTimer = scheduler.schedule(config.heartbeatInterval(), this::onHeartbeatTimer);
    }

    private void cancelElectionTimer() {
        if (electionTimer != null) {
            electionTimer.cancel();
            electionTimer = null;
        }
    }

    private void cancelHeartbeatTimer() {
        if (heartbeatTimer != null) {
            heartbeatTimer.cancel();
            heartbeatTimer = null;
        }
    }

    // ---- outgoing RPCs and their responses ----

    private void sendHeartbeats() {
        long term = state.currentTerm();
        AppendEntriesRequest request = AppendEntriesRequest.newBuilder().setTerm(term).setLeaderId(selfId).build();
        for (String peer : peerIds) {
            send(() -> transport.appendEntries(peer, request),
                    (response, error) -> onAppendEntriesResponse(peer, response, error));
        }
    }

    private void onVoteResponse(String peer, long electionTerm, RequestVoteResponse response, Throwable error) {
        if (stopped) {
            return;
        }
        if (error != null) {
            log.debug("node {}: RequestVote to {} failed: {}", selfId, peer, error.toString());
            return; // the election timeout covers lost votes
        }
        try {
            if (stepDownIfNewerTerm(response.getTerm())) {
                return;
            }
        } catch (IOException e) {
            failStop(e);
            return;
        }
        // A vote counts only for the round it was asked in: not after we won, lost, or moved on.
        if (role != Role.CANDIDATE || state.currentTerm() != electionTerm || !response.getVoteGranted()) {
            return;
        }
        votesReceived.add(peer);
        if (votesReceived.size() >= majority) {
            becomeLeader();
        }
    }

    private void onAppendEntriesResponse(String peer, AppendEntriesResponse response, Throwable error) {
        if (stopped) {
            return;
        }
        if (error != null) {
            log.debug("node {}: AppendEntries to {} failed: {}", selfId, peer, error.toString());
            return; // the next heartbeat retries
        }
        try {
            stepDownIfNewerTerm(response.getTerm());
        } catch (IOException e) {
            failStop(e);
        }
    }

    /** Any message from a newer term: adopt it and follow, leader still unknown. */
    private boolean stepDownIfNewerTerm(long term) throws IOException {
        if (term <= state.currentTerm()) {
            return false;
        }
        state.advanceTerm(term);
        becomeFollower(null);
        return true;
    }

    /**
     * Sends one RPC and hands its outcome to {@code onResult} on the Raft thread. A transport that
     * throws instead of returning a failed future is treated the same way.
     */
    private <R> void send(Supplier<CompletableFuture<R>> call,
            BiConsumer<R, Throwable> onResult) {
        CompletableFuture<R> future;
        try {
            future = call.get();
        } catch (RuntimeException e) {
            future = CompletableFuture.failedFuture(e);
        }
        future.whenComplete((response, error) -> scheduler.execute(() -> onResult.accept(response, error)));
    }

    // ---- lifecycle helpers ----

    private boolean running(CompletableFuture<?> reply) {
        if (started && !stopped) {
            return true;
        }
        reply.completeExceptionally(new IllegalStateException("node " + selfId + " is not running"));
        return false;
    }

    private void failStop(IOException e) {
        log.error("node {} could not persist its Raft state and is stopping", selfId, e);
        stopNow();
    }

    private void stopNow() {
        stopped = true;
        cancelElectionTimer();
        cancelHeartbeatTimer();
    }

    private void publishStatus() {
        status = new RaftStatus(role, state.currentTerm(), Optional.ofNullable(leaderId));
    }

    private void logRoleChange() {
        log.info("node {} became {} in term {}", selfId, role, state.currentTerm());
    }
}
