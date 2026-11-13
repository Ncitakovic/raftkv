package io.github.ncitakovic.raftkv.raft;

import io.github.ncitakovic.raftkv.api.v1.RequestVoteRequest;
import io.github.ncitakovic.raftkv.api.v1.RequestVoteResponse;
import java.io.IOException;
import java.util.Objects;
import java.util.function.LongSupplier;
import java.util.Optional;

/**
 * Decides whether this node grants its vote to a candidate (Raft paper §5.2, §5.4.1).
 *
 * <p>Rules, applied in order:
 *
 * <ol>
 *   <li>If the request's term is lower than {@code currentTerm}, reject. The candidate is stale.
 *   <li>If the request's term is higher, move to that term first ({@link PersistentState#advanceTerm}),
 *       which also clears {@code votedFor}. This happens even if the vote is then refused.
 *   <li>If this node already voted for a <i>different</i> candidate in this term, reject. A repeated
 *       request from the same candidate gets the same answer again (the RPC may have been retried).
 *   <li>Election restriction: grant only if the candidate's log is at least as up to date as this
 *       node's. Compare last log terms first; if they are equal, the higher last log index wins.
 *       Without this rule, a node missing committed entries could become leader and overwrite them.
 *   <li>Otherwise record the vote with {@link PersistentState#voteFor} and grant it.
 * </ol>
 *
 * <p>The response always carries this node's {@code currentTerm} after processing, so a candidate
 * that is behind learns the newer term and steps down.
 *
 * <p><b>Durability.</b> Every change to the term or vote is fsync'd by {@link PersistentState}
 * before {@link #handle} returns, so the caller may send the response as soon as it gets it.
 *
 * <p><b>Election timer.</b> The caller (later {@code RaftNode}) must reset its election timeout only
 * when a vote was granted (§5.2); it reads that from {@link RequestVoteResponse#getVoteGranted()}.
 *
 * <p>Not thread-safe on its own: like all Raft state, it is called only from the node's single
 * Raft thread.
 */
public final class VoteHandler {

    private final PersistentState state;
    private final LongSupplier lastLogIndex;
    private final LongSupplier lastLogTerm;

    /**
     * @param state this node's durable term and vote
     * @param lastLogIndex index of the last entry in this node's log, 0 if empty (in production
     *     {@code DurableKvStore::lastIndex})
     */
    public VoteHandler(PersistentState state, LongSupplier lastLogIndex) {
        // TODO(milestone 3): log entries do not carry a term yet, so every entry counts as term 0.
        //  Once LogEntry gains a term, pass the term of the last entry here instead.
        this(state, lastLogIndex, () -> 0L);
    }

    /**
     * Lets the term of the last log entry be supplied explicitly. Package-private for tests until
     * milestone 3, when log entries get a term and this becomes the production constructor.
     */
    VoteHandler(PersistentState state, LongSupplier lastLogIndex, LongSupplier lastLogTerm) {
        this.state = Objects.requireNonNull(state, "state");
        this.lastLogIndex = Objects.requireNonNull(lastLogIndex, "lastLogIndex");
        this.lastLogTerm = Objects.requireNonNull(lastLogTerm, "lastLogTerm");
    }

    /**
     * Handles a {@code RequestVote} RPC.
     *
     * @return the response to send; {@code vote_granted} tells the caller whether to reset its
     *     election timer
     * @throws IOException if the new term or vote could not be persisted; the caller must then
     *     not reply at all
     */
    public RequestVoteResponse handle(RequestVoteRequest req) throws IOException {
        // 0) Malformed request: refuse without touching our state (no vote, no term change).
        if (req.getTerm() < 1 || req.getCandidateId().isBlank()) {
            return reply(false);
        }

        // 1) A candidate from an older term cannot become leader. Reply with our term so it steps down.
        if (req.getTerm() < state.currentTerm()) {
            return reply(false);
        }

        // 2) A newer term always wins: move to it (this clears our old vote), even if we refuse below.
        if (req.getTerm() > state.currentTerm()) {
            state.advanceTerm(req.getTerm());
        }

        // 3) One vote per term: free if we have not voted, or already voted for this same candidate.
        Optional<String> votedFor = state.votedFor();
        boolean free = votedFor.isEmpty() || votedFor.get().equals(req.getCandidateId());

        // 4) The candidate must have at least everything we have.
        boolean logOk = isLogUpToDate(req.getLastLogTerm(), req.getLastLogIndex());

        // 5) Grant: persist the vote FIRST, then reply. A crash in between must not allow a second vote.
        if (free && logOk) {
            state.voteFor(req.getCandidateId());
            return reply(true);
        }

        // 6) Otherwise refuse.
        return reply(false);
    }

    /**
     * Raft section 5.4.1: compare the last entries. A later term is more up to date regardless of
     * length; with the same term, the longer (or equal) log is at least as up to date.
     */
    private boolean isLogUpToDate(long candidateLastTerm, long candidateLastIndex) {
        long myLastTerm = lastLogTerm.getAsLong();
        long myLastIndex = lastLogIndex.getAsLong();
        if (candidateLastTerm != myLastTerm) {
            return candidateLastTerm > myLastTerm;
        }
        return candidateLastIndex >= myLastIndex;
    }

    /** Every reply carries our CURRENT term, i.e. after a possible advanceTerm in step 2. */
    private RequestVoteResponse reply(boolean granted) {
        return RequestVoteResponse.newBuilder()
                .setTerm(state.currentTerm())
                .setVoteGranted(granted)
                .build();
    }
}
