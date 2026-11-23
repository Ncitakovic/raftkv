package io.github.ncitakovic.raftkv.raft;

import io.github.ncitakovic.raftkv.api.v1.AppendEntriesRequest;
import io.github.ncitakovic.raftkv.api.v1.AppendEntriesResponse;
import io.github.ncitakovic.raftkv.api.v1.RequestVoteRequest;
import io.github.ncitakovic.raftkv.api.v1.RequestVoteResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * A {@link RaftTransport} for tests: it records every request and leaves its reply pending until
 * the test completes it, so the test decides which peer answers, what, and when.
 */
final class InMemoryTransport implements RaftTransport {

    record Sent<Q, R>(String peerId, Q request, CompletableFuture<R> reply) {}

    private final List<Sent<RequestVoteRequest, RequestVoteResponse>> requestVotes = new ArrayList<>();
    private final List<Sent<AppendEntriesRequest, AppendEntriesResponse>> appendEntries = new ArrayList<>();

    @Override
    public CompletableFuture<RequestVoteResponse> requestVote(String peerId, RequestVoteRequest request) {
        CompletableFuture<RequestVoteResponse> reply = new CompletableFuture<>();
        requestVotes.add(new Sent<>(peerId, request, reply));
        return reply;
    }

    @Override
    public CompletableFuture<AppendEntriesResponse> appendEntries(String peerId, AppendEntriesRequest request) {
        CompletableFuture<AppendEntriesResponse> reply = new CompletableFuture<>();
        appendEntries.add(new Sent<>(peerId, request, reply));
        return reply;
    }

    List<Sent<RequestVoteRequest, RequestVoteResponse>> requestVotes() {
        return requestVotes;
    }

    List<Sent<AppendEntriesRequest, AppendEntriesResponse>> appendEntries() {
        return appendEntries;
    }

    /** The most recent RequestVote sent to {@code peerId}. */
    Sent<RequestVoteRequest, RequestVoteResponse> lastRequestVoteTo(String peerId) {
        return requestVotes.reversed().stream()
                .filter(s -> s.peerId().equals(peerId))
                .findFirst()
                .orElseThrow(() -> new AssertionError("No RequestVote sent to " + peerId));
    }
}
