package io.github.ncitakovic.raftkv.raft;

import io.github.ncitakovic.raftkv.api.v1.AppendEntriesRequest;
import io.github.ncitakovic.raftkv.api.v1.AppendEntriesResponse;
import io.github.ncitakovic.raftkv.api.v1.RequestVoteRequest;
import io.github.ncitakovic.raftkv.api.v1.RequestVoteResponse;
import java.util.concurrent.CompletableFuture;

/**
 * Sends Raft RPCs to other nodes. Calls must not block. The returned future may complete on any
 * thread, or fail if the peer cannot be reached; {@link RaftNode} moves every response onto its
 * Raft thread before looking at it.
 */
public interface RaftTransport {

    CompletableFuture<RequestVoteResponse> requestVote(String peerId, RequestVoteRequest request);

    CompletableFuture<AppendEntriesResponse> appendEntries(String peerId, AppendEntriesRequest request);
}
