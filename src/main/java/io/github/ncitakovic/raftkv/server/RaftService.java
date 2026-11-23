package io.github.ncitakovic.raftkv.server;

import io.github.ncitakovic.raftkv.api.v1.AppendEntriesRequest;
import io.github.ncitakovic.raftkv.api.v1.AppendEntriesResponse;
import io.github.ncitakovic.raftkv.api.v1.RaftGrpc;
import io.github.ncitakovic.raftkv.api.v1.RequestVoteRequest;
import io.github.ncitakovic.raftkv.api.v1.RequestVoteResponse;
import io.github.ncitakovic.raftkv.raft.RaftNode;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * The node-to-node {@code Raft} gRPC service. It holds no logic: each RPC is handed to
 * {@link RaftNode}, which handles it on the node's single Raft thread, and the answer is sent back
 * when that is done. gRPC threads therefore never touch Raft state.
 *
 * <p>If the node cannot answer (it is stopped, or could not persist its term or vote), the call
 * fails with {@code UNAVAILABLE} and no answer is sent. The caller treats it like a lost message.
 */
public final class RaftService extends RaftGrpc.RaftImplBase {

    private final RaftNode node;

    public RaftService(RaftNode node) {
        this.node = Objects.requireNonNull(node, "node");
    }

    @Override
    public void requestVote(RequestVoteRequest request, StreamObserver<RequestVoteResponse> responses) {
        reply(node.onRequestVote(request), responses);
    }

    @Override
    public void appendEntries(AppendEntriesRequest request, StreamObserver<AppendEntriesResponse> responses) {
        reply(node.onAppendEntries(request), responses);
    }

    private static <R> void reply(CompletableFuture<R> answer, StreamObserver<R> responses) {
        answer.whenComplete((response, error) -> {
            if (error != null) {
                responses.onError(Status.UNAVAILABLE.withDescription(error.getMessage()).asRuntimeException());
            } else {
                responses.onNext(response);
                responses.onCompleted();
            }
        });
    }
}
