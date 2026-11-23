package io.github.ncitakovic.raftkv.server;

import io.github.ncitakovic.raftkv.api.v1.AppendEntriesRequest;
import io.github.ncitakovic.raftkv.api.v1.AppendEntriesResponse;
import io.github.ncitakovic.raftkv.api.v1.RaftGrpc;
import io.github.ncitakovic.raftkv.api.v1.RequestVoteRequest;
import io.github.ncitakovic.raftkv.api.v1.RequestVoteResponse;
import io.github.ncitakovic.raftkv.raft.RaftTransport;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.channel.ChannelOption;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

/**
 * {@link RaftTransport} over gRPC: one channel per peer, and a deadline on every call.
 *
 * <p><b>Why a deadline.</b> A peer that has crashed or is cut off may never answer. Without a
 * deadline the call would stay open forever. With one, it fails after {@code callTimeout}, which
 * {@link io.github.ncitakovic.raftkv.raft.RaftNode} treats as a lost message: the next heartbeat or
 * election round tries again. The timeout should be below the minimum election timeout.
 *
 * <p><b>Reconnecting quickly.</b> A restarted peer should get the leader's heartbeats as soon as
 * it is up again. Two things delay that:
 *
 * <ul>
 *   <li>A connection attempt to a stopped container's address does not fail at once: it hangs for
 *       seconds (no host answers). A new attempt starts only when that one gives up. In Docker this
 *       left a restarted node without heartbeats for about 5 s. So each attempt gives up after
 *       {@value #CONNECT_TIMEOUT_MS} ms.
 *   <li>After a failed connection, gRPC waits longer and longer (up to minutes) before trying
 *       again. So after every failed call ({@code UNAVAILABLE} or {@code DEADLINE_EXCEEDED}) the
 *       channel is told to retry at once.
 * </ul>
 */
public final class GrpcRaftTransport implements RaftTransport, AutoCloseable {

    /** Longest wait for one TCP connection attempt; peers are on a local network. */
    static final int CONNECT_TIMEOUT_MS = 250;

    private final Map<String, ManagedChannel> channels;
    private final Duration callTimeout;

    /** @param channels one channel per peer ID; closed by {@link #close} */
    public GrpcRaftTransport(Map<String, ManagedChannel> channels, Duration callTimeout) {
        this.channels = Map.copyOf(channels);
        this.callTimeout = Objects.requireNonNull(callTimeout, "callTimeout");
        if (callTimeout.isNegative() || callTimeout.isZero()) {
            throw new IllegalArgumentException("callTimeout must be positive: " + callTimeout);
        }
    }

    /** Opens a plaintext channel to each peer: ID → {@code host:port}. */
    public static GrpcRaftTransport connect(Map<String, String> peerAddresses, Duration callTimeout) {
        Map<String, ManagedChannel> channels = new LinkedHashMap<>();
        peerAddresses.forEach((id, address) ->
                channels.put(id, NettyChannelBuilder.forTarget(address)
                        .usePlaintext()
                        .withOption(ChannelOption.CONNECT_TIMEOUT_MILLIS, CONNECT_TIMEOUT_MS)
                        .build()));
        return new GrpcRaftTransport(channels, callTimeout);
    }

    @Override
    public CompletableFuture<RequestVoteResponse> requestVote(String peerId, RequestVoteRequest request) {
        return call(peerId, (stub, responses) -> stub.requestVote(request, responses));
    }

    @Override
    public CompletableFuture<AppendEntriesResponse> appendEntries(String peerId, AppendEntriesRequest request) {
        return call(peerId, (stub, responses) -> stub.appendEntries(request, responses));
    }

    private <R> CompletableFuture<R> call(String peerId, BiConsumer<RaftGrpc.RaftStub, StreamObserver<R>> rpc) {
        ManagedChannel channel = channels.get(peerId);
        if (channel == null) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("Unknown peer " + peerId));
        }
        CompletableFuture<R> result = new CompletableFuture<>();
        RaftGrpc.RaftStub stub = RaftGrpc.newStub(channel)
                .withDeadlineAfter(callTimeout.toNanos(), TimeUnit.NANOSECONDS);
        rpc.accept(stub, new StreamObserver<>() {
            @Override
            public void onNext(R response) {
                result.complete(response);
            }

            @Override
            public void onError(Throwable error) {
                Status.Code code = Status.fromThrowable(error).getCode();
                if (code == Status.Code.UNAVAILABLE || code == Status.Code.DEADLINE_EXCEEDED) {
                    channel.resetConnectBackoff();
                }
                result.completeExceptionally(error);
            }

            @Override
            public void onCompleted() {
                result.completeExceptionally(new IllegalStateException(peerId + " sent no response"));
            }
        });
        return result;
    }

    @Override
    public void close() {
        channels.values().forEach(ManagedChannel::shutdownNow);
    }
}
