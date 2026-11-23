package io.github.ncitakovic.raftkv.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.ncitakovic.raftkv.api.v1.AppendEntriesRequest;
import io.github.ncitakovic.raftkv.api.v1.AppendEntriesResponse;
import io.github.ncitakovic.raftkv.api.v1.RaftGrpc;
import io.github.ncitakovic.raftkv.api.v1.RequestVoteRequest;
import io.github.ncitakovic.raftkv.api.v1.RequestVoteResponse;
import io.github.ncitakovic.raftkv.raft.ExecutorScheduler;
import io.github.ncitakovic.raftkv.raft.PersistentState;
import io.github.ncitakovic.raftkv.raft.RaftConfig;
import io.github.ncitakovic.raftkv.raft.RaftNode;
import io.github.ncitakovic.raftkv.raft.RaftStatus;
import io.github.ncitakovic.raftkv.raft.Role;
import io.github.ncitakovic.raftkv.raft.VoteHandler;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SplittableRandom;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The gRPC layer end to end: {@link GrpcRaftTransport} → in-process gRPC → {@link RaftService} →
 * a real {@link RaftNode} on its own Raft thread. Waits only on futures with a timeout; never sleeps.
 */
class GrpcRaftTransportTest {

    /** Long timeouts, so node "b" never starts its own election during a test. */
    private static final RaftConfig QUIET =
            new RaftConfig(Duration.ofSeconds(60), Duration.ofSeconds(120), Duration.ofSeconds(1));
    private static final Duration CALL_TIMEOUT = Duration.ofSeconds(5);

    @TempDir
    Path dir;

    private final List<Server> servers = new ArrayList<>();
    private final List<ManagedChannel> channels = new ArrayList<>();
    private ExecutorScheduler scheduler;
    private RaftNode nodeB;
    private GrpcRaftTransport transport;

    @BeforeEach
    void setUp() throws Exception {
        PersistentState state = PersistentState.open(dir);
        scheduler = ExecutorScheduler.singleThread("raft-b");
        // b's own outgoing RPCs are never used in these tests.
        GrpcRaftTransport unused = new GrpcRaftTransport(Map.of(), CALL_TIMEOUT);
        nodeB = new RaftNode("b", List.of("a", "c"), state, new VoteHandler(state, () -> 0), () -> 0, () -> 0,
                unused, scheduler, new SplittableRandom(1), QUIET);
        nodeB.start();

        String name = InProcessServerBuilder.generateName();
        servers.add(InProcessServerBuilder.forName(name).addService(new RaftService(nodeB)).build().start());
        transport = new GrpcRaftTransport(Map.of("b", channel(name)), CALL_TIMEOUT);
    }

    @AfterEach
    void tearDown() {
        transport.close();
        channels.forEach(ManagedChannel::shutdownNow);
        servers.forEach(Server::shutdownNow);
        scheduler.close();
    }

    private ManagedChannel channel(String serverName) {
        ManagedChannel channel = InProcessChannelBuilder.forName(serverName).build();
        channels.add(channel);
        return channel;
    }

    private static <T> T await(CompletableFuture<T> future) throws Exception {
        return future.get(10, TimeUnit.SECONDS);
    }

    private static Status.Code failureCode(CompletableFuture<?> future) {
        ExecutionException e = assertThrows(ExecutionException.class, () -> await(future));
        return Status.fromThrowable(e.getCause()).getCode();
    }

    @Test
    void requestVoteReachesTheNodeAndItsAnswerComesBack() throws Exception {
        RequestVoteResponse response = await(transport.requestVote("b", RequestVoteRequest.newBuilder()
                .setTerm(1).setCandidateId("a").build()));

        assertTrue(response.getVoteGranted());
        assertEquals(1, response.getTerm());
    }

    @Test
    void secondCandidateInTheSameTermIsRefusedOverGrpc() throws Exception {
        await(transport.requestVote("b", RequestVoteRequest.newBuilder().setTerm(1).setCandidateId("a").build()));

        RequestVoteResponse response = await(transport.requestVote("b", RequestVoteRequest.newBuilder()
                .setTerm(1).setCandidateId("c").build()));

        assertFalse(response.getVoteGranted());
    }

    @Test
    void heartbeatMakesTheNodeFollowTheLeader() throws Exception {
        AppendEntriesResponse response = await(transport.appendEntries("b", AppendEntriesRequest.newBuilder()
                .setTerm(3).setLeaderId("a").build()));

        assertTrue(response.getSuccess());
        assertEquals(3, response.getTerm());
        assertEquals(new RaftStatus(Role.FOLLOWER, 3, Optional.of("a")), nodeB.status());
    }

    @Test
    void stoppedNodeAnswersUnavailable() {
        nodeB.stop();

        CompletableFuture<AppendEntriesResponse> call = transport.appendEntries("b",
                AppendEntriesRequest.newBuilder().setTerm(1).setLeaderId("a").build());

        assertEquals(Status.Code.UNAVAILABLE, failureCode(call));
    }

    @Test
    void callToAPeerThatNeverAnswersFailsAtTheDeadline() throws Exception {
        String name = InProcessServerBuilder.generateName();
        servers.add(InProcessServerBuilder.forName(name).addService(new RaftGrpc.RaftImplBase() {
            @Override
            public void requestVote(RequestVoteRequest request, StreamObserver<RequestVoteResponse> responses) {
                // Never answers, like a peer that hangs.
            }
        }).build().start());
        try (GrpcRaftTransport toHangingPeer = new GrpcRaftTransport(Map.of("x", channel(name)),
                Duration.ofMillis(50))) {
            CompletableFuture<RequestVoteResponse> call = toHangingPeer.requestVote("x",
                    RequestVoteRequest.newBuilder().setTerm(1).setCandidateId("a").build());

            assertEquals(Status.Code.DEADLINE_EXCEEDED, failureCode(call));
        }
    }

    @Test
    void callToAPeerThatIsDownFailsUnavailable() {
        try (GrpcRaftTransport toMissingPeer = new GrpcRaftTransport(Map.of("x", channel("no-such-server")),
                CALL_TIMEOUT)) {
            CompletableFuture<RequestVoteResponse> call = toMissingPeer.requestVote("x",
                    RequestVoteRequest.newBuilder().setTerm(1).setCandidateId("a").build());

            assertEquals(Status.Code.UNAVAILABLE, failureCode(call));
        }
    }

    @Test
    void unknownPeerFailsAtOnce() {
        CompletableFuture<RequestVoteResponse> call = transport.requestVote("zz",
                RequestVoteRequest.newBuilder().setTerm(1).setCandidateId("a").build());

        ExecutionException e = assertThrows(ExecutionException.class, () -> await(call));
        assertInstanceOf(IllegalArgumentException.class, e.getCause());
    }
}
