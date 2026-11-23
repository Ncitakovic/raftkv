package io.github.ncitakovic.raftkv;

import io.github.ncitakovic.raftkv.raft.ClusterConfig;
import io.github.ncitakovic.raftkv.raft.ExecutorScheduler;
import io.github.ncitakovic.raftkv.raft.PersistentState;
import io.github.ncitakovic.raftkv.raft.RaftConfig;
import io.github.ncitakovic.raftkv.raft.RaftNode;
import io.github.ncitakovic.raftkv.raft.VoteHandler;
import io.github.ncitakovic.raftkv.server.GrpcRaftTransport;
import io.github.ncitakovic.raftkv.server.KeyValueService;
import io.github.ncitakovic.raftkv.server.RaftService;
import io.github.ncitakovic.raftkv.store.DurableKvStore;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.health.v1.HealthCheckResponse.ServingStatus;
import io.grpc.protobuf.services.HealthStatusManager;
import io.grpc.protobuf.services.ProtoReflectionService;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Optional;
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry point. Configuration comes from environment variables so the same image runs locally,
 * in Docker Compose and in Kubernetes:
 *
 * <ul>
 *   <li>{@code RAFTKV_PORT}: client gRPC port (default 50051)
 *   <li>{@code RAFTKV_DATA_DIR}: directory for the log and the Raft state (default ./data)
 *   <li>{@code RAFTKV_PEERS}: the other cluster members, e.g. {@code b=raftkv-b:50052,c=raftkv-c:50052}.
 *       Without it the server runs alone, without Raft, exactly as in milestone 1.
 *   <li>{@code RAFTKV_NODE_ID}: this node's ID (required with {@code RAFTKV_PEERS})
 *   <li>{@code RAFTKV_RAFT_PORT}: node-to-node gRPC port (default 50052)
 * </ul>
 *
 * <p>In a cluster, nodes elect a leader, but the key-value API is still served by every node from
 * its own local store: writes are not replicated until milestone 3.
 */
public final class RaftKvServer {

    private static final Logger log = LoggerFactory.getLogger(RaftKvServer.class);

    /** Deadline of one node-to-node RPC; below the 150 ms minimum election timeout. */
    static final Duration RAFT_RPC_TIMEOUT = Duration.ofMillis(100);

    private final Server server;
    private final DurableKvStore store;
    private final HealthStatusManager health = new HealthStatusManager();
    private final Optional<RaftParts> raft;

    /** Everything that exists only when the node is part of a cluster. */
    private record RaftParts(RaftNode node, Server server, ExecutorScheduler scheduler, GrpcRaftTransport transport) {}

    /** A single node without Raft, as in milestone 1. */
    public RaftKvServer(int port, Path dataDir) throws IOException {
        this(port, dataDir, Optional.empty());
    }

    public RaftKvServer(int port, Path dataDir, Optional<ClusterConfig> cluster) throws IOException {
        this.store = DurableKvStore.open(dataDir);
        this.server = ServerBuilder.forPort(port)
                .addService(new KeyValueService(store))
                .addService(health.getHealthService())
                .addService(ProtoReflectionService.newInstance())
                .build();
        this.raft = cluster.isEmpty() ? Optional.empty() : Optional.of(createRaft(cluster.get(), dataDir));
    }

    private RaftParts createRaft(ClusterConfig cluster, Path dataDir) throws IOException {
        PersistentState state = PersistentState.open(dataDir);
        // TODO(milestone 3): log entries carry no term yet, so the last log term is 0 (as in VoteHandler).
        VoteHandler votes = new VoteHandler(state, store::lastIndex);
        ExecutorScheduler scheduler = ExecutorScheduler.singleThread("raft-" + cluster.nodeId());
        GrpcRaftTransport transport = GrpcRaftTransport.connect(cluster.peers(), RAFT_RPC_TIMEOUT);
        RaftNode node = new RaftNode(cluster.nodeId(), cluster.peers().keySet(), state, votes, store::lastIndex,
                () -> 0, transport, scheduler, new SplittableRandom(), RaftConfig.defaults());
        Server raftServer = ServerBuilder.forPort(cluster.raftPort())
                .addService(new RaftService(node))
                .build();
        return new RaftParts(node, raftServer, scheduler, transport);
    }

    public void start() throws IOException {
        server.start();
        if (raft.isPresent()) {
            raft.get().server().start();
            raft.get().node().start();
            log.info("Raft service listening on port {}", raft.get().server().getPort());
        }
        health.setStatus("", ServingStatus.SERVING);
        log.info("RaftKV listening on port {} ({} keys, last index {})",
                server.getPort(), store.size(), store.lastIndex());
    }

    public void stop() throws InterruptedException, IOException {
        health.enterTerminalState();
        server.shutdown();
        if (raft.isPresent()) {
            RaftParts parts = raft.get();
            parts.server().shutdown();
            parts.node().stop();
            parts.server().awaitTermination(5, TimeUnit.SECONDS);
            parts.scheduler().close();
            parts.transport().close();
        }
        if (!server.awaitTermination(10, TimeUnit.SECONDS)) {
            server.shutdownNow();
        }
        store.close();
        log.info("RaftKV stopped");
    }

    public static void main(String[] args) throws Exception {
        disableDnsCaching();
        int port = Integer.parseInt(env("RAFTKV_PORT", "50051"));
        Path dataDir = Path.of(env("RAFTKV_DATA_DIR", "data"));
        Optional<ClusterConfig> cluster = ClusterConfig.fromEnv(System::getenv);
        cluster.ifPresentOrElse(
                c -> log.info("Starting node {} with peers {}", c.nodeId(), c.peers()),
                () -> log.info("RAFTKV_PEERS not set: running as a single node without Raft"));

        RaftKvServer node = new RaftKvServer(port, dataDir, cluster);
        node.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                node.stop();
            } catch (Exception e) {
                log.error("Error during shutdown", e);
            }
        }, "shutdown"));
        node.server.awaitTermination();
    }

    /**
     * Peers are found by DNS name (e.g. {@code raftkv-b}), and in Docker or Kubernetes a restarted
     * peer may come back under a new address, or its name may not resolve while it is down. By
     * default the JVM remembers a failed lookup for 10 s, so the leader could not reach a restarted
     * peer for up to 10 s. The peer would then time out and start a needless election. With caching
     * off, every reconnect looks the name up again. Lookups happen only when a channel connects, so
     * this costs little.
     */
    private static void disableDnsCaching() {
        java.security.Security.setProperty("networkaddress.cache.ttl", "0");
        java.security.Security.setProperty("networkaddress.cache.negative.ttl", "0");
    }

    private static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }
}
