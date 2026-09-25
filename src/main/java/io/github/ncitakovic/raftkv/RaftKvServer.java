package io.github.ncitakovic.raftkv;

import io.github.ncitakovic.raftkv.server.KeyValueService;
import io.github.ncitakovic.raftkv.store.DurableKvStore;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.health.v1.HealthCheckResponse.ServingStatus;
import io.grpc.protobuf.services.HealthStatusManager;
import io.grpc.protobuf.services.ProtoReflectionService;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry point. Configuration comes from environment variables so the same image runs locally,
 * in Docker Compose and in Kubernetes:
 *
 * <ul>
 *   <li>{@code RAFTKV_PORT} — gRPC port (default 50051)
 *   <li>{@code RAFTKV_DATA_DIR} — directory for the log (default ./data)
 * </ul>
 */
public final class RaftKvServer {

    private static final Logger log = LoggerFactory.getLogger(RaftKvServer.class);

    private final Server server;
    private final DurableKvStore store;
    private final HealthStatusManager health = new HealthStatusManager();

    public RaftKvServer(int port, Path dataDir) throws IOException {
        this.store = DurableKvStore.open(dataDir);
        this.server = ServerBuilder.forPort(port)
                .addService(new KeyValueService(store))
                .addService(health.getHealthService())
                .addService(ProtoReflectionService.newInstance())
                .build();
    }

    public void start() throws IOException {
        server.start();
        health.setStatus("", ServingStatus.SERVING);
        log.info("RaftKV listening on port {} ({} keys, last index {})",
                server.getPort(), store.size(), store.lastIndex());
    }

    public void stop() throws InterruptedException, IOException {
        health.enterTerminalState();
        server.shutdown();
        if (!server.awaitTermination(10, TimeUnit.SECONDS)) {
            server.shutdownNow();
        }
        store.close();
        log.info("RaftKV stopped");
    }

    public static void main(String[] args) throws Exception {
        int port = Integer.parseInt(env("RAFTKV_PORT", "50051"));
        Path dataDir = Path.of(env("RAFTKV_DATA_DIR", "data"));

        RaftKvServer node = new RaftKvServer(port, dataDir);
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

    private static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }
}
