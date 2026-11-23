package io.github.ncitakovic.raftkv.raft;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

/**
 * Who this node is in the cluster and how to reach the others.
 *
 * @param nodeId this node's ID, as stored in {@code votedFor} and sent in RPCs
 * @param raftPort port of this node's {@code Raft} gRPC service (node-to-node traffic)
 * @param peers the other nodes: ID → {@code host:port} of their Raft service, in the given order
 */
public record ClusterConfig(String nodeId, int raftPort, Map<String, String> peers) {

    public static final int DEFAULT_RAFT_PORT = 50052;

    public ClusterConfig {
        Objects.requireNonNull(nodeId, "nodeId");
        if (nodeId.isBlank()) {
            throw new IllegalArgumentException("Node ID must not be blank");
        }
        if (raftPort < 0 || raftPort > 65_535) {
            throw new IllegalArgumentException("Invalid Raft port " + raftPort);
        }
        if (peers.containsKey(nodeId)) {
            throw new IllegalArgumentException("Peers must not include this node (" + nodeId + ")");
        }
        peers = Collections.unmodifiableMap(new LinkedHashMap<>(peers));
    }

    /**
     * Reads {@code RAFTKV_NODE_ID}, {@code RAFTKV_PEERS} and {@code RAFTKV_RAFT_PORT}. Without
     * {@code RAFTKV_PEERS} the node runs alone, without Raft, and this returns empty.
     *
     * @param env looks up an environment variable ({@code System::getenv} in production)
     * @throws IllegalArgumentException if the variables are set but invalid
     */
    public static Optional<ClusterConfig> fromEnv(Function<String, String> env) {
        String peers = trimmed(env.apply("RAFTKV_PEERS"));
        if (peers == null) {
            return Optional.empty();
        }
        String nodeId = trimmed(env.apply("RAFTKV_NODE_ID"));
        if (nodeId == null) {
            throw new IllegalArgumentException("RAFTKV_PEERS is set, so RAFTKV_NODE_ID is required");
        }
        String port = trimmed(env.apply("RAFTKV_RAFT_PORT"));
        int raftPort;
        try {
            raftPort = port == null ? DEFAULT_RAFT_PORT : Integer.parseInt(port);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("RAFTKV_RAFT_PORT is not a number: " + port, e);
        }
        return Optional.of(new ClusterConfig(nodeId, raftPort, parsePeers(peers)));
    }

    /** Parses {@code "b=raftkv-b:50052,c=raftkv-c:50052"} into {@code {b=raftkv-b:50052, c=raftkv-c:50052}}. */
    static Map<String, String> parsePeers(String spec) {
        Map<String, String> peers = new LinkedHashMap<>();
        for (String entry : spec.split(",")) {
            String[] idAndAddress = entry.trim().split("=", -1);
            if (idAndAddress.length != 2) {
                throw invalidPeer(entry, "expected id=host:port");
            }
            String id = idAndAddress[0].trim();
            String address = idAndAddress[1].trim();
            int colon = address.lastIndexOf(':');
            if (id.isEmpty() || colon <= 0 || colon == address.length() - 1) {
                throw invalidPeer(entry, "expected id=host:port");
            }
            try {
                int port = Integer.parseInt(address.substring(colon + 1));
                if (port < 1 || port > 65_535) {
                    throw invalidPeer(entry, "port out of range");
                }
            } catch (NumberFormatException e) {
                throw invalidPeer(entry, "port is not a number");
            }
            if (peers.put(id, address) != null) {
                throw invalidPeer(entry, "duplicate ID " + id);
            }
        }
        return peers;
    }

    private static IllegalArgumentException invalidPeer(String entry, String reason) {
        return new IllegalArgumentException("Invalid RAFTKV_PEERS entry '" + entry.trim() + "': " + reason);
    }

    private static String trimmed(String value) {
        return value == null || value.isBlank() ? null : value.trim();
    }
}
