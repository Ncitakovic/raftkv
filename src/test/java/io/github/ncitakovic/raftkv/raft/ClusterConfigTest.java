package io.github.ncitakovic.raftkv.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class ClusterConfigTest {

    private static Optional<ClusterConfig> fromEnv(Map<String, String> env) {
        return ClusterConfig.fromEnv(env::get);
    }

    @Test
    void withoutPeersTheNodeRunsAlone() {
        assertTrue(fromEnv(Map.of()).isEmpty());
        assertTrue(fromEnv(Map.of("RAFTKV_NODE_ID", "a", "RAFTKV_PEERS", "  ")).isEmpty());
    }

    @Test
    void parsesNodeIdPeersInOrderAndRaftPort() {
        ClusterConfig config = fromEnv(Map.of(
                "RAFTKV_NODE_ID", "a",
                "RAFTKV_PEERS", "c=raftkv-c:50052, b=raftkv-b:50052",
                "RAFTKV_RAFT_PORT", "6000")).orElseThrow();

        assertEquals("a", config.nodeId());
        assertEquals(6000, config.raftPort());
        assertEquals(Map.of("b", "raftkv-b:50052", "c", "raftkv-c:50052"), config.peers());
        assertEquals(List.of("c", "b"), List.copyOf(config.peers().keySet()));
    }

    @Test
    void raftPortDefaultsTo50052() {
        ClusterConfig config = fromEnv(Map.of("RAFTKV_NODE_ID", "a", "RAFTKV_PEERS", "b=localhost:7000")).orElseThrow();
        assertEquals(50052, config.raftPort());
    }

    @Test
    void peersRequireANodeId() {
        assertThrows(IllegalArgumentException.class, () -> fromEnv(Map.of("RAFTKV_PEERS", "b=raftkv-b:50052")));
    }

    @Test
    void rejectsInvalidSettings() {
        for (String peers : List.of("b", "b=", "=host:1", "b=host", "b=host:", "b=host:x", "b=host:70000",
                "b=h:1,b=h:2", "b=h:1,a=h:2")) {
            assertThrows(IllegalArgumentException.class,
                    () -> fromEnv(Map.of("RAFTKV_NODE_ID", "a", "RAFTKV_PEERS", peers)), peers);
        }
        assertThrows(IllegalArgumentException.class, () -> fromEnv(Map.of(
                "RAFTKV_NODE_ID", "a", "RAFTKV_PEERS", "b=h:1", "RAFTKV_RAFT_PORT", "fast")));
    }
}
