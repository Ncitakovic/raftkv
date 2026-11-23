package io.github.ncitakovic.raftkv.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class RaftConfigTest {

    @Test
    void defaultsAreThoseOfTheRaftPaper() {
        RaftConfig config = RaftConfig.defaults();
        assertEquals(Duration.ofMillis(150), config.electionTimeoutMin());
        assertEquals(Duration.ofMillis(300), config.electionTimeoutMax());
        assertEquals(Duration.ofMillis(50), config.heartbeatInterval());
    }

    @Test
    void rejectsTimingsThatWouldCauseNeedlessElections() {
        Duration ms50 = Duration.ofMillis(50);
        Duration ms150 = Duration.ofMillis(150);
        Duration ms300 = Duration.ofMillis(300);
        assertThrows(IllegalArgumentException.class, () -> new RaftConfig(ms300, ms150, ms50), "min above max");
        assertThrows(IllegalArgumentException.class, () -> new RaftConfig(ms150, ms150, ms50), "no random range");
        assertThrows(IllegalArgumentException.class, () -> new RaftConfig(ms150, ms300, ms150), "heartbeat too slow");
        assertThrows(IllegalArgumentException.class, () -> new RaftConfig(ms150, ms300, Duration.ZERO), "no heartbeat");
    }
}
