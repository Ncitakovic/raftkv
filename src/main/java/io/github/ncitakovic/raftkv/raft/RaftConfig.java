package io.github.ncitakovic.raftkv.raft;

import java.time.Duration;
import java.util.Objects;

/**
 * Timing of leader election.
 *
 * @param electionTimeoutMin shortest election timeout; each timeout is drawn at random from
 *     {@code [min, max]} so that nodes rarely time out together and split the vote
 * @param electionTimeoutMax longest election timeout
 * @param heartbeatInterval how often a leader sends heartbeats; must be well below the minimum
 *     election timeout, or followers start elections while the leader is healthy
 */
public record RaftConfig(Duration electionTimeoutMin, Duration electionTimeoutMax, Duration heartbeatInterval) {

    public RaftConfig {
        Objects.requireNonNull(electionTimeoutMin, "electionTimeoutMin");
        Objects.requireNonNull(electionTimeoutMax, "electionTimeoutMax");
        Objects.requireNonNull(heartbeatInterval, "heartbeatInterval");
        if (heartbeatInterval.isNegative() || heartbeatInterval.isZero()) {
            throw new IllegalArgumentException("heartbeatInterval must be positive: " + heartbeatInterval);
        }
        if (electionTimeoutMin.compareTo(electionTimeoutMax) >= 0) {
            throw new IllegalArgumentException("electionTimeoutMin " + electionTimeoutMin
                    + " must be below electionTimeoutMax " + electionTimeoutMax);
        }
        if (heartbeatInterval.compareTo(electionTimeoutMin) >= 0) {
            throw new IllegalArgumentException("heartbeatInterval " + heartbeatInterval
                    + " must be below electionTimeoutMin " + electionTimeoutMin);
        }
    }

    /** 150–300 ms election timeouts and a 50 ms heartbeat, as suggested in the Raft paper. */
    public static RaftConfig defaults() {
        return new RaftConfig(Duration.ofMillis(150), Duration.ofMillis(300), Duration.ofMillis(50));
    }
}
