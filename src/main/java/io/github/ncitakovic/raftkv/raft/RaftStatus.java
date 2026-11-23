package io.github.ncitakovic.raftkv.raft;

import java.util.Objects;
import java.util.Optional;

/**
 * An immutable snapshot of a node's role, term and known leader, safe to read from any thread.
 *
 * @param leaderId the leader of {@code term} if this node knows it (itself when it is the leader)
 */
public record RaftStatus(Role role, long term, Optional<String> leaderId) {

    public RaftStatus {
        Objects.requireNonNull(role, "role");
        Objects.requireNonNull(leaderId, "leaderId");
    }
}
