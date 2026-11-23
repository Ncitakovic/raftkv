package io.github.ncitakovic.raftkv.raft;

/** The three roles of a Raft node (Raft paper, Figure 4). */
public enum Role {
    FOLLOWER,
    CANDIDATE,
    LEADER
}
