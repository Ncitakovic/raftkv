package io.github.ncitakovic.raftkv.raft;

/** The roles of a Raft node (Raft paper, Figure 4, plus the PreVote phase from Ongaro's thesis §9.6). */
public enum Role {
    FOLLOWER,
    /**
     * Its election timeout expired, and it is asking the others whether they would vote for it,
     * without changing its term. Only if a majority says yes does it become a {@link #CANDIDATE}.
     */
    PRE_CANDIDATE,
    CANDIDATE,
    LEADER
}
