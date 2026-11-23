package io.github.ncitakovic.raftkv.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.ncitakovic.raftkv.api.v1.RequestVoteRequest;
import io.github.ncitakovic.raftkv.api.v1.RequestVoteResponse;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.LongStream;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Leader election in a 3-node cluster in one JVM ({@link SimulatedCluster}): crashes, restarts and
 * network partitions on a fake clock. Each scenario runs with 50 seeds, i.e. 50 different orders of
 * message delays and election timeouts. Throughout every run, after every single step, the cluster
 * checks that no term ever has two leaders and that no node's term goes backwards.
 *
 * <p>Time limits are generous (seconds of simulated time for elections that normally take well
 * under one), so that a failure means "never converges", not "was a bit slow".
 */
class ClusterElectionTest {

    private static final List<String> IDS = List.of("a", "b", "c");
    private static final long ELECTION_LIMIT_MS = 5_000;

    @TempDir
    Path dir;

    static LongStream seeds() {
        return LongStream.rangeClosed(1, 50);
    }

    private SimulatedCluster cluster(long seed) {
        return new SimulatedCluster(dir, IDS, seed);
    }

    private static List<String> others(String id) {
        return IDS.stream().filter(other -> !other.equals(id)).toList();
    }

    // (a)
    @ParameterizedTest(name = "seed {0}")
    @MethodSource("seeds")
    void startElectsExactlyOneLeaderThatStays(long seed) {
        SimulatedCluster cluster = cluster(seed);
        cluster.startAll();

        String leader = cluster.awaitStableLeader(ELECTION_LIMIT_MS);
        long term = cluster.status(leader).term();

        // Heartbeats (every 50 ms) keep it in place: no new elections while nothing fails.
        cluster.run(2_000);
        assertEquals(Optional.of(leader), Optional.of(cluster.awaitStableLeader(0)), cluster.describe());
        assertEquals(term, cluster.status(leader).term(), "no election without a failure");
    }

    // (b)
    @ParameterizedTest(name = "seed {0}")
    @MethodSource("seeds")
    void crashedLeaderIsReplacedByANewLeaderInAHigherTerm(long seed) {
        SimulatedCluster cluster = cluster(seed);
        cluster.startAll();
        String oldLeader = cluster.awaitStableLeader(ELECTION_LIMIT_MS);
        long oldTerm = cluster.status(oldLeader).term();

        cluster.crash(oldLeader);
        String newLeader = cluster.awaitStableLeader(others(oldLeader), ELECTION_LIMIT_MS);

        assertNotEquals(oldLeader, newLeader);
        assertTrue(cluster.status(newLeader).term() > oldTerm, cluster.describe());

        // The old leader restarts from its data directory and joins as a follower.
        long newTerm = cluster.status(newLeader).term();
        cluster.start(oldLeader);
        assertEquals(newLeader, cluster.awaitStableLeader(ELECTION_LIMIT_MS));
        assertEquals(Role.FOLLOWER, cluster.status(oldLeader).role());
        assertEquals(newTerm, cluster.status(newLeader).term(), "the restart caused no election");
    }

    // (b') PreVote
    @ParameterizedTest(name = "seed {0}")
    @MethodSource("seeds")
    void restartedOldLeaderThatHearsNoHeartbeatsDoesNotDisruptTheNewLeader(long seed) {
        SimulatedCluster cluster = cluster(seed);
        cluster.startAll();
        String oldLeader = cluster.awaitStableLeader(ELECTION_LIMIT_MS);
        long oldTerm = cluster.status(oldLeader).term();
        cluster.crash(oldLeader);
        String newLeader = cluster.awaitStableLeader(others(oldLeader), ELECTION_LIMIT_MS);
        long newTerm = cluster.status(newLeader).term();

        // As seen in Docker: after the restart, the others' connections to it stay stuck for a while,
        // so no heartbeat reaches it, while its own new connections to them work. It times out again
        // and again (every 150-300 ms) and asks for PreVotes, which the others refuse: they hear the
        // leader. Without PreVote it would raise its term and force the new leader to step down.
        others(oldLeader).forEach(other -> cluster.blockRequests(other, oldLeader));
        cluster.start(oldLeader);
        cluster.run(2_000);

        assertEquals(new RaftStatus(Role.LEADER, newTerm, Optional.of(newLeader)), cluster.status(newLeader),
                cluster.describe());
        assertEquals(newTerm, cluster.highestLeaderTerm(), "no newer term");
        RaftStatus restarted = cluster.status(oldLeader);
        assertEquals(Role.PRE_CANDIDATE, restarted.role(), cluster.describe());
        assertEquals(oldTerm, restarted.term(), "it never raised its term");

        // Once heartbeats get through, it simply follows the new leader.
        cluster.healAll();
        assertEquals(newLeader, cluster.awaitStableLeader(ELECTION_LIMIT_MS));
        assertEquals(newTerm, cluster.status(oldLeader).term());
    }

    // (c)
    @ParameterizedTest(name = "seed {0}")
    @MethodSource("seeds")
    void partitionOneVersusTwoLoneNodeNeitherRaisesTheTermNorDisruptsTheLeader(long seed) {
        SimulatedCluster cluster = cluster(seed);
        cluster.startAll();
        String leader = cluster.awaitStableLeader(ELECTION_LIMIT_MS);
        long term = cluster.status(leader).term();
        String alone = others(leader).getFirst();

        cluster.isolate(alone);
        cluster.run(3_000);

        // The side with two nodes keeps its leader, in the same term.
        assertEquals(leader, cluster.awaitStableLeader(others(alone), 0));
        assertEquals(term, cluster.status(leader).term());
        // The lone node keeps timing out and asking for PreVotes that never arrive. Its term stays:
        // with PreVote, a node raises its term only after a majority said it could win.
        assertEquals(new RaftStatus(Role.PRE_CANDIDATE, term, Optional.empty()), cluster.status(alone),
                cluster.describe());
        cluster.leadersByTerm().values().forEach(leaders -> assertFalse(leaders.contains(alone), cluster.describe()));

        // After the partition heals, the lone node follows the same leader; nobody's term changed.
        cluster.healAll();
        assertEquals(leader, cluster.awaitStableLeader(ELECTION_LIMIT_MS));
        for (String id : IDS) {
            assertEquals(term, cluster.status(id).term(), cluster.describe());
        }
        assertEquals(term, cluster.highestLeaderTerm());
    }

    // (d)
    @ParameterizedTest(name = "seed {0}")
    @MethodSource("seeds")
    void isolatedOldLeaderStepsDownWhenItCanReachTheClusterAgain(long seed) {
        SimulatedCluster cluster = cluster(seed);
        cluster.startAll();
        String oldLeader = cluster.awaitStableLeader(ELECTION_LIMIT_MS);
        long oldTerm = cluster.status(oldLeader).term();

        cluster.isolate(oldLeader);
        String newLeader = cluster.awaitStableLeader(others(oldLeader), ELECTION_LIMIT_MS);
        long newTerm = cluster.status(newLeader).term();
        assertTrue(newTerm > oldTerm);
        // Cut off, the old leader does not know it was replaced: it still thinks it leads the old
        // term. That is allowed (one leader per term), and harmless: it cannot reach a majority.
        assertEquals(new RaftStatus(Role.LEADER, oldTerm, Optional.of(oldLeader)), cluster.status(oldLeader));

        cluster.healAll();
        // The first heartbeat from the new leader (or reply to its own) carries the newer term.
        cluster.run(SimulatedCluster.MAX_DELAY_MS * 2 + RaftConfig.defaults().heartbeatInterval().toMillis());

        RaftStatus old = cluster.status(oldLeader);
        assertEquals(Role.FOLLOWER, old.role(), cluster.describe());
        assertEquals(newTerm, old.term());
        assertEquals(newLeader, cluster.awaitStableLeader(ELECTION_LIMIT_MS));
    }

    // (e)
    @ParameterizedTest(name = "seed {0}")
    @MethodSource("seeds")
    void splitVoteIsResolvedInALaterRound(long seed) {
        // Every message takes exactly 5 ms here, so the timing below is exact; later election
        // timeouts are still random per seed.
        SimulatedCluster cluster = new SimulatedCluster(dir, IDS, seed, 5);
        // c is down. a and b time out at the same instant (t = 150) and ask each other for a PreVote.
        // Neither has a leader, so both say yes (t = 155), and at t = 160 both become candidates of
        // term 1 and vote for themselves. Each then refuses the other's real RequestVote (t = 165),
        // so neither gets 2 of 3 votes.
        cluster.start("a", 150L);
        cluster.start("b", 150L);
        cluster.run(160);
        assertEquals(new RaftStatus(Role.CANDIDATE, 1, Optional.empty()), cluster.status("a"));
        assertEquals(new RaftStatus(Role.CANDIDATE, 1, Optional.empty()), cluster.status("b"));

        // Random timeouts for the next rounds break the tie.
        String leader = cluster.awaitStableLeader(List.of("a", "b"), ELECTION_LIMIT_MS);

        assertFalse(cluster.leadersByTerm().containsKey(1L), "term 1 was a split vote: " + cluster.describe());
        assertTrue(cluster.status(leader).term() >= 2);

        cluster.start("c");
        assertEquals(leader, cluster.awaitStableLeader(ELECTION_LIMIT_MS));
    }

    // (f)
    @ParameterizedTest(name = "seed {0}")
    @MethodSource("seeds")
    void restartedNodeDoesNotVoteTwiceInTheSameTerm(long seed) throws Exception {
        SimulatedCluster cluster = cluster(seed);
        cluster.startAll();
        String leader = cluster.awaitStableLeader(ELECTION_LIMIT_MS);
        long term = cluster.status(leader).term();
        // The leader won with its own vote and at least one follower's, recorded on that follower's disk.
        String voter = others(leader).stream()
                .filter(id -> cluster.state(id).votedFor().equals(Optional.of(leader)))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no follower voted for " + leader + ": " + cluster.describe()));
        String rival = others(leader).stream().filter(id -> !id.equals(voter)).findFirst().orElseThrow();

        cluster.crash(voter);
        cluster.start(voter); // a new process, same data directory

        // The rival asks for the voter's vote in the same term, with an equally up-to-date log.
        CompletableFuture<RequestVoteResponse> reply = cluster.node(voter).onRequestVote(RequestVoteRequest.newBuilder()
                .setTerm(term).setCandidateId(rival).setLastLogIndex(0).setLastLogTerm(0).build());
        cluster.runPending();

        assertFalse(reply.get().getVoteGranted(), "the vote for " + leader + " survived the restart");
        assertEquals(Optional.of(leader), cluster.state(voter).votedFor());
        assertEquals(term, cluster.state(voter).currentTerm());

        // And the cluster carries on under the same leader.
        assertEquals(leader, cluster.awaitStableLeader(ELECTION_LIMIT_MS));
    }
}
