package io.github.ncitakovic.raftkv.raft;

import io.github.ncitakovic.raftkv.api.v1.AppendEntriesRequest;
import io.github.ncitakovic.raftkv.api.v1.AppendEntriesResponse;
import io.github.ncitakovic.raftkv.api.v1.RequestVoteRequest;
import io.github.ncitakovic.raftkv.api.v1.RequestVoteResponse;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.random.RandomGenerator;
import java.util.stream.Collectors;

/**
 * Several {@link RaftNode}s in one JVM, on one {@link FakeScheduler} clock, connected by a
 * simulated in-memory network. Everything is driven by one seed: message delays and every node's
 * election timeouts. The same seed always replays the same run, so a failing seed can be debugged.
 *
 * <p><b>Network.</b> Each request and each response takes a random 1–10 ms. A message to a node that
 * is down, or over a link that is cut, fails (like a refused connection); a response is also lost if
 * the link is cut or the sender restarted while it was in flight. Nodes never share objects: they
 * only see each other's protobuf messages.
 *
 * <p><b>Crash and restart.</b> {@link #crash} stops a node and drops it from the network. {@link #start}
 * after a crash creates a new {@link RaftNode} and {@link PersistentState} over the <i>same</i> data
 * directory, so the node remembers exactly what it had fsync'd: its term and vote.
 *
 * <p><b>Invariants checked after every task</b> (every timer, RPC and response):
 *
 * <ul>
 *   <li><b>Election Safety</b> (Raft paper, Figure 3): at most one leader per term, over the whole
 *       history, not only at one instant. Two nodes that are leader of the same term at different
 *       times are caught too.
 *   <li><b>Terms never decrease</b> for a node, including across restarts.
 * </ul>
 */
final class SimulatedCluster {

    static final int MIN_DELAY_MS = 1;
    static final int MAX_DELAY_MS = 10;

    private final Path baseDir;
    private final List<String> ids;
    private final RaftConfig config = RaftConfig.defaults();
    private final FakeScheduler scheduler = new FakeScheduler();
    private final SplittableRandom networkRandom;
    private final Map<String, SplittableRandom> nodeRandoms = new HashMap<>();

    private final Map<String, Member> running = new LinkedHashMap<>();
    private final Map<String, Integer> incarnations = new HashMap<>();
    private final Set<Set<String>> cutLinks = new HashSet<>();

    private final Map<Long, Set<String>> leadersByTerm = new TreeMap<>();
    private final Map<String, Long> highestTermSeen = new HashMap<>();

    private record Member(RaftNode node, PersistentState state) {}

    SimulatedCluster(Path baseDir, List<String> ids, long seed) {
        this.baseDir = baseDir;
        this.ids = List.copyOf(ids);
        SplittableRandom root = new SplittableRandom(seed);
        this.networkRandom = root.split();
        for (String id : this.ids) {
            nodeRandoms.put(id, root.split());
        }
        scheduler.afterEachTask(this::checkInvariants);
    }

    // ---- lifecycle ----

    void startAll() {
        ids.forEach(this::start);
    }

    /** Starts {@code id}, or restarts it after a crash, over its own data directory. */
    void start(String id) {
        start(id, null);
    }

    /**
     * Starts {@code id} with a fixed first election timeout, to make two nodes time out at the same
     * instant. Later timeouts come from the seeded random source as usual.
     */
    void start(String id, Long firstElectionTimeoutMs) {
        if (running.containsKey(id)) {
            throw new IllegalStateException(id + " is already running");
        }
        PersistentState state;
        try {
            state = PersistentState.open(baseDir.resolve(id));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        RandomGenerator random = nodeRandoms.get(id);
        if (firstElectionTimeoutMs != null) {
            random = new FirstValueThen(firstElectionTimeoutMs, random);
        }
        List<String> peers = ids.stream().filter(p -> !p.equals(id)).toList();
        // Empty logs everywhere: milestone 2 has no replication, so the log check always passes.
        VoteHandler votes = new VoteHandler(state, () -> 0, () -> 0);
        RaftNode node = new RaftNode(id, peers, state, votes, () -> 0, () -> 0, new NodeTransport(id), scheduler,
                random, config);
        incarnations.merge(id, 1, Integer::sum);
        running.put(id, new Member(node, state));
        node.start();
        scheduler.runPending();
    }

    /** Kills {@code id}: its timers stop and it no longer sends or receives anything. */
    void crash(String id) {
        Member member = running.remove(id);
        if (member == null) {
            throw new IllegalStateException(id + " is not running");
        }
        member.node().stop();
        scheduler.runPending();
    }

    // ---- network faults ----

    void disconnect(String a, String b) {
        cutLinks.add(Set.of(a, b));
    }

    /** Cuts every link between {@code id} and the other nodes. */
    void isolate(String id) {
        ids.stream().filter(other -> !other.equals(id)).forEach(other -> disconnect(id, other));
    }

    void healAll() {
        cutLinks.clear();
    }

    private boolean connected(String a, String b) {
        return !cutLinks.contains(Set.of(a, b));
    }

    // ---- time ----

    void run(long ms) {
        scheduler.advance(ms);
    }

    long now() {
        return scheduler.now();
    }

    /**
     * Runs until the nodes in {@code group} agree on one leader: exactly one of them is LEADER, and
     * all others are its followers in the same term. Fails after {@code maxMs} of simulated time.
     */
    String awaitStableLeader(Collection<String> group, long maxMs) {
        long deadline = now() + maxMs;
        while (true) {
            Optional<String> leader = agreedLeader(group);
            if (leader.isPresent()) {
                return leader.get();
            }
            if (now() >= deadline) {
                throw new AssertionError("No agreed leader among " + group + " after " + maxMs + " ms: " + describe());
            }
            run(5);
        }
    }

    String awaitStableLeader(long maxMs) {
        return awaitStableLeader(running.keySet(), maxMs);
    }

    private Optional<String> agreedLeader(Collection<String> group) {
        List<String> leaders = new ArrayList<>();
        for (String id : group) {
            Member m = running.get(id);
            if (m != null && m.node().status().role() == Role.LEADER) {
                leaders.add(id);
            }
        }
        if (leaders.size() != 1) {
            return Optional.empty();
        }
        String leader = leaders.getFirst();
        long term = status(leader).term();
        for (String id : group) {
            if (id.equals(leader)) {
                continue;
            }
            RaftStatus s = status(id);
            if (s.role() != Role.FOLLOWER || s.term() != term || !s.leaderId().equals(Optional.of(leader))) {
                return Optional.empty();
            }
        }
        return Optional.of(leader);
    }

    // ---- inspection ----

    RaftStatus status(String id) {
        Member m = running.get(id);
        if (m == null) {
            throw new IllegalStateException(id + " is not running");
        }
        return m.node().status();
    }

    PersistentState state(String id) {
        return running.get(id).state();
    }

    RaftNode node(String id) {
        return running.get(id).node();
    }

    List<String> ids() {
        return ids;
    }

    List<String> runningIds() {
        return List.copyOf(running.keySet());
    }

    /** Every node that was ever leader, by term. */
    Map<Long, Set<String>> leadersByTerm() {
        return leadersByTerm;
    }

    /** Lets a test run RPCs it sends itself to completion. */
    void runPending() {
        scheduler.runPending();
    }

    String describe() {
        return "t=" + now() + "ms " + running.entrySet().stream()
                .map(e -> e.getKey() + "=" + e.getValue().node().status())
                .collect(Collectors.joining(", ", "{", "}"))
                + " leadersByTerm=" + leadersByTerm;
    }

    // ---- invariants ----

    private void checkInvariants() {
        for (var e : running.entrySet()) {
            String id = e.getKey();
            RaftStatus s = e.getValue().node().status();
            long previous = highestTermSeen.getOrDefault(id, 0L);
            if (s.term() < previous) {
                throw new AssertionError("Term of " + id + " went back from " + previous + " to " + s.term()
                        + ": " + describe());
            }
            highestTermSeen.put(id, s.term());
            if (s.role() == Role.LEADER) {
                Set<String> leaders = leadersByTerm.computeIfAbsent(s.term(), t -> new TreeSet<>());
                leaders.add(id);
                if (leaders.size() > 1) {
                    throw new AssertionError("Two leaders in term " + s.term() + ": " + leaders + " at " + describe());
                }
            }
        }
    }

    // ---- simulated network ----

    private int delay() {
        return networkRandom.nextInt(MIN_DELAY_MS, MAX_DELAY_MS + 1);
    }

    /**
     * Delivers one RPC from {@code from} to {@code to} after a network delay, and its response back
     * after another. All of it runs on the scheduler, like everything else in the simulation.
     */
    private <R> CompletableFuture<R> deliver(String from, String to, Function<RaftNode, CompletableFuture<R>> call) {
        CompletableFuture<R> result = new CompletableFuture<>();
        int senderIncarnation = incarnations.get(from);
        scheduler.schedule(Duration.ofMillis(delay()), () -> {
            Member target = running.get(to);
            if (target == null || !connected(from, to)) {
                result.completeExceptionally(new IOException(to + " is unreachable from " + from));
                return;
            }
            call.apply(target.node()).whenComplete((response, error) ->
                    scheduler.schedule(Duration.ofMillis(delay()), () -> {
                        if (error != null) {
                            result.completeExceptionally(error);
                        } else if (!connected(from, to) || !running.containsKey(from)
                                || incarnations.get(from) != senderIncarnation) {
                            result.completeExceptionally(new IOException("response from " + to + " was lost"));
                        } else {
                            result.complete(response);
                        }
                    }));
        });
        return result;
    }

    private final class NodeTransport implements RaftTransport {
        private final String from;

        NodeTransport(String from) {
            this.from = from;
        }

        @Override
        public CompletableFuture<RequestVoteResponse> requestVote(String peerId, RequestVoteRequest request) {
            return deliver(from, peerId, node -> node.onRequestVote(request));
        }

        @Override
        public CompletableFuture<AppendEntriesResponse> appendEntries(String peerId, AppendEntriesRequest request) {
            return deliver(from, peerId, node -> node.onAppendEntries(request));
        }
    }

    /** Returns {@code first} for the first draw, then delegates to {@code rest}. */
    private static final class FirstValueThen implements RandomGenerator {
        private final long first;
        private final RandomGenerator rest;
        private boolean used;

        FirstValueThen(long first, RandomGenerator rest) {
            this.first = first;
            this.rest = rest;
        }

        @Override
        public long nextLong(long origin, long bound) {
            if (!used) {
                used = true;
                return first;
            }
            return rest.nextLong(origin, bound);
        }

        @Override
        public long nextLong() {
            return rest.nextLong();
        }
    }
}
