# RaftKV design

This document describes what RaftKV does today, how Raft will be added, and the trade-offs behind each decision. It is updated at every milestone.

## 1. Goals and non-goals

**Goals**

- **Linearizable writes.** Once a `Put` or `Delete` is acknowledged, every later read reflects it, even if nodes crash afterwards.
- **Availability under minority failure.** A cluster of `2f + 1` nodes keeps working with up to `f` nodes down or partitioned away (3 nodes tolerate 1 failure, 5 tolerate 2).
- **Understandable code.** Clarity is preferred over raw performance; every component is small enough to read in one sitting.
- **Tested under failure.** Correctness claims are backed by tests that kill nodes and cut network links.

**Non-goals (for now)**

- Multi-key transactions, sharding, or datasets larger than memory.
- Byzantine faults. Nodes are assumed to crash and restart, not to lie.
- Dynamic membership changes. Cluster size is fixed at startup.

## 2. Milestone 1: durable single node (done)

### Write path

1. `KeyValueService` validates the request (non-empty key ≤ 1 KiB, value ≤ 1 MiB).
2. `DurableKvStore.write` takes a lock, so writes are serialized and get consecutive indexes.
3. `WriteAheadLog.append` writes the record and calls `fsync` (`FileChannel.force`).
4. The entry is applied to `KvStateMachine`.
5. Only then is the client answered.

Because step 3 finishes before step 5, **every acknowledged write is on disk**. This is the same guarantee a Raft node needs before it can answer a leader's `AppendEntries`.

### Read path

Reads come straight from the in-memory map in `KvStateMachine`. On a single node this is trivially up to date. In a cluster it is not; see §4.4.

### Log format

Each record is `[length:int][crc32:int][payload]`, and the payload is `[index:long][type:byte][keyLen:int][key][valueLen:int][value]`.

- **Length prefix**: lets recovery skip from record to record without parsing payloads.
- **CRC32 over the payload**: detects torn writes (a crash in the middle of a record) and bit rot.
- **Explicit index**: recovery checks that indexes are contiguous, so a missing or reordered record is caught instead of silently producing wrong state.

### Crash recovery

On open, records are replayed from the start. Replay stops at the first record that is incomplete, has an impossible length, or fails its checksum. The file is truncated there and `fsync`'d.

Truncating is safe because a record that failed to reach disk intact was never `fsync`'d, so it was never acknowledged. Dropping it cannot lose a write a client was told succeeded.

### Log vs. state machine

The log is the source of truth; the map is derived from it. `KvStateMachine.apply` rejects entries that are out of order or applied twice, and applying the same log always produces the same state (there's a test for this). That determinism is the core assumption of replicated state machines: if every node applies the same commands in the same order, every node ends up with the same data.

### Trade-offs taken

| Decision | Alternative | Why |
|---|---|---|
| `fsync` on every write | Group commit (batch several writes per `fsync`) | Simplest correct option. Group commit is a planned optimization once benchmarks exist. |
| One global write lock | Lock-free append queue | Raft also needs a single ordering of writes, so the lock costs little now. |
| Whole dataset in memory | LSM tree or B-tree on disk | Keeps the focus on consensus rather than storage engines. |
| Log grows forever | Snapshots + compaction | Planned for milestone 6; restart time grows with log size until then. |
| Custom binary log format | Protobuf-encoded records | Fixed layout makes torn-write detection trivial and keeps the log independent of the API schema. |

## 3. Milestone 2: leader election (done)

New state on every node, **persisted before answering any RPC**: `currentTerm` and `votedFor`. If these were lost on restart, a node could vote twice in the same term and two leaders could be elected.

- Each node starts as a **follower** with a randomized election timeout (for example 150–300 ms).
- If it hears nothing from a leader before the timeout, it becomes a **candidate**: it increments `currentTerm`, votes for itself, and sends `RequestVote` to the others.
- A node grants its vote only if it hasn't voted in this term and the candidate's log is **at least as up to date** as its own (compare last log term, then last index). This rule is what stops a node with missing entries from becoming leader.
- A candidate with votes from a majority becomes **leader** and sends heartbeats (empty `AppendEntries`) to hold its position.
- Any RPC carrying a higher term makes the receiver step down to follower.

Randomized timeouts make split votes rare, and when one happens a new election with new random timeouts resolves it.

Planned node-to-node API (`raft.proto`): `RequestVote(term, candidateId, lastLogIndex, lastLogTerm) → (term, voteGranted)`.

### 3.1 Persistent term and vote (done)

`PersistentState` stores `currentTerm` and `votedFor` in a small file, `raft-state`.

**Why on disk, and before replying.** Take three nodes A, B and C. In term 5, A votes for B, and B now has 2 of 3 votes, so it becomes leader. If A's vote lived only in memory and A restarted, A would forget it, and could then vote for C in the same term 5. C would also have 2 of 3 votes, giving two leaders in one term (split brain). Writing the vote to disk before answering makes "at most one vote per term" survive restarts. A test (`restartedNodeCannotVoteTwiceInTheSameTerm`) reproduces exactly this scenario.

**Atomic overwrite.** The log is append-only, but this file is replaced on each change, and replacing it in place would leave a half-written file after a crash. Each update writes `raft-state.tmp`, fsyncs it, atomically renames it over `raft-state`, and fsyncs the directory. The disk therefore always holds the complete old state or the complete new state. A leftover `.tmp` file at startup means the crash happened before the rename, so it is discarded and the old file is kept.

**Corruption fails loudly.** The log may drop a torn tail record because it was never acknowledged. Here the reverse holds: silently resetting to "term 0, no vote" could let the node vote twice. A state file that fails its CRC32 check stops the node with an error instead.

**API.** `advanceTerm(t)` moves to a higher term and clears the vote; `voteFor(id)` records a vote, allowing a repeat for the same candidate but rejecting a different one; `startElection(self)` increments the term and votes for itself in a single atomic write, so a crash can never leave the node in a new term without its own vote recorded.

### 3.2 Node-to-node API (done)

[`raft.proto`](../src/main/proto/raft.proto) defines the `Raft` gRPC service that nodes use to talk to each other. It lives next to the client-facing `KeyValue` service in the same `raftkv.v1` package, but only cluster members call it.

- `RequestVote(term, candidate_id, last_log_index, last_log_term) → (term, vote_granted)`
- `AppendEntries(term, leader_id) → (term, success)`

Every response carries the receiver's `term`, so a node that is behind (a stale candidate or a deposed leader) learns about the newer term from any reply and steps down. Node IDs are strings, matching `votedFor` in `PersistentState`. Terms and indexes are `uint64`; index 0 and term 0 mean "empty log".

**`AppendEntries` is only a heartbeat for now.** Milestone 2 needs it to keep followers from starting elections, nothing more. The replication fields (`prev_log_index`, `prev_log_term`, `entries`, `leader_commit`) are documented in the proto as a comment and will be added as fields 3–6 in milestone 3. Adding fields later is backward compatible in protobuf, so nodes do not need a new API version.

This step only defines the messages and generates the Java stubs. The service is not implemented or registered on the server yet.

### 3.3 Vote rules (done)

`VoteHandler.handle` answers a `RequestVote` (Raft paper, Figure 2). It first rejects malformed requests (`term` 0 or an empty `candidate_id`, which is what proto3 produces when the field is missing) without touching any state, so a bad request can never use up this node's vote. Then:

1. **Stale term:** if the request's term is lower than `currentTerm`, refuse.
2. **Newer term:** if it is higher, move to that term first (`advanceTerm`, which clears the vote). The node stays in the new term even if it then refuses the vote.
3. **One vote per term:** refuse if this node already voted for a different candidate in this term. A repeated request from the same candidate is granted again, because the RPC may have been retried.
4. **Election restriction (§5.4.1):** grant only if the candidate's log is at least as up to date: the higher last log term wins, and with equal terms the longer (or equal) log wins.

A granted vote is fsync'd by `PersistentState` before `handle` returns. Every response carries the node's term after processing. The caller resets its election timer only when `vote_granted` is true. Log entries have no term yet, so the production constructor uses 0 as the last log term until milestone 3.

**Known follow-ups**

- One fsync for term and vote together: a higher-term request that is granted now costs two writes (`advanceTerm`, then `voteFor`). This is safe but slower; a combined atomic write, like `startElection`, is an optimization.
- Read the last log entry's index and term in a single call (milestone 3), so the two values always come from the same log state.

### 3.4 Leader election state machine (done)

`RaftNode` implements the FOLLOWER → CANDIDATE → LEADER diagram (Raft paper, Figure 4). `AppendEntries` is only a heartbeat; log replication comes in milestone 3.

**One thread, injected time.** Every change to Raft state runs as a task on a `Scheduler`, which runs tasks one at a time on a single thread. Incoming RPCs (`onRequestVote`, `onAppendEntries`) and the responses the `RaftTransport` returns as `CompletableFuture`s are all handed to that thread, so no locks are needed. The scheduler is also the only clock: production uses `ExecutorScheduler` (a single-threaded `ScheduledExecutorService`), while tests use a `FakeScheduler` whose time moves only when the test calls `advance(ms)`. Election timeouts come from an injected `RandomGenerator`, so tests know each timeout exactly. Other threads read a published, immutable `RaftStatus` (role, term, known leader).

- **Follower:** waits a random election timeout, 150–300 ms. The timer is reset **only** by a valid `AppendEntries` from the leader of the current term (or a newer one), or by granting a vote. A refused `RequestVote` or a stale `AppendEntries` does not reset it, so they cannot keep a node from noticing a dead leader.
- **Candidate:** `PersistentState.startElection` raises the term and records its own vote in one fsync'd write, before any `RequestVote` goes out. The request carries the last log index and term. Votes are counted only if they answer the current round (same term, still a candidate), so a late vote from an earlier round never counts. Without a majority, the next timeout starts a new round. (Since §3.6, every election is preceded by a PreVote round.)
- **Leader:** with a majority (its own vote included) it sends heartbeats to every peer at once, then every 50 ms. Votes that arrive after it won are ignored.
- **Stepping down:** any request or response with a higher term moves the node to that term (`advanceTerm`) and makes it a follower, cancelling the heartbeat timer. For an incoming `RequestVote`, `VoteHandler` may raise the term, so `RaftNode` compares `currentTerm` before and after; a vote can be refused and the node still steps down. A valid `AppendEntries` makes a candidate or leader a follower of its sender.
- **Timer on stepping down:** a candidate keeps its running election timer; a leader has none, so it gets a new one, or it would never start another election.

**Fail-stop on disk errors.** If the term or vote cannot be written, the node logs the error and stops: no reply is sent and no `RequestVote` goes out. Continuing without a durable vote could let it vote twice in one term.

Role changes are logged as `node {id} became {ROLE} in term {term}`. A single-node cluster becomes leader on its own vote.

The gRPC `Raft` service, the gRPC `RaftTransport` and the wiring into `RaftKvServer` are in §3.7.

### 3.5 Cluster simulation tests (done)

`ClusterElectionTest` runs three `RaftNode`s in one JVM through `SimulatedCluster`: one `FakeScheduler` clock and an in-memory network in which every request and response takes a random 1–10 ms. A message to a node that is down, or over a cut link, fails like a refused connection. A crashed node is restarted as a new `RaftNode` over the **same data directory**, so it remembers only what it had fsync'd. One seed drives every message delay and election timeout, so a failing seed replays exactly.

**Invariants, checked after every simulated step** (every timer, RPC and response), not only at the end:

- **Election Safety:** at most one leader per term, across the whole run. Two nodes that lead the same term at different times are also caught.
- **Terms never decrease** for a node, including across restarts.

**Scenarios**, each run with 50 seeds (300 runs, about 20 s):

| | Scenario | Checks |
|---|---|---|
| a | Start 3 nodes | Exactly one leader that all agree on; no new election in the next 2 s |
| b | Crash the leader | The other two elect a new leader in a higher term; the old one restarts as a follower and the term does not change |
| c | Partition 1 + 2 | The pair keeps a leader; the lone node is never leader; after healing, one leader again |
| d | Isolate the leader, then heal | While cut off it still thinks it leads the old term (allowed and harmless); after healing it becomes a follower in the new term |
| e | Split vote | With the third node down, two nodes time out at the same instant, vote for themselves and refuse each other; term 1 has no leader and a later round elects one |
| f | Restart a voter | After a restart, a node that voted for the leader refuses a rival in the same term |

**The tests catch bugs.** With the "already voted for someone else" check removed from `VoteHandler`, 144 of the 300 runs fail, because `PersistentState.voteFor` still refuses a second vote and throws. With that guard removed as well, 126 runs fail on the invariant itself, for example `Two leaders in term 1: [b, c] at t=163ms`. Neither change was committed.

PreVote (§3.6) later changed scenarios c and e and added one more; the table there is current.

### 3.6 PreVote (done)

**Problem.** Running the cluster in Docker showed that restarting a stopped leader often moved leadership back to it. Docker drops a stopped container's address, so for 0.1–0.6 s after the restart the other nodes' connections to it were still stuck and no heartbeat reached it. Its first election timeout (150–300 ms) expired, it raised its term and asked for votes, and the healthy leader, seeing a higher term, stepped down. A node cut off by a partition did the same when it came back. Nothing unsafe happened, but every such return cost an election with no leader in between.

**Solution** (Ongaro, PhD thesis §9.6). When its election timeout expires, a node first becomes a **pre-candidate**. It sends `RequestVote` with `pre_vote = true` and `term = currentTerm + 1`, "would you vote for me?", without changing its own term or vote.

- **Receiver:** changes nothing (term, `votedFor`, election timer) and says yes only if (a) it has not heard a valid `AppendEntries` from a leader within the minimum election timeout (150 ms), and (b) the sender's log is at least as up to date as its own (the same check as a real vote). A leader always says no.
- **Sender:** with a majority of yeses (its own included) it becomes a candidate and runs a real election, which works exactly as before. Without a majority, the next timeout starts another PreVote round **in the same term**. A PreVote round is matched to its replies by a round counter, because the term does not change.
- **Replies do not move terms.** A PreVote reply carrying a higher term does not make the sender a follower; only real RPCs apply "higher term → follower". The sender learns the newer term from the leader's next heartbeat or from a real `RequestVote`. (etcd adopts the term from a refused PreVote reply; either choice is safe, because terms only affect safety through real votes.)
- **Real votes are unchanged.** `VoteHandler.handle` keeps the same rules and refuses a PreVote; `handlePreVote` writes nothing to disk.

**Why it works.** A node can now raise its term only if a majority would vote for it, which needs a majority that has lost contact with the leader. A restarted or reconnected node whose peers still hear the leader stays a pre-candidate in its old term until the leader's heartbeat reaches it, and then it follows. Rule (a) needs a clock, so `Scheduler` got `nowMillis()` (monotonic; `FakeScheduler` time in tests). `Role` got `PRE_CANDIDATE`, and the log shows `node {id} became PRE_CANDIDATE in term {term}`.

**Tests.** `VoteHandlerTest`: a PreVote changes neither term nor vote, also on disk after a restart; it is refused while a leader is active, and uses the same log check. `RaftNodeTest`: a timeout leads to a PreVote before `startElection`; without a majority the term never grows, even after replies with a higher term; a late yes from an earlier round does not count; a follower that heard the leader within 150 ms refuses without resetting its timer; a leader always refuses. Cluster scenarios (50 seeds each, 350 runs in total):

| | Scenario | Checks |
|---|---|---|
| b′ | Crash the leader, wait for a new one, restart the old one with the others' requests to it blocked for 2 s | The new leader stays leader and its term does not change; the restarted node stays a pre-candidate in its old term, then follows |
| c | Partition 1 + 2, then heal | The lone node does not raise its term and does not disrupt the leader; after healing, every node is in the original term under the same leader |
| e | Split vote | Both nodes pass the PreVote at the same instant (fixed 5 ms delays), then split the real vote as before |

**The tests catch the bug.** With the "heard from a leader recently" check removed, scenario b′ fails for all 50 seeds (the restarted node wins term 4 and the new leader of term 3 steps down), scenario c for 11 seeds, and one `RaftNodeTest`. The change was not committed.

### 3.7 Running a real cluster (done)

**Configuration.** `RAFTKV_PEERS` (e.g. `b=raftkv-b:50052,c=raftkv-c:50052`), `RAFTKV_NODE_ID` and `RAFTKV_RAFT_PORT` (default 50052) are parsed by `ClusterConfig`, which rejects malformed entries, duplicate IDs and a node listed as its own peer. Without `RAFTKV_PEERS` the server runs exactly as in milestone 1: no Raft, no `raft-state` file.

**Wiring (`RaftKvServer`).** In a cluster, a node opens `PersistentState` in its data directory (next to the log), creates a `RaftNode` on an `ExecutorScheduler` thread named `raft-{id}`, and serves the `Raft` gRPC service on its own port, separate from the client API. The last log index comes from `DurableKvStore::lastIndex`, which is now `synchronized`: the Raft thread reads it while writes run on gRPC threads. The last log term is 0 until log entries carry a term (milestone 3).

**`RaftService`** (server side) has no logic: it hands each RPC to `RaftNode`, which handles it on the Raft thread, so gRPC threads never touch Raft state. If the node cannot answer (stopped, or a failed persist), the call fails with `UNAVAILABLE`.

**`GrpcRaftTransport`** (client side) keeps one plaintext channel per peer and puts a **100 ms deadline** on every call (below the 150 ms minimum election timeout). A peer that never answers becomes a failed call, which `RaftNode` treats as a lost message.

**Reconnecting after a restart.** Docker showed three delays between a peer coming back and the leader reaching it. Each one is now handled:

| Cause | Effect seen | Fix |
|---|---|---|
| The JVM remembers a failed DNS lookup for 10 s; a stopped container's name does not resolve | Leader could not resolve the restarted peer for up to 10 s | DNS caching off (`networkaddress.cache.ttl` and `.negative.ttl` = 0) |
| A connection attempt to a stopped container's address hangs for seconds instead of failing | Restarted node got no heartbeat for ~5 s (it stayed a pre-candidate thanks to PreVote) | Connect timeout of 250 ms per attempt |
| gRPC waits longer and longer between reconnect attempts | Slow recovery after a long outage | `resetConnectBackoff()` after every `UNAVAILABLE` or `DEADLINE_EXCEEDED` |

Before PreVote and these fixes, a leader restarted after 13 s took leadership back in every trial. Now, in three trials of stop leader, wait 13 s, start it again, the restarted node followed the new leader within about 0.2 s, and the new leader kept its term. The time from stopping a leader to a new one is about 0.25 s:

```
19:25:28.454 [shutdown] RaftKvServer - RaftKV stopped                  # leader c stopped
19:25:28.656 [raft-b] node b became PRE_CANDIDATE in term 2
19:25:28.684 [raft-b] node b became CANDIDATE in term 3
19:25:28.717 [raft-b] node b became LEADER in term 3                   # 0.26 s later
19:25:44.464 [main] PersistentState - Loaded Raft state: term 2, votedFor c
19:25:44.598 [raft-c] node c became FOLLOWER in term 2
19:25:44.755 [raft-c] node c became PRE_CANDIDATE in term 2            # no heartbeat yet; term stays
19:25:44.788 [raft-c] node c became FOLLOWER in term 3                 # follows b; no election
```

**Known startup cost.** On a cold start the first RPCs take longer than the 100 ms deadline (connection setup, class loading), so the first few PreVote rounds fail and a leader appears after about 1 s. PreVote keeps those failed rounds from raising the term.

**Logging.** slf4j-simple with timestamps (`HH:mm:ss.SSS`), so election timing can be read from the logs.

## 4. Milestone 3: log replication (next)

### 4.1 Log entries gain a term

Every entry will carry the term in which the leader created it. The on-disk format gets a version byte so milestone-1 logs can still be read.

### 4.2 `AppendEntries`

The leader sends `(term, leaderId, prevLogIndex, prevLogTerm, entries[], leaderCommit)`. A follower rejects the call if it has no entry at `prevLogIndex` with term `prevLogTerm`. The leader then steps back through its log for that follower until they match, and the follower **deletes its conflicting suffix** and appends the leader's entries. This is why the log needs a truncate-from-index operation.

### 4.3 Commit

An entry is **committed** once it is stored on a majority. The leader only counts replicas for entries **from its own current term** (Raft paper §5.4.2); older entries become committed indirectly. Only committed entries are applied to the state machine, and only then is the client answered.

### 4.4 Reads

Serving reads from any node's map can return stale data (for example, from a deposed leader that doesn't know it's been replaced yet). Planned approach: send reads to the leader and use the **ReadIndex** protocol, where the leader confirms it is still leader by getting heartbeat acknowledgements from a majority before answering. Lease-based reads are faster but rely on bounded clock drift, so they're left as a possible later optimization.

### 4.5 Clients

Followers reply to writes with `FAILED_PRECONDITION` plus a leader hint, and the client retries against the leader. Retrying a write after a timeout can apply it twice; the fix (client IDs with sequence numbers, deduplicated in the state machine) is noted as future work.

## 5. Milestone 4: testing under failure (planned)

- **In-process cluster**: 3–5 nodes in one JVM, connected through a fake network layer that can drop, delay or reorder messages and partition nodes into groups.
- **Scenarios**: kill the leader during writes; partition the leader into a minority; restart nodes with their logs; repeat many times with random seeds.
- **Checks**: at most one leader per term; every acknowledged write is readable afterwards; all nodes' applied logs are prefixes of each other.
- **Container level**: Docker Compose cluster with `docker kill` and `docker network disconnect` for end-to-end checks.

## 6. Milestone 5: Kubernetes (planned)

- **StatefulSet**: stable pod names (`raftkv-0`, `raftkv-1`, …) give each node a fixed identity and peer address through a headless Service.
- **PersistentVolumeClaim per pod**: the log and term/vote state must survive pod rescheduling.
- **Probes**: the built-in gRPC health service backs Kubernetes gRPC liveness and readiness probes.
- **PodDisruptionBudget**: stops voluntary disruptions (node drains) from taking down a majority at once.
- **Hardening**: non-root user (already in the image), read-only root filesystem, dropped Linux capabilities, resource limits.

## 7. Milestone 6: snapshots and benchmarks (planned)

- Periodically write the state machine to a snapshot file, then discard log entries it covers. Followers that fall too far behind receive the snapshot (`InstallSnapshot`).
- Measure write throughput and latency (p50/p99) for 1, 3 and 5 nodes, and **failover time**: from killing the leader to the first successful write on the new one.
- Try group commit and compare `fsync` cost before and after.

## 8. References

- Diego Ongaro and John Ousterhout, *In Search of an Understandable Consensus Algorithm (Extended Version)*, 2014. <https://raft.github.io/raft.pdf>
- Diego Ongaro, *Consensus: Bridging Theory and Practice* (PhD thesis), 2014. ReadIndex, client sessions, membership changes.
- The Raft visualization at <https://raft.github.io/>
