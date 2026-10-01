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

## 3. Milestone 2: leader election (next)

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

## 4. Milestone 3: log replication (planned)

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
