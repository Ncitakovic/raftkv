# RaftKV

[![CI](https://github.com/Ncitakovic/raftkv/actions/workflows/ci.yml/badge.svg)](https://github.com/Ncitakovic/raftkv/actions/workflows/ci.yml)

A replicated key-value store in Java, built step by step on the [Raft consensus algorithm](https://raft.github.io/raft.pdf).

The goal is a small cluster (3–5 nodes) that keeps accepting reads and writes when a minority of nodes crash or get cut off from the network, **without ever losing a write it has acknowledged**. The project is a learning vehicle for distributed-systems fundamentals: consensus, replication, durability, failure detection, and testing under failure.

> **Status: work in progress — milestones 1 and 2 of 6 complete.** Today RaftKV is a durable store with a gRPC API, and a cluster of nodes elects a leader with Raft. Log replication comes next: until then, every node serves the key-value API from its own data. See the [roadmap](#roadmap).

## What works today

- **Leader election (Raft)**: nodes exchange `RequestVote`/`AppendEntries` over gRPC ([`raft.proto`](src/main/proto/raft.proto)), elect one leader per term with randomized timeouts, and replace a failed leader in about 0.25 s. The term and vote are fsync'd before any reply. PreVote keeps a restarted or reconnected node from disrupting a healthy leader.
- **Simulation tests**: 3-node clusters in one JVM on a fake clock, with crashes, restarts and partitions, checking "at most one leader per term" after every step, 50 seeds per scenario.
- **gRPC API**: `Put`, `Get`, `Delete` ([`kv.proto`](src/main/proto/kv.proto)), plus the standard gRPC health and reflection services.
- **Write-ahead log**: every write is appended to a checksummed log and `fsync`'d *before* it is acknowledged.
- **Crash recovery**: on startup, the log is replayed to rebuild state. A torn or corrupted record at the tail (from a crash mid-write) is detected by CRC32 and truncated.
- **Log + state machine split**: writes go *log → state machine → client*, the exact path a Raft leader will use, so replication slots in without changing the storage format.
- **Container-ready**: multi-stage Docker image running as a non-root user, configured by environment variables.
- **Tests**: log recovery (torn and corrupted records), state-machine determinism, restart durability, and concurrent writers.

## Quick start

Requirements: Java 21 (Maven is downloaded by the included wrapper), or just Docker.

```bash
# Build and run the tests
./mvnw verify                            # on Windows: mvnw.cmd verify

# Run locally
java -jar target/raftkv.jar            # listens on :50051, stores data in ./data

# Or with Docker
docker compose up --build
```

Talk to it with [grpcurl](https://github.com/fullstorydev/grpcurl) (reflection is enabled, so no proto file is needed):

```bash
# Values are bytes, so they are base64 in JSON ("Tmlrb2xh" = "Nikola")
grpcurl -plaintext -d '{"key":"user:1","value":"Tmlrb2xh"}' localhost:50051 raftkv.v1.KeyValue/Put
grpcurl -plaintext -d '{"key":"user:1"}'                       localhost:50051 raftkv.v1.KeyValue/Get
grpcurl -plaintext -d '{"key":"user:1"}'                       localhost:50051 raftkv.v1.KeyValue/Delete
grpcurl -plaintext localhost:50051 grpc.health.v1.Health/Check
```

Restart the server (or the container) and the data is still there.

### Configuration

| Variable           | Default  | Meaning |
|--------------------|----------|---------|
| `RAFTKV_PORT`      | `50051`  | Client gRPC port (`KeyValue` API) |
| `RAFTKV_DATA_DIR`  | `./data` | Directory holding the log and the Raft state (`raft-state`) |
| `RAFTKV_PEERS`     | (unset)  | Other cluster members, e.g. `b=raftkv-b:50052,c=raftkv-c:50052`. Unset: a single node without Raft |
| `RAFTKV_NODE_ID`   | (unset)  | This node's ID; required with `RAFTKV_PEERS` |
| `RAFTKV_RAFT_PORT` | `50052`  | Node-to-node gRPC port (`Raft` service) |

## Run a 3-node cluster

[`docker-compose.cluster.yml`](docker-compose.cluster.yml) starts nodes `a`, `b` and `c` (containers `raftkv-a`, `raftkv-b`, `raftkv-c`), each with its own data volume. Every role change is logged as `node {id} became {ROLE} in term {term}`.

```bash
docker compose -f docker-compose.cluster.yml up --build -d

# Watch the elections (Ctrl+C stops watching, not the cluster)
docker compose -f docker-compose.cluster.yml logs -f | grep became
```

Within about a second exactly one node logs `became LEADER`:

```
node a became PRE_CANDIDATE in term 0     # timed out: "would you vote for me?"
node a became CANDIDATE in term 1         # a majority said yes: real election
node b became FOLLOWER in term 1
node a became LEADER in term 1
```

Kill the leader and watch the others elect a new one in a higher term (here `a` was the leader):

```bash
docker stop raftkv-a
#   node b became PRE_CANDIDATE in term 1
#   node b became CANDIDATE in term 2
#   node c became FOLLOWER in term 2
#   node b became LEADER in term 2
```

Bring the old leader back. It remembers its old term and vote from disk, and follows the new leader; there is no new election:

```bash
docker start raftkv-a
#   PersistentState - Loaded Raft state: term 1, votedFor a
#   node a became FOLLOWER in term 1          # then follows b in term 2
```

Stop and remove everything, including the data volumes:

```bash
docker compose -f docker-compose.cluster.yml down -v
```

Writes are not replicated yet (milestone 3): the client API is published on ports 50051 (a), 50061 (b) and 50071 (c), and each node serves its own data.

## Architecture

```
            ┌──────────────── RaftKV node ────────────────┐
 client ──► │ KeyValueService (gRPC, validation)          │
   gRPC     │        │ write                  ▲ read      │
            │        ▼                        │           │
            │ WriteAheadLog ──apply──► KvStateMachine     │
            │ (append + fsync)          (in-memory map)   │
            └──────────────────────────────────────────────┘
```

| Package    | Responsibility |
|------------|----------------|
| `log`      | `Command`, `LogEntry`, and `WriteAheadLog`: the durable, checksummed, append-only log. Will become the Raft log. |
| `store`    | `KvStateMachine` (deterministic apply of entries, in order, exactly once) and `DurableKvStore` (single-node write path). |
| `raft`     | Leader election: `RaftNode` (follower → pre-candidate → candidate → leader, on one thread), `VoteHandler` (vote rules), `PersistentState` (durable term and vote), `Scheduler` (the Raft thread and clock), `ClusterConfig`. |
| `server`   | `KeyValueService`: gRPC endpoints, input limits, mapping failures to gRPC status codes. `RaftService` and `GrpcRaftTransport`: node-to-node RPCs. |

The full design, the Raft plan and the trade-offs are in **[docs/DESIGN.md](docs/DESIGN.md)**.

## Roadmap

| # | Milestone | Status |
|---|-----------|--------|
| 1 | Single-node store: gRPC API, write-ahead log, crash recovery, Docker, CI | ✅ Done |
| 2 | Leader election: terms, `RequestVote`, randomized election timeouts, persisted `currentTerm`/`votedFor`, PreVote, 3-node Docker cluster | ✅ Done |
| 3 | Log replication: `AppendEntries`, consistency check, commit index, follower redirects to leader | 🔨 Next |
| 4 | Fault-injection test suite: kill nodes, partition the network, check no acknowledged write is lost | ⏳ Planned |
| 5 | Kubernetes: StatefulSet with stable identities, persistent volumes, gRPC probes | ⏳ Planned |
| 6 | Snapshots and log compaction; benchmarks (throughput, failover time) | ⏳ Planned |

## License

[MIT](LICENSE)
