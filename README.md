# RaftKV

[![CI](https://github.com/Ncitakovic/raftkv/actions/workflows/ci.yml/badge.svg)](https://github.com/Ncitakovic/raftkv/actions/workflows/ci.yml)

A replicated key-value store in Java, built step by step on the [Raft consensus algorithm](https://raft.github.io/raft.pdf).

The goal is a small cluster (3–5 nodes) that keeps accepting reads and writes when a minority of nodes crash or get cut off from the network, **without ever losing a write it has acknowledged**. The project is a learning vehicle for distributed-systems fundamentals: consensus, replication, durability, failure detection, and testing under failure.

> **Status: work in progress — milestone 1 of 6 complete.** Today RaftKV is a durable single-node store with a gRPC API. Replication is being built next; see the [roadmap](#roadmap).

## What works today

- **gRPC API**: `Put`, `Get`, `Delete` ([`kv.proto`](src/main/proto/kv.proto)), plus the standard gRPC health and reflection services.
- **Write-ahead log**: every write is appended to a checksummed log and `fsync`'d *before* it is acknowledged.
- **Crash recovery**: on startup, the log is replayed to rebuild state. A torn or corrupted record at the tail (from a crash mid-write) is detected by CRC32 and truncated.
- **Log + state machine split**: writes go *log → state machine → client*, the exact path a Raft leader will use, so replication slots in without changing the storage format.
- **Container-ready**: multi-stage Docker image running as a non-root user, configured by environment variables.
- **Tests**: log recovery (torn and corrupted records), state-machine determinism, restart durability, and concurrent writers.

## Quick start

Requirements: Java 21 and Maven, or just Docker.

```bash
# Build and run the tests
mvn verify

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

| Variable          | Default  | Meaning                           |
|-------------------|----------|-----------------------------------|
| `RAFTKV_PORT`     | `50051`  | gRPC listen port                  |
| `RAFTKV_DATA_DIR` | `./data` | Directory holding the log file    |

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
| `server`   | `KeyValueService`: gRPC endpoints, input limits, mapping failures to gRPC status codes. |

The full design, the Raft plan and the trade-offs are in **[docs/DESIGN.md](docs/DESIGN.md)**.

## Roadmap

| # | Milestone | Status |
|---|-----------|--------|
| 1 | Single-node store: gRPC API, write-ahead log, crash recovery, Docker, CI | ✅ Done |
| 2 | Leader election: terms, `RequestVote`, randomized election timeouts, persisted `currentTerm`/`votedFor` | 🔨 In progress (persistent term/vote done) |
| 3 | Log replication: `AppendEntries`, consistency check, commit index, follower redirects to leader | ⏳ Planned |
| 4 | Fault-injection test suite: kill nodes, partition the network, check no acknowledged write is lost | ⏳ Planned |
| 5 | Kubernetes: StatefulSet with stable identities, persistent volumes, gRPC probes | ⏳ Planned |
| 6 | Snapshots and log compaction; benchmarks (throughput, failover time) | ⏳ Planned |

## License

[MIT](LICENSE)
