# Distributed Key-Value Store

A distributed key-value store built in Go, using the Raft consensus algorithm
to replicate data across a cluster of nodes.

This project is being built incrementally. Each stage is fully working and
tested before the next stage is added.

## Stages

- [x] **Stage 1 — Basic Key-Value Store**: a single-node, in-memory,
      thread-safe key-value store supporting PUT / GET / DELETE.
- [ ] **Stage 2 — Networking**: expose the store over TCP so clients can
      issue commands remotely.
- [ ] **Stage 3 — Raft consensus**: leader election, heartbeats, log
      replication, and majority-based commit across multiple nodes.
- [ ] **Stage 4 — Persistence & recovery**: durable Raft state and log so
      nodes can crash and rejoin the cluster without losing data.
- [ ] **Stage 5 — Failure testing**: automated tests that kill, partition,
      and restart nodes to verify the cluster stays consistent.

## Architecture (Stage 1)

```
Raft-KV/
├── go.mod
└── internal/
    └── store/
        ├── store.go       # in-memory key-value store
        └── store_test.go  # unit tests
```

`internal/store` implements `Store`, a key-value map guarded by a
`sync.RWMutex` so it is safe to call from multiple goroutines at once. This
matters starting now, not later: once networking and Raft are added, the
store will be read and written concurrently by request handlers and the
Raft apply loop, so it is built thread-safe from the first line of code.

The package is intentionally isolated from networking and consensus logic.
Later stages will add sibling packages (`internal/raft`, `internal/transport`)
and a `cmd/` entrypoint that wire into `Store` without needing to change it.

### API

```go
s := store.New()

s.Put("key", "value")
value, ok := s.Get("key")
s.Delete("key")
```

## Running tests

```bash
go test ./...
```
