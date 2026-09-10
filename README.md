# Distributed Key-Value Store

A distributed key-value store built in Go, using the Raft consensus algorithm
to replicate data across a cluster of nodes.

This project is being built incrementally. Each stage is fully working and
tested before the next stage is added.

## Stages

- [x] **Stage 1 — Basic Key-Value Store**: a single-node, in-memory,
      thread-safe key-value store supporting PUT / GET / DELETE.
- [x] **Stage 2 — Networking**: expose the store over TCP so clients can
      issue commands remotely.
- [ ] **Stage 3 — Raft consensus** *(future work)*: leader election,
      heartbeats, log replication, and majority-based commit across
      multiple nodes.
- [ ] **Stage 4 — Persistence & recovery** *(future work)*: durable Raft
      state and log so nodes can crash and rejoin the cluster without
      losing data.
- [ ] **Stage 5 — Failure testing** *(future work)*: automated tests that
      kill, partition, and restart nodes to verify the cluster stays
      consistent.

## Architecture (Stage 2)

```
Raft-KV/
├── go.mod
├── cmd/
│   └── node/
│       └── main.go            # entrypoint: flags, wiring, signal handling
└── internal/
    ├── store/
    │   ├── store.go           # in-memory key-value store
    │   └── store_test.go
    └── server/
        ├── server.go          # TCP accept loop, connection handling, shutdown
        ├── protocol.go        # request parsing, response formatting
        ├── server_test.go     # integration tests over real TCP
        └── protocol_test.go   # unit tests for the parser
```

**`internal/store`** implements `Store`, a key-value map guarded by a
`sync.RWMutex` so it is safe to call from multiple goroutines at once.

**`internal/server`** exposes a `Store` over TCP. It depends only on a small
`KV` interface:

```go
type KV interface {
    Put(key, value string) error
    Get(key string) (value string, ok bool, err error)
    Delete(key string) error
}
```

`*store.Store` satisfies this interface, so `server.New(store.New())` works
today with no changes to `store`. The networking layer never references
`store.Store` directly — this is deliberate, so that a future Raft-backed
implementation of `KV` can be swapped in without touching `internal/server`
at all.

Every method returns an `error`. `*store.Store` always returns `nil` today
— a local, single-node store can't fail a write or a read — but the
networking layer already turns a non-nil error into an `ERR <message>`
response. This is forward-looking for Stage 3: a Raft-backed `KV` will need
to fail a write when the node isn't the leader, a proposal times out, or
the cluster can't reach quorum, and it will need to fail (or explicitly
flag) a read if strong consistency requires routing `GET` through the
leader rather than serving it from a possibly-stale follower state
machine. Adding the error return now means that decision can be made in
Stage 3 without changing this interface, the wire protocol, or every call
site again.

`Server.Serve(ln net.Listener)` accepts an already-bound listener (mirroring
`net/http`'s `Serve`/`ListenAndServe` split) rather than an address string,
which keeps the server package itself address-agnostic — the listening
address is configured by whoever calls `Serve`, in this case `cmd/node`.

Each accepted connection is handled on its own goroutine, reading
newline-delimited commands and writing one response line per command.
`Server.Shutdown()` closes the listener, force-closes any open connections
to unblock in-flight reads, and waits for every connection goroutine to
exit before returning — so shutdown is deterministic and never hangs or
leaks goroutines.

**`cmd/node`** is the binary entrypoint. It parses a `-addr` flag, wires a
`store.Store` into a `server.Server`, and listens for `SIGINT`/`SIGTERM` to
trigger a graceful shutdown.

## Running a node

```bash
go run ./cmd/node -addr :9000
```

`-addr` defaults to `:9000` and accepts anything `net.Listen("tcp", ...)`
accepts, e.g. `127.0.0.1:9001` to bind to loopback only. Press `Ctrl+C` (or
send `SIGTERM`) to shut the node down gracefully.

## Protocol

A simple newline-delimited text protocol: one command per line, one
response per line. Commands are case-insensitive; a `PUT` value may itself
contain spaces (it is everything after the key on the line).

**Requests:**
```
PUT <key> <value>
GET <key>
DELETE <key>
```

**Responses:**

| Situation                    | Response          |
|-------------------------------|-------------------|
| `PUT` / `DELETE` succeeded    | `OK`              |
| `GET` found the key           | `VALUE <value>`   |
| `GET` did not find the key    | `NOT_FOUND`       |
| Malformed or unknown command  | `ERR <message>`   |

An error never closes the connection — the client can keep issuing
commands on the same connection afterward.

### Example session

```bash
$ nc 127.0.0.1 9000
PUT foo bar
OK
GET foo
VALUE bar
DELETE foo
OK
GET foo
NOT_FOUND
BOGUS
ERR unknown command "BOGUS"
```

## Running tests

```bash
go test -race ./...
```
