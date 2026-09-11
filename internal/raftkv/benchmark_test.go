package raftkv

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"raftkv/internal/raft"
	"raftkv/internal/store"
)

// BenchmarkSingleNodePut measures end-to-end PUT latency/throughput
// through the real, full pipeline a client write takes: Propose ->
// durable persistence -> commit -> ApplyChannel -> local store -> the
// caller sees success. Deliberately realistic, not tuned for speed:
// persistence is a real on-disk FilePersister (temp-file write, fsync,
// atomic rename, directory fsync) via b.TempDir(), exactly as cmd/node
// uses. A single-node cluster commits an entry as soon as it is durably
// appended (majority of 1), so this isolates and measures exactly the
// cost that dominates every write regardless of cluster size: real
// synchronous disk persistence plus the Raft/RaftKV bookkeeping around
// it. It does not include network round trips to other nodes or the
// client-facing TCP protocol.
//
// (A three-node variant was tried and deliberately not used for the
// number in the README: sustained back-to-back proposals with zero
// think-time -- not representative of a real client -- can pile up
// concurrent replication goroutines faster than they drain against
// raft.FakeTransport, which checks a call's context deadline only once
// up front rather than enforcing it for the call's duration. That's a
// property of the in-memory test transport under synthetic load, not of
// the consensus algorithm or the production raft.TCPTransport (which
// uses real socket deadlines) -- but it makes a tight-loop 3-node
// benchmark occasionally misleadingly slow, so this benchmark reports
// the single-node number instead, which is stable and still measures
// the real, dominant cost: durable persistence.
//
// Run with:
//
//	go test ./internal/raftkv -run '^$' -bench BenchmarkSingleNodePut -benchtime 3s
func BenchmarkSingleNodePut(b *testing.B) {
	const id = "solo"

	persister, err := raft.NewFilePersister(filepath.Join(b.TempDir(), "state.json"))
	if err != nil {
		b.Fatalf("NewFilePersister: %v", err)
	}
	rn, err := raft.NewNode(raft.Config{ID: id, Transport: raft.NewFakeTransport(raft.NewNetwork(), id), Persister: persister})
	if err != nil {
		b.Fatalf("NewNode: %v", err)
	}
	kv := New(Config{Node: rn, Store: store.New()})

	rn.Start()
	kv.Start()
	b.Cleanup(func() {
		kv.Stop()
		rn.Stop()
	})

	deadline := time.Now().Add(5 * time.Second)
	for rn.State().Role != raft.Leader && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if rn.State().Role != raft.Leader {
		b.Fatal("node never became leader before benchmark start")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i)
		if err := kv.Put(key, "value"); err != nil {
			b.Fatalf("Put: %v", err)
		}
	}
}
