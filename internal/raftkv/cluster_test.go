package raftkv

import (
	"fmt"
	"testing"
	"time"

	"raftkv/internal/raft"
	"raftkv/internal/store"
)

// Small, fast timing so tests run quickly and deterministically-ish —
// same rationale as internal/raft's own test helpers: FakeTransport RPCs
// are direct function calls (no real I/O), so there's no network latency
// to budget for, and the heartbeat/election margin is generous.
const (
	testElectionTimeoutMin = 50 * time.Millisecond
	testElectionTimeoutMax = 100 * time.Millisecond
	testHeartbeatInterval  = 10 * time.Millisecond
	testRPCTimeout         = 20 * time.Millisecond
	testProposalTimeout    = 500 * time.Millisecond
)

// testNode pairs a raft.Node with the RaftKV wrapping it, for tests.
type testNode struct {
	raft *raft.Node
	kv   *RaftKV
}

// newTestCluster creates n Raft nodes, each with its own RaftKV and
// backing store.Store, wired together over a shared in-memory Network.
// It does not start anything — call startAll once ready.
func newTestCluster(t *testing.T, n int) ([]*testNode, *raft.Network) {
	t.Helper()

	network := raft.NewNetwork()
	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("node-%d", i)
	}

	nodes := make([]*testNode, n)
	for i, id := range ids {
		peers := make([]string, 0, n-1)
		for _, other := range ids {
			if other != id {
				peers = append(peers, other)
			}
		}
		transport := raft.NewFakeTransport(network, id)
		rn := raft.NewNode(raft.Config{
			ID:                 id,
			Peers:              peers,
			Transport:          transport,
			ElectionTimeoutMin: testElectionTimeoutMin,
			ElectionTimeoutMax: testElectionTimeoutMax,
			HeartbeatInterval:  testHeartbeatInterval,
			RPCTimeout:         testRPCTimeout,
		})
		network.Register(id, rn)

		kv := New(Config{
			Node:            rn,
			Store:           store.New(),
			ProposalTimeout: testProposalTimeout,
		})

		nodes[i] = &testNode{raft: rn, kv: kv}
	}

	t.Cleanup(func() {
		for _, n := range nodes {
			n.kv.Stop()
			n.raft.Stop()
		}
	})

	return nodes, network
}

func startAll(nodes []*testNode) {
	for _, n := range nodes {
		n.raft.Start()
		n.kv.Start()
	}
}

// waitForLeader polls until exactly one node in nodes reports itself as
// Leader, failing the test if that never happens within a generous
// bound.
func waitForLeader(t *testing.T, nodes []*testNode) *testNode {
	t.Helper()
	var leader *testNode
	eventually(t, 2*time.Second, 5*time.Millisecond, func() bool {
		var found *testNode
		count := 0
		for _, n := range nodes {
			if n.raft.State().Role == raft.Leader {
				found = n
				count++
			}
		}
		if count == 1 {
			leader = found
			return true
		}
		return false
	})
	return leader
}

// eventually polls cond every interval until it returns true, failing
// the test if timeout elapses first.
func eventually(t *testing.T, timeout, interval time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("condition not met within %s", timeout)
		}
		time.Sleep(interval)
	}
}

func idsOf(nodes []*testNode) []string {
	ids := make([]string, len(nodes))
	for i, n := range nodes {
		ids[i] = n.raft.ID()
	}
	return ids
}
