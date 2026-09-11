package raft

import (
	"fmt"
	"testing"
	"time"
)

// Small, fast timing for tests: HeartbeatInterval is a fifth of
// ElectionTimeoutMin, a wide enough margin that heartbeats never race an
// election timeout even under scheduler jitter. FakeTransport RPCs are
// direct function calls (no real I/O), so there is no network latency to
// budget for on top of that.
const (
	testElectionTimeoutMin = 50 * time.Millisecond
	testElectionTimeoutMax = 100 * time.Millisecond
	testHeartbeatInterval  = 10 * time.Millisecond
	testRPCTimeout         = 20 * time.Millisecond
)

// newTestCluster creates n Nodes wired together over a shared in-memory
// Network, all using small/fast timing so tests run quickly. It does not
// start the nodes; call startAll once ready, so a test can mutate state
// (e.g. partition a peer) before anything starts timing out.
func newTestCluster(t *testing.T, n int) ([]*Node, *Network) {
	t.Helper()

	network := NewNetwork()
	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("node-%d", i)
	}

	nodes := make([]*Node, n)
	for i, id := range ids {
		peers := make([]string, 0, n-1)
		for _, other := range ids {
			if other != id {
				peers = append(peers, other)
			}
		}
		transport := NewFakeTransport(network, id)
		node := NewNode(Config{
			ID:                 id,
			Peers:              peers,
			Transport:          transport,
			ElectionTimeoutMin: testElectionTimeoutMin,
			ElectionTimeoutMax: testElectionTimeoutMax,
			HeartbeatInterval:  testHeartbeatInterval,
			RPCTimeout:         testRPCTimeout,
		})
		network.Register(id, node)
		nodes[i] = node
	}

	t.Cleanup(func() {
		for _, node := range nodes {
			node.Stop()
		}
	})

	return nodes, network
}

func startAll(nodes []*Node) {
	for _, n := range nodes {
		n.Start()
	}
}

// eventually polls cond every interval until it returns true, failing the
// test if timeout elapses first. Election tests depend on "did the
// expected state eventually happen" rather than a fixed sleep tied to
// exact timer values — that's what keeps them from being flaky under
// scheduler jitter: the common case returns as soon as the condition is
// met, and only a genuine failure to converge burns the full timeout.
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

func leaders(nodes []*Node) []*Node {
	var out []*Node
	for _, n := range nodes {
		if n.State().Role == Leader {
			out = append(out, n)
		}
	}
	return out
}
