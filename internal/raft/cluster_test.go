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

// mustNewNode calls NewNode and fails the test if it returns an error.
// NewNode only fails when Persister.LoadState fails, which NoopPersister
// (the default used by nearly every test here) never does — so this
// just removes the same boilerplate error check from every call site.
func mustNewNode(t *testing.T, cfg Config) *Node {
	t.Helper()
	n, err := NewNode(cfg)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	return n
}

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
		node := mustNewNode(t, Config{
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

// waitForLeader polls until exactly one node in nodes reports itself as
// Leader, failing the test if that never happens within a generous
// bound.
func waitForLeader(t *testing.T, nodes []*Node) *Node {
	t.Helper()
	var leader *Node
	eventually(t, 2*time.Second, 5*time.Millisecond, func() bool {
		ls := leaders(nodes)
		if len(ls) == 1 {
			leader = ls[0]
			return true
		}
		return false
	})
	return leader
}

// collectApplied reads exactly count entries from n's apply channel,
// failing the test if they don't all arrive within timeout.
func collectApplied(t *testing.T, n *Node, count int, timeout time.Duration) []AppliedEntry {
	t.Helper()
	out := make([]AppliedEntry, 0, count)
	deadline := time.After(timeout)
	for len(out) < count {
		select {
		case e := <-n.ApplyChannel():
			out = append(out, e)
		case <-deadline:
			t.Fatalf("timed out waiting for %d applied entries, got %d", count, len(out))
		}
	}
	return out
}

// logsEqual reports whether a and b have identical entries (including
// the index-0 sentinel).
func logsEqual(a, b []LogEntry) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Index != b[i].Index || a[i].Term != b[i].Term || string(a[i].Command) != string(b[i].Command) {
			return false
		}
	}
	return true
}

// nextIndexFor reads a leader's current nextIndex for peer under its own
// lock. White-box access, for tests only.
func nextIndexFor(n *Node, peer string) uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.nextIndex[peer]
}

// matchIndexFor reads a leader's current matchIndex for peer under its
// own lock. White-box access, for tests only.
func matchIndexFor(n *Node, peer string) uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.matchIndex[peer]
}
