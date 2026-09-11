package raftkv

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"raftkv/internal/raft"
	"raftkv/internal/store"
)

// 1. PUT through the leader commits and is applied on all reachable
// nodes.
func TestPutCommitsAndAppliesOnAllReachableNodes(t *testing.T) {
	nodes, _ := newTestCluster(t, 3)
	startAll(nodes)
	leader := waitForLeader(t, nodes)

	if err := leader.kv.Put("foo", "bar"); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	for _, n := range nodes {
		eventually(t, 2*time.Second, 5*time.Millisecond, func() bool {
			v, ok, _ := n.kv.store.Get("foo")
			return ok && v == "bar"
		})
	}
}

// 2. DELETE through the leader commits and is applied.
func TestDeleteCommitsAndApplies(t *testing.T) {
	nodes, _ := newTestCluster(t, 3)
	startAll(nodes)
	leader := waitForLeader(t, nodes)

	if err := leader.kv.Put("foo", "bar"); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if err := leader.kv.Delete("foo"); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	for _, n := range nodes {
		eventually(t, 2*time.Second, 5*time.Millisecond, func() bool {
			_, ok, _ := n.kv.store.Get("foo")
			return !ok
		})
	}
}

// 3. Follower PUT returns NOT_LEADER and does not mutate its store.
func TestFollowerPutReturnsNotLeaderAndDoesNotMutate(t *testing.T) {
	nodes, _ := newTestCluster(t, 3)
	startAll(nodes)
	leader := waitForLeader(t, nodes)

	follower := anyOtherThan(nodes, leader)

	err := follower.kv.Put("foo", "bar")
	var nle *NotLeaderError
	if !errors.As(err, &nle) {
		t.Fatalf("expected *NotLeaderError, got %v (%T)", err, err)
	}

	if _, ok, _ := follower.kv.store.Get("foo"); ok {
		t.Fatal("follower store was mutated despite returning NOT_LEADER")
	}
}

// 4. Follower DELETE returns NOT_LEADER.
func TestFollowerDeleteReturnsNotLeader(t *testing.T) {
	nodes, _ := newTestCluster(t, 3)
	startAll(nodes)
	leader := waitForLeader(t, nodes)

	if err := leader.kv.Put("foo", "bar"); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	eventually(t, 2*time.Second, 5*time.Millisecond, func() bool {
		_, ok, _ := anyOtherThan(nodes, leader).kv.store.Get("foo")
		return ok
	})

	follower := anyOtherThan(nodes, leader)
	err := follower.kv.Delete("foo")
	var nle *NotLeaderError
	if !errors.As(err, &nle) {
		t.Fatalf("expected *NotLeaderError, got %v (%T)", err, err)
	}

	if _, ok, _ := follower.kv.store.Get("foo"); !ok {
		t.Fatal("follower store was mutated (key deleted) despite returning NOT_LEADER")
	}
}

// 5. Follower GET returns NOT_LEADER.
func TestFollowerGetReturnsNotLeader(t *testing.T) {
	nodes, _ := newTestCluster(t, 3)
	startAll(nodes)
	leader := waitForLeader(t, nodes)

	follower := anyOtherThan(nodes, leader)
	_, _, err := follower.kv.Get("foo")
	var nle *NotLeaderError
	if !errors.As(err, &nle) {
		t.Fatalf("expected *NotLeaderError, got %v (%T)", err, err)
	}
}

// 6. Leader GET returns committed data.
func TestLeaderGetReturnsCommittedData(t *testing.T) {
	nodes, _ := newTestCluster(t, 3)
	startAll(nodes)
	leader := waitForLeader(t, nodes)

	if err := leader.kv.Put("foo", "bar"); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	value, ok, err := leader.kv.Get("foo")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !ok || value != "bar" {
		t.Fatalf("Get = (%q, %v), want (\"bar\", true)", value, ok)
	}
}

// 7. Successful PUT returns only after apply, not merely after local
// append. This is checked deterministically, not by timing: Put's
// implementation only returns after receiving a result that is sent
// strictly after the store mutation completes (program order within
// applyOne, then channel receive, then return) — so if this ever failed
// to hold, the store read immediately below would be racy and would
// eventually fail, not "usually pass".
func TestPutReturnsOnlyAfterApply(t *testing.T) {
	nodes, _ := newTestCluster(t, 3)
	startAll(nodes)
	leader := waitForLeader(t, nodes)

	if err := leader.kv.Put("foo", "bar"); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	value, ok, _ := leader.kv.store.Get("foo")
	if !ok || value != "bar" {
		t.Fatalf("store state immediately after Put = (%q, %v), want (\"bar\", true)", value, ok)
	}
}

// 8. Write cannot succeed without a majority.
func TestWriteCannotSucceedWithoutMajority(t *testing.T) {
	nodes, network := newTestCluster(t, 3)
	startAll(nodes)
	leader := waitForLeader(t, nodes)

	network.SetUnreachable(leader.raft.ID(), true)

	err := leader.kv.Put("foo", "bar")
	if err == nil {
		t.Fatal("expected Put to fail without a majority")
	}
	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("expected ErrTimeout, got %v", err)
	}

	for _, n := range nodes {
		if _, ok, _ := n.kv.store.Get("foo"); ok {
			t.Fatalf("node %s applied an uncommitted write", n.raft.ID())
		}
	}
}

// 9. Write times out / returns an error when quorum is unavailable.
// Distinct from #8: a genuine minority partition (not full isolation),
// and it checks the call actually waited out the timeout rather than
// failing for some other, faster reason.
func TestWriteTimesOutWhenQuorumUnavailable(t *testing.T) {
	nodes, network := newTestCluster(t, 5)
	startAll(nodes)
	leader := waitForLeader(t, nodes)

	minority := []*testNode{leader}
	var majority []*testNode
	for _, n := range nodes {
		if n == leader {
			continue
		}
		if len(minority) < 2 {
			minority = append(minority, n)
		} else {
			majority = append(majority, n)
		}
	}
	network.Partition(idsOf(minority), idsOf(majority))

	start := time.Now()
	err := leader.kv.Put("foo", "bar")
	elapsed := time.Since(start)

	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("expected ErrTimeout, got %v", err)
	}
	if elapsed < testProposalTimeout {
		t.Fatalf("Put returned after %s, before its %s timeout elapsed", elapsed, testProposalTimeout)
	}
}

// 10. Leadership loss while a client waits causes the write to fail
// cleanly.
func TestLeadershipLossWhileWaitingFailsCleanly(t *testing.T) {
	nodes, network := newTestCluster(t, 3)
	startAll(nodes)
	leader := waitForLeader(t, nodes)

	var others []*testNode
	for _, n := range nodes {
		if n != leader {
			others = append(others, n)
		}
	}

	// Partition the leader alone, away from the other two (a majority).
	network.Partition([]string{leader.raft.ID()}, idsOf(others))

	// This can never commit while the leader is isolated; it blocks
	// until something resolves it.
	done := make(chan error, 1)
	go func() {
		done <- leader.kv.Put("foo", "stuck")
	}()

	// The majority side elects its own new leader and commits its own
	// entry at the same log index the stuck proposal occupies.
	newLeader := waitForLeader(t, others)
	if err := newLeader.kv.Put("foo", "elsewhere"); err != nil {
		t.Fatalf("Put on the new leader failed: %v", err)
	}

	// Heal the partition so the old leader observes the new leader's
	// (higher-term) entry superseding its own.
	network.Heal()

	select {
	case err := <-done:
		var lle *LeadershipLostError
		if !errors.As(err, &lle) {
			t.Fatalf("expected *LeadershipLostError, got %v (%T)", err, err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("stuck Put never returned after the partition healed")
	}

	leader.kv.mu.Lock()
	n := len(leader.kv.waiters)
	leader.kv.mu.Unlock()
	if n != 0 {
		t.Fatalf("waiters map has %d entries after leadership loss, want 0 (leak)", n)
	}
}

// 11. Multiple concurrent client writes commit and apply correctly.
func TestConcurrentClientWritesCommitAndApplyCorrectly(t *testing.T) {
	nodes, _ := newTestCluster(t, 3)
	startAll(nodes)
	leader := waitForLeader(t, nodes)

	const n = 20
	var wg sync.WaitGroup
	errCh := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("key-%d", i)
			value := fmt.Sprintf("value-%d", i)
			if err := leader.kv.Put(key, value); err != nil {
				errCh <- fmt.Errorf("key %d: %w", i, err)
			}
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Errorf("concurrent Put failed: %v", err)
	}

	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%d", i)
		want := fmt.Sprintf("value-%d", i)
		for _, node := range nodes {
			node := node
			eventually(t, 2*time.Second, 5*time.Millisecond, func() bool {
				v, ok, _ := node.kv.store.Get(key)
				return ok && v == want
			})
		}
	}
}

// 12. Every committed command is applied once per node.
func TestEveryCommittedCommandAppliedOncePerNode(t *testing.T) {
	network := raft.NewNetwork()
	const id = "solo"
	rn := raft.NewNode(raft.Config{
		ID:                 id,
		Transport:          raft.NewFakeTransport(network, id),
		ElectionTimeoutMin: testElectionTimeoutMin,
		ElectionTimeoutMax: testElectionTimeoutMax,
		HeartbeatInterval:  testHeartbeatInterval,
		RPCTimeout:         testRPCTimeout,
	})
	network.Register(id, rn)

	cs := newCountingStore()
	kv := New(Config{Node: rn, Store: cs, ProposalTimeout: testProposalTimeout})

	rn.Start()
	kv.Start()
	t.Cleanup(func() { kv.Stop(); rn.Stop() })

	// A solo node still has to wait out its own (randomized) election
	// timeout before it becomes leader; it doesn't win instantly on
	// Start().
	eventually(t, 2*time.Second, 5*time.Millisecond, func() bool {
		return rn.State().Role == raft.Leader
	})

	for i := 0; i < 10; i++ {
		if err := kv.Put("k", fmt.Sprintf("v%d", i)); err != nil {
			t.Fatalf("Put %d failed: %v", i, err)
		}
	}

	cs.mu.Lock()
	got := cs.counts["k"]
	cs.mu.Unlock()
	if got != 10 {
		t.Fatalf("Put(\"k\", ...) was applied %d times, want exactly 10 (no double-application)", got)
	}
}

// 13. Malformed replicated commands do not crash the apply loop.
func TestMalformedReplicatedCommandDoesNotCrashApplyLoop(t *testing.T) {
	nodes, _ := newTestCluster(t, 1) // a single node is always its own leader
	startAll(nodes)
	leader := waitForLeader(t, nodes)

	// Bypass RaftKV entirely and inject a malformed command directly into
	// the Raft log, simulating corruption or an incompatible future
	// command version.
	if _, _, ok := leader.raft.Propose([]byte("not valid json")); !ok {
		t.Fatal("expected Propose to succeed (this node is its own leader)")
	}

	// The apply loop must survive that and keep working.
	if err := leader.kv.Put("foo", "bar"); err != nil {
		t.Fatalf("Put after a malformed entry failed: %v", err)
	}
	v, ok, _ := leader.kv.store.Get("foo")
	if !ok || v != "bar" {
		t.Fatalf("store state = (%q, %v), want (\"bar\", true)", v, ok)
	}
}

// 14. No goroutine/waiter leaks in the timeout path (the leadership-loss
// path's leak check lives inside test 10, above).
func TestNoGoroutineOrWaiterLeakOnRepeatedTimeouts(t *testing.T) {
	nodes, network := newTestCluster(t, 3)
	startAll(nodes)
	leader := waitForLeader(t, nodes)

	network.SetUnreachable(leader.raft.ID(), true)
	leader.kv.timeout = 30 * time.Millisecond // white-box: shorten for this test

	runtime.GC()
	before := runtime.NumGoroutine()

	for i := 0; i < 20; i++ {
		err := leader.kv.Put(fmt.Sprintf("k%d", i), "v")
		if !errors.Is(err, ErrTimeout) {
			t.Fatalf("iteration %d: expected ErrTimeout, got %v", i, err)
		}
	}

	leader.kv.mu.Lock()
	n := len(leader.kv.waiters)
	leader.kv.mu.Unlock()
	if n != 0 {
		t.Fatalf("waiters map has %d entries after 20 timed-out proposals, want 0", n)
	}

	runtime.GC()
	after := runtime.NumGoroutine()
	if after > before+2 { // small tolerance for scheduler/runtime noise
		t.Fatalf("goroutine count grew from %d to %d after 20 timed-out proposals", before, after)
	}
}

func anyOtherThan(nodes []*testNode, exclude *testNode) *testNode {
	for _, n := range nodes {
		if n != exclude {
			return n
		}
	}
	return nil
}

// countingStore wraps store.Store and counts Put calls per key, to
// detect double-application directly rather than inferring it from final
// state (which idempotent PUTs would hide).
type countingStore struct {
	*store.Store
	mu     sync.Mutex
	counts map[string]int
}

func newCountingStore() *countingStore {
	return &countingStore{Store: store.New(), counts: make(map[string]int)}
}

func (s *countingStore) Put(key, value string) error {
	s.mu.Lock()
	s.counts[key]++
	s.mu.Unlock()
	return s.Store.Put(key, value)
}
