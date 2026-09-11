package raft

import (
	"context"
	"sync"
	"testing"
)

// controlledTransport wraps a FakeTransport and, for AppendEntries calls
// to one specific target, lets a test hold back delivery of a chosen
// call's reply to the caller until explicitly released — while the
// underlying request is still processed by the target immediately, in
// real call order.
//
// This exists to deterministically reproduce "an older request's reply
// is delayed and arrives after a newer request's reply already landed"
// without depending on goroutine scheduling or real timing: the test
// controls exactly which call's reply is released, and when.
type controlledTransport struct {
	*FakeTransport
	target string

	mu      sync.Mutex
	nextIdx int
	holds   map[int]chan struct{}
	// assigned reports, in order, the call index assigned to each
	// AppendEntries call to target, right after the target has processed
	// it (so a test can wait for "call N has been delivered and is now
	// either held or about to return") instead of polling or sleeping.
	assigned chan int
}

func newControlledTransport(inner *FakeTransport, target string) *controlledTransport {
	return &controlledTransport{
		FakeTransport: inner,
		target:        target,
		holds:         make(map[int]chan struct{}),
		assigned:      make(chan int, 8),
	}
}

// hold marks the callIndex'th (0-based) AppendEntries call to target as
// one whose reply must be withheld from the caller until release is
// called for that same index.
func (t *controlledTransport) hold(callIndex int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.holds[callIndex] = make(chan struct{})
}

func (t *controlledTransport) release(callIndex int) {
	t.mu.Lock()
	gate := t.holds[callIndex]
	t.mu.Unlock()
	if gate != nil {
		close(gate)
	}
}

func (t *controlledTransport) AppendEntries(ctx context.Context, target string, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	// Deliver to the target immediately, exactly as a real call would —
	// only the reply's return to the caller is ever delayed.
	reply, err := t.FakeTransport.AppendEntries(ctx, target, args)
	if target != t.target || err != nil {
		return reply, err
	}

	t.mu.Lock()
	idx := t.nextIdx
	t.nextIdx++
	gate := t.holds[idx]
	t.mu.Unlock()

	t.assigned <- idx

	if gate != nil {
		<-gate
	}
	return reply, err
}

// TestStaleRejectionDoesNotRegressNextIndexBelowMatchIndexPlusOne
// reproduces the exact scenario from the Stage 4 review: two
// AppendEntries RPCs to the same follower are concurrently in flight.
// The older one (sent against a higher, since-superseded nextIndex) is
// delayed. A newer one (sent against an already-backtracked, correct
// nextIndex) succeeds first and advances matchIndex/nextIndex. Only then
// does the older request's rejection finally arrive.
//
// Every synchronization point below is a real channel receive tied to an
// actual event (a call being delivered, a goroutine completing) rather
// than a sleep or a polling loop, so the interleaving this test needs is
// guaranteed on every run, not just likely.
func TestStaleRejectionDoesNotRegressNextIndexBelowMatchIndexPlusOne(t *testing.T) {
	const term = uint64(1)
	const peer = "follower"

	network := NewNetwork()

	ct := newControlledTransport(NewFakeTransport(network, "leader"), peer)
	leader := mustNewNode(t, Config{ID: "leader", Peers: []string{peer}, Transport: ct})
	network.Register("leader", leader)

	follower := mustNewNode(t, Config{ID: peer, Peers: []string{"leader"}, Transport: NewFakeTransport(network, peer)})
	network.Register(peer, follower)

	// Follower starts with a real log up to index 8 (term 1) — this is
	// the true, correct position it will still be at when the older
	// request (below) is actually delivered to it.
	follower.mu.Lock()
	follower.currentTerm = term
	for i := uint64(1); i <= 8; i++ {
		follower.log = append(follower.log, LogEntry{Index: i, Term: term, Command: []byte{byte(i)}})
	}
	follower.mu.Unlock()

	// Leader has a longer log, up to index 12 (term 1), and — as if an
	// earlier round already optimistically assumed the follower was
	// almost caught up — nextIndex=10, well ahead of the follower's true
	// position of 8.
	leader.mu.Lock()
	leader.currentTerm = term
	leader.role = Leader
	leader.leaderID = "leader"
	for i := uint64(1); i <= 12; i++ {
		leader.log = append(leader.log, LogEntry{Index: i, Term: term, Command: []byte{byte(i)}})
	}
	leader.nextIndex = map[string]uint64{peer: 10}
	leader.matchIndex = map[string]uint64{peer: 0}
	leader.mu.Unlock()

	// Round A ("older"): sent while nextIndex=10 -> PrevLogIndex=9. The
	// follower is still only at index 8, so this is genuinely rejected.
	// Hold its reply back from the leader.
	ct.hold(0)
	doneA := make(chan struct{})
	go func() {
		leader.replicateToPeer(term, peer, "leader", 0)
		close(doneA)
	}()

	// Wait for round A's request to actually be delivered and processed
	// by the follower (its rejection is already computed at this point;
	// only its return to the leader is being held).
	if idx := <-ct.assigned; idx != 0 {
		t.Fatalf("first AppendEntries call got index %d, want 0", idx)
	}

	// Simulate an earlier, already-applied backtrack: nextIndex drops to
	// 9 before round B is sent.
	leader.mu.Lock()
	leader.nextIndex[peer] = 9
	leader.mu.Unlock()

	// Round B ("newer"): sent using the corrected nextIndex=9 ->
	// PrevLogIndex=8, which matches the follower's true position. Not
	// held, so it completes as soon as it's processed.
	doneB := make(chan struct{})
	go func() {
		leader.replicateToPeer(term, peer, "leader", 0)
		close(doneB)
	}()
	<-doneB

	if got := matchIndexFor(leader, peer); got != 12 {
		t.Fatalf("matchIndex after round B = %d, want 12", got)
	}
	if got := nextIndexFor(leader, peer); got != 13 {
		t.Fatalf("nextIndex after round B = %d, want 13", got)
	}

	// Now release round A's stale rejection.
	ct.release(0)
	<-doneA

	match := matchIndexFor(leader, peer)
	next := nextIndexFor(leader, peer)
	if next < match+1 {
		t.Fatalf("nextIndex (%d) fell below matchIndex+1 (%d) after a stale, out-of-order rejection", next, match+1)
	}
	if next != match+1 {
		t.Fatalf("nextIndex = %d, want exactly matchIndex+1 = %d after the stale rejection", next, match+1)
	}
}
