package raftkv

import (
	"testing"
	"time"

	"raftkv/internal/raft"
	"raftkv/internal/store"
)

// TestProposeApplyRaceBeforeWaiterRegistration reproduces the exact race
// identified in review: an entry can commit and be applied before the
// proposing call gets around to registering a waiter for it — most
// easily on a single-node cluster, where Node.Propose can advance
// commitIndex synchronously, so RaftKV's own apply consumer can process
// the entry before the proposing goroutine so much as returns from
// Propose.
//
// This is forced deterministically, not left to scheduler luck: the
// test calls Node.Propose directly (bypassing RaftKV.Put entirely), then
// polls two independently observable facts — the store reflecting the
// write, and the applied-result record existing — before ever calling
// the registration/wait step. By construction, applyOne has therefore
// already run to completion, past its "no waiter found" branch, before
// waitForResult is invoked. That is precisely the ordering the bug
// required.
func TestProposeApplyRaceBeforeWaiterRegistration(t *testing.T) {
	network := raft.NewNetwork()
	const id = "solo"
	rn := mustNewRaftNode(t, raft.Config{
		ID:                 id,
		Transport:          raft.NewFakeTransport(network, id),
		ElectionTimeoutMin: testElectionTimeoutMin,
		ElectionTimeoutMax: testElectionTimeoutMax,
		HeartbeatInterval:  testHeartbeatInterval,
		RPCTimeout:         testRPCTimeout,
	})
	network.Register(id, rn)

	kv := New(Config{Node: rn, Store: store.New(), ProposalTimeout: testProposalTimeout})

	rn.Start()
	kv.Start()
	t.Cleanup(func() { kv.Stop(); rn.Stop() })

	// A solo node still waits out its own randomized election timeout
	// before becoming leader.
	eventually(t, 2*time.Second, 5*time.Millisecond, func() bool {
		return rn.State().Role == raft.Leader
	})

	// Propose directly -- bypassing RaftKV.Put / proposeAndWait / the
	// registration step entirely -- so the test controls exactly when
	// "registration" happens relative to "apply", instead of hoping the
	// two goroutines happen to interleave the buggy way.
	encoded, err := encodeCommand(command{Op: opPut, Key: "foo", Value: "bar"})
	if err != nil {
		t.Fatalf("encodeCommand: %v", err)
	}
	index, term, isLeader := rn.Propose(encoded)
	if !isLeader {
		t.Fatal("expected Propose to succeed (this node is its own leader)")
	}

	// Wait for the store to actually reflect the write: proof the apply
	// consumer's applyOne call has run past the point where it mutates
	// the store (which happens before it ever touches the waiters map).
	eventually(t, 2*time.Second, 5*time.Millisecond, func() bool {
		v, ok, _ := kv.store.Get("foo")
		return ok && v == "bar"
	})

	// Wait for the applied-result record to exist: proof applyOne has
	// now also passed its "no waiter found, so record it" branch. Since
	// no waiter was ever registered (we bypassed RaftKV.Put), this is
	// guaranteed to happen -- it's just a matter of the few remaining
	// sequential instructions in applyOne actually running.
	eventually(t, 2*time.Second, 5*time.Millisecond, func() bool {
		kv.mu.Lock()
		_, ok := kv.applied[index]
		kv.mu.Unlock()
		return ok
	})

	// Only now register to wait -- apply has unambiguously already
	// happened. Against the old implementation (no applied-result check)
	// this call would register a waiter nothing will ever resolve, and
	// block for the full proposal timeout before returning ErrTimeout
	// for a write that had already succeeded.
	if err := kv.waitForResult(index, term); err != nil {
		t.Fatalf("waitForResult returned %v, want nil — the entry had already committed and applied before registration", err)
	}
}
