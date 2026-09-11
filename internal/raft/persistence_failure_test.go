package raft

import (
	"path/filepath"
	"testing"
	"time"
)

// 1. Failed persistence while starting an election prevents the node
// from campaigning normally: a single-node cluster that would otherwise
// win instantly must not become leader.
func TestElectionStopsOnPersistenceFailure(t *testing.T) {
	fp := newFailingPersister(nil)
	fp.SetFail(true)
	n := mustNewNode(t, Config{ID: "a", Persister: fp})

	n.startElection()

	st := n.State()
	if st.PersistenceError == nil {
		t.Fatal("expected a persistence error to be recorded")
	}
	if st.Role == Leader {
		t.Fatal("node became leader despite a failed persist during its own election")
	}
}

// 2. Failed persistence while granting a vote does not return
// VoteGranted: true.
func TestHandleRequestVoteDoesNotGrantOnPersistenceFailure(t *testing.T) {
	fp := newFailingPersister(nil)
	n := mustNewNode(t, Config{ID: "a", Persister: fp})

	// Reach term 1 first, while persistence still works, so the
	// upcoming vote request doesn't also need a term adoption --
	// isolating exactly the vote-grant's own persist call.
	n.HandleAppendEntries(&AppendEntriesArgs{Term: 1, LeaderID: "leader"})
	if n.State().Term != 1 {
		t.Fatalf("setup: term = %d, want 1", n.State().Term)
	}

	fp.SetFail(true)
	reply := n.HandleRequestVote(&RequestVoteArgs{Term: 1, CandidateID: "b"})
	if reply.VoteGranted {
		t.Fatal("expected the vote NOT to be granted when persisting it fails")
	}
	if n.State().PersistenceError == nil {
		t.Fatal("expected a persistence error to be recorded")
	}
}

// 3. Failed persistence while accepting AppendEntries does not return
// Success: true.
func TestHandleAppendEntriesDoesNotSucceedOnPersistenceFailure(t *testing.T) {
	fp := newFailingPersister(nil)
	n := mustNewNode(t, Config{ID: "a", Persister: fp})

	fp.SetFail(true)
	reply := n.HandleAppendEntries(&AppendEntriesArgs{
		Term: 1, LeaderID: "leader", PrevLogIndex: 0, PrevLogTerm: 0,
		Entries: []LogEntry{{Index: 1, Term: 1, Command: []byte("x")}},
	})
	if reply.Success {
		t.Fatal("expected Success=false when persisting the new entries fails")
	}
	if n.State().PersistenceError == nil {
		t.Fatal("expected a persistence error to be recorded")
	}
}

// 4. Failed persistence during a leader's Propose does not allow the
// proposal to proceed as a successful proposal.
func TestProposeDoesNotSucceedOnPersistenceFailure(t *testing.T) {
	fp := newFailingPersister(nil)
	n := mustNewNode(t, Config{ID: "a", Persister: fp})

	// Become leader first, while persistence still works (a single-node
	// cluster wins its own election immediately).
	n.startElection()
	if n.State().Role != Leader {
		t.Fatalf("setup: role = %s, want Leader", n.State().Role)
	}

	fp.SetFail(true)
	index, term, ok := n.Propose([]byte("cmd"))
	if ok {
		t.Fatalf("expected Propose to fail when persisting the new entry fails, got (index=%d, term=%d, ok=true)", index, term)
	}
	if n.State().PersistenceError == nil {
		t.Fatal("expected a persistence error to be recorded")
	}
}

// 5. A node that has entered the persistence-failed state remains
// fail-stopped even if the persister would succeed on a later call.
func TestPersistenceFailedNodeStaysFailedEvenIfPersisterRecovers(t *testing.T) {
	fp := newFailingPersister(nil)
	n := mustNewNode(t, Config{ID: "a", Persister: fp})

	fp.SetFail(true)
	n.HandleRequestVote(&RequestVoteArgs{Term: 1, CandidateID: "b"})
	firstErr := n.State().PersistenceError
	if firstErr == nil {
		t.Fatal("expected the node to have entered the persistence-failed state")
	}

	// The underlying persister "recovers".
	fp.SetFail(false)

	// The node must not recover on its own: it must still refuse to
	// grant a vote, and the recorded error must be the first one,
	// unchanged.
	reply := n.HandleRequestVote(&RequestVoteArgs{Term: 2, CandidateID: "c"})
	if reply.VoteGranted {
		t.Fatal("a persistence-failed node granted a vote after the persister recovered -- it must stay fail-stopped")
	}
	if n.State().PersistenceError != firstErr {
		t.Fatal("the recorded persistence error changed; it must be permanent and fixed at the first failure")
	}
}

// 6. A persistence-failed leader stops behaving as a valid leader: once
// its persistence fails, it must not be able to propose further entries
// or replicate anything new to its followers.
func TestPersistenceFailedLeaderStopsReplicating(t *testing.T) {
	network := NewNetwork()
	fpA := newFailingPersister(nil)
	fpB := newFailingPersister(nil)

	a := mustNewNode(t, Config{
		ID: "a", Peers: []string{"b"}, Transport: NewFakeTransport(network, "a"), Persister: fpA,
		ElectionTimeoutMin: testElectionTimeoutMin, ElectionTimeoutMax: testElectionTimeoutMax,
		HeartbeatInterval: testHeartbeatInterval, RPCTimeout: testRPCTimeout,
	})
	b := mustNewNode(t, Config{
		ID: "b", Peers: []string{"a"}, Transport: NewFakeTransport(network, "b"), Persister: fpB,
		ElectionTimeoutMin: testElectionTimeoutMin, ElectionTimeoutMax: testElectionTimeoutMax,
		HeartbeatInterval: testHeartbeatInterval, RPCTimeout: testRPCTimeout,
	})
	network.Register("a", a)
	network.Register("b", b)

	all := []*Node{a, b}
	for _, n := range all {
		n.Start()
	}
	t.Cleanup(func() {
		for _, n := range all {
			n.Stop()
		}
	})

	leader := waitForLeader(t, all)
	var leaderFP *failingPersister
	if leader == a {
		leaderFP = fpA
	} else {
		leaderFP = fpB
	}
	var follower *Node
	for _, n := range all {
		if n != leader {
			follower = n
		}
	}

	// One entry replicates normally first, while persistence still
	// works.
	if _, _, ok := leader.Propose([]byte("first")); !ok {
		t.Fatal("setup Propose failed")
	}
	eventually(t, 2*time.Second, 5*time.Millisecond, func() bool {
		return len(follower.Log()) == 2
	})

	// Now the leader's persistence starts failing.
	leaderFP.SetFail(true)

	if _, _, ok := leader.Propose([]byte("second")); ok {
		t.Fatal("expected Propose to fail once the leader's persistence fails")
	}
	if leader.State().PersistenceError == nil {
		t.Fatal("expected the leader to have recorded a persistence error")
	}

	// No further replication happens: the follower's log must not grow
	// past what it already had.
	time.Sleep(10 * testHeartbeatInterval)
	if got := len(follower.Log()); got != 2 {
		t.Fatalf("follower log grew to length %d after the leader's persistence failed, want 2 (no further replication)", got)
	}
}

// 7. Restarting/reconstructing a node from the last successfully
// persisted state works normally, even though a later save had failed.
func TestRestartRecoversLastSuccessfullyPersistedStateAfterFailure(t *testing.T) {
	dir := t.TempDir()
	statePath := filepath.Join(dir, "state.json")

	realPersister, err := NewFilePersister(statePath)
	if err != nil {
		t.Fatalf("NewFilePersister: %v", err)
	}
	fp := newFailingPersister(realPersister)

	n1, err := NewNode(Config{ID: "a", Persister: fp})
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}

	// A successful vote, persisted normally through to the real file.
	n1.HandleRequestVote(&RequestVoteArgs{Term: 3, CandidateID: "b"})
	if got := n1.State(); got.Term != 3 || got.VotedFor != "b" {
		t.Fatalf("setup: state = %+v, want term=3 votedFor=\"b\"", got)
	}

	// Persistence now fails: a further state change is attempted but
	// never durably saved.
	fp.SetFail(true)
	n1.HandleRequestVote(&RequestVoteArgs{Term: 4, CandidateID: "c"})
	if n1.State().PersistenceError == nil {
		t.Fatal("expected the node to have fail-stopped")
	}

	// Restart: a fresh Node against the same underlying file, through a
	// working Persister (as a real restart would use).
	persister2, err := NewFilePersister(statePath)
	if err != nil {
		t.Fatalf("NewFilePersister (restart): %v", err)
	}
	n2, err := NewNode(Config{ID: "a", Persister: persister2})
	if err != nil {
		t.Fatalf("NewNode (restart): %v", err)
	}

	st := n2.State()
	if st.Term != 3 || st.VotedFor != "b" {
		t.Fatalf("recovered state = (term=%d, votedFor=%q), want (term=3, votedFor=\"b\") -- the last successful save, not the failed one", st.Term, st.VotedFor)
	}
	if st.PersistenceError != nil {
		t.Fatalf("a freshly restarted node should not carry over the old persistence error, got %v", st.PersistenceError)
	}
}

// 9. No double-vote scenario is possible through an injected SaveState
// failure: once a vote-persist fails, the node must never grant a
// different candidate's vote request in that same term either, even
// after the persister recovers.
func TestNoDoubleVoteThroughInjectedPersistenceFailure(t *testing.T) {
	fp := newFailingPersister(nil)
	n := mustNewNode(t, Config{ID: "a", Persister: fp})

	fp.SetFail(true)
	r1 := n.HandleRequestVote(&RequestVoteArgs{Term: 1, CandidateID: "b"})
	if r1.VoteGranted {
		t.Fatal("expected the first vote (with failing persistence) to be rejected")
	}

	fp.SetFail(false)
	r2 := n.HandleRequestVote(&RequestVoteArgs{Term: 1, CandidateID: "c"})
	if r2.VoteGranted {
		t.Fatal("a persistence-failed node granted a second vote in the same term -- double-vote risk")
	}
}

// 10. No follower can be counted as successful replication when its
// required log persistence failed: the leader's matchIndex for that
// peer must never advance.
func TestFollowerNotCountedAsReplicatedWhenPersistenceFails(t *testing.T) {
	network := NewNetwork()
	fpC := newFailingPersister(nil)
	fpC.SetFail(true)

	a := mustNewNode(t, Config{
		ID: "a", Peers: []string{"b", "c"}, Transport: NewFakeTransport(network, "a"),
		ElectionTimeoutMin: testElectionTimeoutMin, ElectionTimeoutMax: testElectionTimeoutMax,
		HeartbeatInterval: testHeartbeatInterval, RPCTimeout: testRPCTimeout,
	})
	b := mustNewNode(t, Config{
		ID: "b", Peers: []string{"a", "c"}, Transport: NewFakeTransport(network, "b"),
		ElectionTimeoutMin: testElectionTimeoutMin, ElectionTimeoutMax: testElectionTimeoutMax,
		HeartbeatInterval: testHeartbeatInterval, RPCTimeout: testRPCTimeout,
	})
	c := mustNewNode(t, Config{
		ID: "c", Peers: []string{"a", "b"}, Transport: NewFakeTransport(network, "c"), Persister: fpC,
		ElectionTimeoutMin: testElectionTimeoutMin, ElectionTimeoutMax: testElectionTimeoutMax,
		HeartbeatInterval: testHeartbeatInterval, RPCTimeout: testRPCTimeout,
	})
	network.Register("a", a)
	network.Register("b", b)
	network.Register("c", c)

	// c starts isolated: its persistence already fails, so it couldn't
	// safely vote or lead anyway; keep it out of the initial election so
	// a/b can elect a leader between themselves (a majority of 2 of 3).
	network.SetUnreachable("c", true)

	ab := []*Node{a, b}
	for _, n := range ab {
		n.Start()
	}
	t.Cleanup(func() {
		for _, n := range ab {
			n.Stop()
		}
	})

	leader := waitForLeader(t, ab)
	if _, _, ok := leader.Propose([]byte("x")); !ok {
		t.Fatal("Propose failed")
	}
	eventually(t, 2*time.Second, 5*time.Millisecond, func() bool {
		return leader.State().CommitIndex >= 1
	})

	// Bring c into the picture: it will receive AppendEntries, but its
	// own log persistence always fails.
	c.Start()
	t.Cleanup(c.Stop)
	network.SetUnreachable("c", false)

	time.Sleep(10 * testHeartbeatInterval)
	if got := matchIndexFor(leader, "c"); got != 0 {
		t.Fatalf("leader's matchIndex for c = %d, want 0 (c's persistence always fails, so it must never be counted as replicated)", got)
	}
	if c.State().PersistenceError == nil {
		t.Fatal("expected c to have recorded a persistence error after receiving AppendEntries it couldn't persist")
	}
}
