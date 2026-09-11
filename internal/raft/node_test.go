package raft

import (
	"testing"
	"time"
)

func TestThreeNodeClusterElectsExactlyOneLeader(t *testing.T) {
	nodes, _ := newTestCluster(t, 3)
	startAll(nodes)

	var leader *Node
	eventually(t, 2*time.Second, 5*time.Millisecond, func() bool {
		ls := leaders(nodes)
		if len(ls) == 1 {
			leader = ls[0]
			return true
		}
		return false
	})

	term := leader.State().Term
	for _, n := range nodes {
		st := n.State()
		if st.Term == term && st.Role == Leader && n != leader {
			t.Fatalf("node %s is also a leader in term %d", st.ID, term)
		}
	}
}

func TestFollowersStayFollowersDuringHeartbeats(t *testing.T) {
	nodes, _ := newTestCluster(t, 3)
	startAll(nodes)

	var leaderTerm uint64
	eventually(t, 2*time.Second, 5*time.Millisecond, func() bool {
		ls := leaders(nodes)
		if len(ls) == 1 {
			leaderTerm = ls[0].State().Term
			return true
		}
		return false
	})

	// Give the leader several heartbeat intervals to keep the cluster
	// settled, then confirm nothing re-elected.
	time.Sleep(10 * testHeartbeatInterval)

	followerCount := 0
	for _, n := range nodes {
		st := n.State()
		if st.Role == Follower {
			followerCount++
			if st.Term != leaderTerm {
				t.Fatalf("follower %s term %d does not match leader term %d", st.ID, st.Term, leaderTerm)
			}
		}
	}
	if followerCount != 2 {
		t.Fatalf("expected 2 followers, got %d", followerCount)
	}
}

func TestLeaderFailureTriggersNewElection(t *testing.T) {
	nodes, _ := newTestCluster(t, 3)
	startAll(nodes)

	var leader *Node
	eventually(t, 2*time.Second, 5*time.Millisecond, func() bool {
		ls := leaders(nodes)
		if len(ls) == 1 {
			leader = ls[0]
			return true
		}
		return false
	})
	oldTerm := leader.State().Term
	leader.Stop()

	remaining := make([]*Node, 0, 2)
	for _, n := range nodes {
		if n != leader {
			remaining = append(remaining, n)
		}
	}

	eventually(t, 2*time.Second, 5*time.Millisecond, func() bool {
		ls := leaders(remaining)
		return len(ls) == 1 && ls[0].State().Term > oldTerm
	})
}

func TestOldLeaderStepsDownOnStaleTerm(t *testing.T) {
	nodes, network := newTestCluster(t, 3)
	startAll(nodes)

	var leader *Node
	eventually(t, 2*time.Second, 5*time.Millisecond, func() bool {
		ls := leaders(nodes)
		if len(ls) == 1 {
			leader = ls[0]
			return true
		}
		return false
	})
	oldTerm := leader.State().Term

	// Isolate the leader: it keeps believing it's leader and keeps
	// sending heartbeats, but nothing reaches it and nothing from it
	// reaches anyone else.
	network.SetUnreachable(leader.ID(), true)

	var others []*Node
	for _, n := range nodes {
		if n != leader {
			others = append(others, n)
		}
	}

	eventually(t, 2*time.Second, 5*time.Millisecond, func() bool {
		ls := leaders(others)
		return len(ls) == 1 && ls[0].State().Term > oldTerm
	})

	// Heal the partition. The old leader's next heartbeat will draw a
	// higher-term reply from the new leader's followers, and it must step
	// down.
	network.SetUnreachable(leader.ID(), false)

	eventually(t, 2*time.Second, 5*time.Millisecond, func() bool {
		st := leader.State()
		return st.Role == Follower && st.Term > oldTerm
	})
}

func TestHandleRequestVoteRejectsStaleTerm(t *testing.T) {
	network := NewNetwork()
	n := NewNode(Config{ID: "a", Peers: []string{"b"}, Transport: NewFakeTransport(network, "a")})
	network.Register("a", n)

	// Advance the node to term 5 the same way it would happen normally:
	// observing a higher term via an RPC.
	n.HandleAppendEntries(&AppendEntriesArgs{Term: 5, LeaderID: "b"})
	if got := n.State().Term; got != 5 {
		t.Fatalf("term = %d, want 5", got)
	}

	reply := n.HandleRequestVote(&RequestVoteArgs{Term: 3, CandidateID: "c"})
	if reply.VoteGranted {
		t.Fatal("expected vote to be rejected for a stale term")
	}
	if reply.Term != 5 {
		t.Fatalf("reply.Term = %d, want 5", reply.Term)
	}
	if got := n.State().VotedFor; got != "" {
		t.Fatalf("votedFor = %q, want empty (a stale request must not consume a vote)", got)
	}
}

func TestHandleAppendEntriesRejectsStaleTerm(t *testing.T) {
	network := NewNetwork()
	n := NewNode(Config{ID: "a", Peers: []string{"b"}, Transport: NewFakeTransport(network, "a")})
	network.Register("a", n)

	n.HandleRequestVote(&RequestVoteArgs{Term: 5, CandidateID: "b"}) // bumps term to 5, votes for b

	reply := n.HandleAppendEntries(&AppendEntriesArgs{Term: 3, LeaderID: "stale-leader"})
	if reply.Success {
		t.Fatal("expected stale-term heartbeat to be rejected")
	}
	if reply.Term != 5 {
		t.Fatalf("reply.Term = %d, want 5", reply.Term)
	}
	if st := n.State(); st.LeaderID == "stale-leader" {
		t.Fatal("a stale-term leader must not be adopted as the known leader")
	}
}

func TestHandleRequestVoteGrantsOncePerTerm(t *testing.T) {
	network := NewNetwork()
	n := NewNode(Config{ID: "a", Peers: []string{"b", "c"}, Transport: NewFakeTransport(network, "a")})
	network.Register("a", n)

	r1 := n.HandleRequestVote(&RequestVoteArgs{Term: 1, CandidateID: "b"})
	if !r1.VoteGranted {
		t.Fatal("expected the first vote request in term 1 to be granted")
	}

	r2 := n.HandleRequestVote(&RequestVoteArgs{Term: 1, CandidateID: "c"})
	if r2.VoteGranted {
		t.Fatal("expected a second, different candidate in the same term to be rejected")
	}

	r3 := n.HandleRequestVote(&RequestVoteArgs{Term: 1, CandidateID: "b"})
	if !r3.VoteGranted {
		t.Fatal("expected a repeated request from the already-voted-for candidate to be granted again")
	}
}

func TestCandidateRequiresTrueMajority(t *testing.T) {
	nodes, network := newTestCluster(t, 5)
	candidate := nodes[0]

	// Isolate the candidate's would-be electorate down to 1 reachable
	// peer: 1 (self) + 1 = 2 votes, short of the majority of 3 needed in
	// a 5-node cluster. (This also isolates those 3 peers from each
	// other, so no node in the cluster can reach a majority.)
	for _, n := range nodes[2:] {
		network.SetUnreachable(n.ID(), true)
	}

	startAll(nodes)

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if candidate.State().Role == Leader {
			t.Fatal("candidate became leader with only 2 of 5 votes")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestConcurrentElectionsNeverProduceTwoLeadersInSameTerm(t *testing.T) {
	nodes, _ := newTestCluster(t, 5)
	startAll(nodes)

	// Force every node to attempt an election at (as close to) the same
	// moment as possible, several times, to manufacture the
	// concurrent-candidacy scenario deterministically rather than relying
	// on randomized timers to collide on their own.
	for round := 0; round < 5; round++ {
		for _, n := range nodes {
			go n.startElection()
		}
		time.Sleep(20 * time.Millisecond)

		seen := make(map[uint64]string)
		for _, n := range nodes {
			st := n.State()
			if st.Role != Leader {
				continue
			}
			if other, ok := seen[st.Term]; ok {
				t.Fatalf("two leaders in term %d: %s and %s", st.Term, other, st.ID)
			}
			seen[st.Term] = st.ID
		}
	}

	// Despite the induced chaos, the cluster still converges on exactly
	// one leader.
	eventually(t, 2*time.Second, 5*time.Millisecond, func() bool {
		return len(leaders(nodes)) == 1
	})
}

func TestCandidateStepsDownOnSameTermAppendEntries(t *testing.T) {
	network := NewNetwork()
	// "b" is never registered, so it is permanently unreachable: n's own
	// startElection can never actually win, guaranteeing it stays a
	// Candidate deterministically (no timing dependency) until something
	// else changes its role.
	n := NewNode(Config{ID: "a", Peers: []string{"b"}, Transport: NewFakeTransport(network, "a")})
	network.Register("a", n)

	n.startElection()
	st := n.State()
	if st.Role != Candidate || st.Term != 1 {
		t.Fatalf("expected Candidate at term 1 after startElection, got %+v", st)
	}

	// Raft paper §5.2: a candidate that receives an AppendEntries from a
	// leader whose term is at least as large as its own must recognize
	// that leader and return to Follower state.
	reply := n.HandleAppendEntries(&AppendEntriesArgs{Term: 1, LeaderID: "b"})
	if !reply.Success {
		t.Fatalf("expected AppendEntries to succeed, got %+v", reply)
	}

	st = n.State()
	if st.Role != Follower {
		t.Fatalf("expected node to step down to Follower, got role %s", st.Role)
	}
	if st.Term != 1 {
		t.Fatalf("term = %d, want 1 (a same-term step-down must not bump the term)", st.Term)
	}
	if st.LeaderID != "b" {
		t.Fatalf("leaderID = %q, want %q", st.LeaderID, "b")
	}
}

func TestSameTermStepDownPreservesVotedFor(t *testing.T) {
	network := NewNetwork()
	n := NewNode(Config{ID: "a", Peers: []string{"b"}, Transport: NewFakeTransport(network, "a")})
	network.Register("a", n)

	n.startElection() // votes for itself ("a") at term 1
	if got := n.State().VotedFor; got != "a" {
		t.Fatalf("votedFor = %q, want %q after self-vote", got, "a")
	}

	n.HandleAppendEntries(&AppendEntriesArgs{Term: 1, LeaderID: "b"}) // same-term step-down

	st := n.State()
	if st.Role != Follower {
		t.Fatalf("expected Follower after step-down, got %s", st.Role)
	}
	if st.VotedFor != "a" {
		t.Fatalf("votedFor = %q, want %q (a same-term step-down must not clear the vote)", st.VotedFor, "a")
	}

	// The safety consequence of the above: since this node already voted
	// for itself in term 1, it must not also grant its vote to a
	// different candidate in that same term after stepping down.
	reply := n.HandleRequestVote(&RequestVoteArgs{Term: 1, CandidateID: "c"})
	if reply.VoteGranted {
		t.Fatal("expected a different candidate's vote request in the same term to be rejected")
	}
}

func TestSplitVoteRetriesWithHigherTerm(t *testing.T) {
	nodes, _ := newTestCluster(t, 4) // majority = 3

	// Manufacture a genuine split vote at term 1: two of the four nodes
	// commit their only term-1 vote to phantom candidates before anything
	// starts. Since a node grants at most one vote per term, node-2 and
	// node-3's votes are now permanently unavailable for term 1 — whoever
	// among node-0/node-1 ends up as the real term-1 candidate(s) can
	// reach at most 2 of 4 votes (self + the other, if it hasn't also
	// voted elsewhere), short of the majority of 3. This holds
	// deterministically regardless of exactly how the real election
	// timers interleave.
	if r := nodes[2].HandleRequestVote(&RequestVoteArgs{Term: 1, CandidateID: "phantom-a"}); !r.VoteGranted {
		t.Fatal("expected node-2 to grant its term-1 vote to the phantom candidate")
	}
	if r := nodes[3].HandleRequestVote(&RequestVoteArgs{Term: 1, CandidateID: "phantom-b"}); !r.VoteGranted {
		t.Fatal("expected node-3 to grant its term-1 vote to the phantom candidate")
	}

	startAll(nodes)

	// Nobody can possibly win term 1.
	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			if st := n.State(); st.Term == 1 && st.Role == Leader {
				t.Fatalf("%s became leader in term 1 despite the manufactured split vote", st.ID)
			}
		}
		time.Sleep(5 * time.Millisecond)
	}

	// The split does not wedge the cluster permanently: normal election
	// timeouts drive a later attempt, at a strictly higher term, that
	// does succeed.
	eventually(t, 2*time.Second, 5*time.Millisecond, func() bool {
		ls := leaders(nodes)
		return len(ls) == 1 && ls[0].State().Term > 1
	})
}
