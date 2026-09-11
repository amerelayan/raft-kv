package raft

import (
	"testing"
	"time"
)

// 1. Leader appends an entry.
func TestLeaderAppendsEntry(t *testing.T) {
	nodes, _ := newTestCluster(t, 3)
	startAll(nodes)
	leader := waitForLeader(t, nodes)

	index, term, ok := leader.Propose([]byte("set x=1"))
	if !ok {
		t.Fatal("expected Propose to succeed on the leader")
	}
	if index != 1 {
		t.Fatalf("index = %d, want 1", index)
	}
	if term != leader.State().Term {
		t.Fatalf("term = %d, want the leader's current term %d", term, leader.State().Term)
	}

	log := leader.Log()
	if len(log) != 2 { // sentinel + 1 entry
		t.Fatalf("leader log length = %d, want 2", len(log))
	}
	if log[1].Index != 1 || log[1].Term != term || string(log[1].Command) != "set x=1" {
		t.Fatalf("leader's own log entry = %+v, unexpected", log[1])
	}
}

// 2. Followers replicate the entry.
func TestFollowersReplicateEntry(t *testing.T) {
	nodes, _ := newTestCluster(t, 3)
	startAll(nodes)
	leader := waitForLeader(t, nodes)

	_, term, ok := leader.Propose([]byte("set x=1"))
	if !ok {
		t.Fatal("expected Propose to succeed")
	}

	eventually(t, 2*time.Second, 5*time.Millisecond, func() bool {
		for _, n := range nodes {
			log := n.Log()
			if len(log) < 2 || log[1].Term != term || string(log[1].Command) != "set x=1" {
				return false
			}
		}
		return true
	})
}

// 3. Majority replication advances commitIndex.
func TestMajorityReplicationAdvancesCommitIndex(t *testing.T) {
	nodes, _ := newTestCluster(t, 3)
	startAll(nodes)
	leader := waitForLeader(t, nodes)

	index, _, ok := leader.Propose([]byte("cmd"))
	if !ok {
		t.Fatal("expected Propose to succeed")
	}

	eventually(t, 2*time.Second, 5*time.Millisecond, func() bool {
		return leader.State().CommitIndex >= index
	})
}

// 4. Entry is not committed without a majority.
func TestEntryNotCommittedWithoutMajority(t *testing.T) {
	nodes, network := newTestCluster(t, 3)
	startAll(nodes)
	leader := waitForLeader(t, nodes)

	// Isolate the leader from everyone: alone, it is not a majority of 3.
	network.SetUnreachable(leader.ID(), true)

	index, _, ok := leader.Propose([]byte("cmd"))
	if !ok {
		t.Fatal("expected Propose to succeed (appending to the local log doesn't require connectivity)")
	}

	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		if leader.State().CommitIndex >= index {
			t.Fatal("entry committed without a majority of the cluster replicating it")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// 5. Follower rejects AppendEntries with wrong PrevLogIndex.
func TestFollowerRejectsWrongPrevLogIndex(t *testing.T) {
	network := NewNetwork()
	n := mustNewNode(t, Config{ID: "a", Peers: []string{"b"}, Transport: NewFakeTransport(network, "a")})
	network.Register("a", n)

	// n's log only has the sentinel (index 0); PrevLogIndex=5 can't exist.
	reply := n.HandleAppendEntries(&AppendEntriesArgs{
		Term: 1, LeaderID: "b", PrevLogIndex: 5, PrevLogTerm: 1,
	})
	if reply.Success {
		t.Fatal("expected rejection for a PrevLogIndex beyond the end of the log")
	}
	if reply.Term != 1 {
		t.Fatalf("reply.Term = %d, want 1", reply.Term)
	}
}

// 6. Follower rejects AppendEntries with wrong PrevLogTerm.
func TestFollowerRejectsWrongPrevLogTerm(t *testing.T) {
	network := NewNetwork()
	n := mustNewNode(t, Config{ID: "a", Peers: []string{"b"}, Transport: NewFakeTransport(network, "a")})
	network.Register("a", n)

	ok1 := n.HandleAppendEntries(&AppendEntriesArgs{
		Term: 1, LeaderID: "b", PrevLogIndex: 0, PrevLogTerm: 0,
		Entries: []LogEntry{{Index: 1, Term: 1, Command: []byte("x")}},
	})
	if !ok1.Success {
		t.Fatalf("expected initial append to succeed, got %+v", ok1)
	}

	// Claim PrevLogIndex=1 but with the wrong term (2, not 1).
	reply := n.HandleAppendEntries(&AppendEntriesArgs{
		Term: 2, LeaderID: "b", PrevLogIndex: 1, PrevLogTerm: 2,
	})
	if reply.Success {
		t.Fatal("expected rejection for a mismatched PrevLogTerm")
	}
}

// 7. Conflicting follower log entries are overwritten correctly.
func TestConflictingEntriesAreOverwritten(t *testing.T) {
	network := NewNetwork()
	n := mustNewNode(t, Config{ID: "a", Peers: []string{"b"}, Transport: NewFakeTransport(network, "a")})
	network.Register("a", n)

	r := n.HandleAppendEntries(&AppendEntriesArgs{
		Term: 1, LeaderID: "old-leader", PrevLogIndex: 0, PrevLogTerm: 0,
		Entries: []LogEntry{
			{Index: 1, Term: 1, Command: []byte("a")},
			{Index: 2, Term: 1, Command: []byte("b")},
			{Index: 3, Term: 1, Command: []byte("c")},
		},
	})
	if !r.Success {
		t.Fatalf("expected initial append to succeed, got %+v", r)
	}

	// A new leader at term 2 overwrites from index 2 onward.
	r = n.HandleAppendEntries(&AppendEntriesArgs{
		Term: 2, LeaderID: "new-leader", PrevLogIndex: 1, PrevLogTerm: 1,
		Entries: []LogEntry{
			{Index: 2, Term: 2, Command: []byte("x")},
			{Index: 3, Term: 2, Command: []byte("y")},
		},
	})
	if !r.Success {
		t.Fatalf("expected conflict-resolving append to succeed, got %+v", r)
	}

	log := n.Log()
	if len(log) != 4 { // sentinel + 3 entries
		t.Fatalf("log length = %d, want 4", len(log))
	}
	if log[1].Term != 1 || string(log[1].Command) != "a" {
		t.Fatalf("entry 1 = %+v, want the original unconflicted entry", log[1])
	}
	if log[2].Term != 2 || string(log[2].Command) != "x" {
		t.Fatalf("entry 2 = %+v, want the new leader's overwrite", log[2])
	}
	if log[3].Term != 2 || string(log[3].Command) != "y" {
		t.Fatalf("entry 3 = %+v, want the new leader's overwrite", log[3])
	}
}

// 8. Leader decrements/backtracks nextIndex after rejection, and the
// backtrack-and-retry cycle actually converges the follower's log.
func TestLeaderBacktracksNextIndexAfterRejection(t *testing.T) {
	// nextIndex is only ever set once, optimistically, when a node
	// becomes leader (one past its own last log index at that moment) --
	// it is never re-derived later as the leader's log grows. So a peer
	// isolated from before the very first Propose never causes a
	// rejection at all: its nextIndex starts at 1, and PrevLogIndex=0
	// always matches trivially. A genuine rejection requires a peer
	// whose nextIndex was set optimistically high (because a *new*
	// leader's own log already has entries this peer never received) --
	// exactly what happens after a leadership change. Five nodes are
	// used so 3 survivors (a real majority) can elect that new leader
	// while the lagging node stays isolated throughout.
	nodes, network := newTestCluster(t, 5)
	startAll(nodes)
	leader := waitForLeader(t, nodes)

	var lagging *Node
	for _, n := range nodes {
		if n != leader {
			lagging = n
			break
		}
	}

	// Isolate one follower before any entries are proposed, so it never
	// receives them.
	network.SetUnreachable(lagging.ID(), true)

	for i := 0; i < 3; i++ {
		if _, _, ok := leader.Propose([]byte{byte('a' + i)}); !ok {
			t.Fatalf("Propose %d failed", i)
		}
	}
	eventually(t, 2*time.Second, 5*time.Millisecond, func() bool {
		return leader.State().CommitIndex >= 3
	})

	// Kill the original leader. A new leader is elected from the
	// remaining nodes (a majority of 3 even excluding the isolated one),
	// and per Raft initializes its nextIndex for every peer
	// optimistically -- one past its own last log index -- without
	// knowing the isolated node never actually got anything.
	leader.Stop()

	var survivors []*Node
	for _, n := range nodes {
		if n != leader && n != lagging {
			survivors = append(survivors, n)
		}
	}
	newLeader := waitForLeader(t, survivors)

	before := nextIndexFor(newLeader, lagging.ID())
	if before != 4 {
		t.Fatalf("new leader's nextIndex for the lagging node = %d, want 4 (one past its own last log index)", before)
	}
	if got := matchIndexFor(newLeader, lagging.ID()); got != 0 {
		t.Fatalf("new leader's matchIndex for the lagging node = %d, want 0 (never yet contacted)", got)
	}

	// Reconnect: the new leader's optimistic nextIndex is wrong (the
	// lagging node has nothing), so its first attempt is rejected,
	// forcing a real backtrack.
	network.SetUnreachable(lagging.ID(), false)

	eventually(t, 2*time.Second, 5*time.Millisecond, func() bool {
		return nextIndexFor(newLeader, lagging.ID()) < before
	})

	// The backtrack-and-retry cycle must converge the lagging node's log
	// with the new leader's, not just move the counter once.
	eventually(t, 2*time.Second, 5*time.Millisecond, func() bool {
		return logsEqual(newLeader.Log(), lagging.Log())
	})

	// Once caught up, the standard invariant holds exactly: nextIndex is
	// one past the highest index known replicated.
	if got, want := nextIndexFor(newLeader, lagging.ID()), matchIndexFor(newLeader, lagging.ID())+1; got != want {
		t.Fatalf("nextIndex = %d, want matchIndex+1 = %d once caught up", got, want)
	}
}

// 9. Follower commitIndex advances from LeaderCommit.
func TestFollowerCommitIndexAdvancesFromLeaderCommit(t *testing.T) {
	network := NewNetwork()
	n := mustNewNode(t, Config{ID: "a", Peers: []string{"b"}, Transport: NewFakeTransport(network, "a")})
	network.Register("a", n)

	reply := n.HandleAppendEntries(&AppendEntriesArgs{
		Term: 1, LeaderID: "b", PrevLogIndex: 0, PrevLogTerm: 0,
		Entries: []LogEntry{
			{Index: 1, Term: 1, Command: []byte("a")},
			{Index: 2, Term: 1, Command: []byte("b")},
			{Index: 3, Term: 1, Command: []byte("c")},
		},
		LeaderCommit: 2,
	})
	if !reply.Success {
		t.Fatalf("expected append to succeed, got %+v", reply)
	}
	if got := n.State().CommitIndex; got != 2 {
		t.Fatalf("commitIndex = %d, want 2", got)
	}

	// LeaderCommit beyond what was actually sent this round is capped at
	// the index of the last new entry, not blindly trusted.
	reply = n.HandleAppendEntries(&AppendEntriesArgs{
		Term: 1, LeaderID: "b", PrevLogIndex: 3, PrevLogTerm: 1,
		LeaderCommit: 100,
	})
	if !reply.Success {
		t.Fatalf("expected heartbeat to succeed, got %+v", reply)
	}
	if got := n.State().CommitIndex; got != 3 {
		t.Fatalf("commitIndex = %d, want 3 (capped at last log index, not LeaderCommit)", got)
	}
}

// 10. Committed entries are applied exactly once and in order.
func TestCommittedEntriesAppliedOnceInOrder(t *testing.T) {
	network := NewNetwork()
	n := mustNewNode(t, Config{ID: "a", Peers: []string{"b"}, Transport: NewFakeTransport(network, "a")})
	network.Register("a", n)
	n.Start()
	t.Cleanup(n.Stop)

	reply := n.HandleAppendEntries(&AppendEntriesArgs{
		Term: 1, LeaderID: "b", PrevLogIndex: 0, PrevLogTerm: 0,
		Entries: []LogEntry{
			{Index: 1, Term: 1, Command: []byte("a")},
			{Index: 2, Term: 1, Command: []byte("b")},
			{Index: 3, Term: 1, Command: []byte("c")},
		},
		LeaderCommit: 3,
	})
	if !reply.Success {
		t.Fatalf("expected append to succeed, got %+v", reply)
	}

	applied := collectApplied(t, n, 3, 2*time.Second)
	for i, want := range []string{"a", "b", "c"} {
		if applied[i].Index != uint64(i+1) {
			t.Fatalf("applied[%d].Index = %d, want %d", i, applied[i].Index, i+1)
		}
		if string(applied[i].Command) != want {
			t.Fatalf("applied[%d].Command = %q, want %q", i, applied[i].Command, want)
		}
	}

	// Exactly once: nothing more should arrive.
	select {
	case extra := <-n.ApplyChannel():
		t.Fatalf("unexpected extra applied entry: %+v", extra)
	case <-time.After(50 * time.Millisecond):
	}

	if got := n.State().LastApplied; got != 3 {
		t.Fatalf("lastApplied = %d, want 3", got)
	}
}

// 11. RequestVote rejects a candidate with a stale log.
func TestRequestVoteRejectsStaleLog(t *testing.T) {
	network := NewNetwork()
	n := mustNewNode(t, Config{ID: "a", Peers: []string{"b"}, Transport: NewFakeTransport(network, "a")})
	network.Register("a", n)

	r := n.HandleAppendEntries(&AppendEntriesArgs{
		Term: 1, LeaderID: "b", PrevLogIndex: 0, PrevLogTerm: 0,
		Entries: []LogEntry{
			{Index: 1, Term: 1, Command: []byte("a")},
			{Index: 2, Term: 1, Command: []byte("b")},
		},
	})
	if !r.Success {
		t.Fatalf("setup append failed: %+v", r)
	}

	// A candidate at a higher term, but whose log ends at an older term,
	// must be rejected -- its log is less up to date even though its
	// term is higher.
	reply := n.HandleRequestVote(&RequestVoteArgs{
		Term: 5, CandidateID: "stale-candidate", LastLogIndex: 1, LastLogTerm: 0,
	})
	if reply.VoteGranted {
		t.Fatal("expected vote to be rejected for a candidate with a stale log")
	}

	// A candidate at the same log term but a shorter log must also be
	// rejected.
	reply = n.HandleRequestVote(&RequestVoteArgs{
		Term: 6, CandidateID: "short-candidate", LastLogIndex: 1, LastLogTerm: 1,
	})
	if reply.VoteGranted {
		t.Fatal("expected vote to be rejected for a candidate with a shorter log at the same term")
	}
}

// 12. RequestVote accepts an equally/up-to-date candidate when normal
// voting rules allow.
func TestRequestVoteAcceptsUpToDateLog(t *testing.T) {
	network := NewNetwork()
	n := mustNewNode(t, Config{ID: "a", Peers: []string{"b"}, Transport: NewFakeTransport(network, "a")})
	network.Register("a", n)

	r := n.HandleAppendEntries(&AppendEntriesArgs{
		Term: 1, LeaderID: "b", PrevLogIndex: 0, PrevLogTerm: 0,
		Entries: []LogEntry{{Index: 1, Term: 1, Command: []byte("a")}},
	})
	if !r.Success {
		t.Fatalf("setup append failed: %+v", r)
	}

	// Exactly as up to date (same last-log term and index) must be
	// granted.
	reply := n.HandleRequestVote(&RequestVoteArgs{
		Term: 2, CandidateID: "equal-candidate", LastLogIndex: 1, LastLogTerm: 1,
	})
	if !reply.VoteGranted {
		t.Fatal("expected vote to be granted for an equally up-to-date candidate")
	}

	// A strictly more up-to-date candidate must also be granted a vote.
	reply = n.HandleRequestVote(&RequestVoteArgs{
		Term: 3, CandidateID: "ahead-candidate", LastLogIndex: 2, LastLogTerm: 1,
	})
	if !reply.VoteGranted {
		t.Fatal("expected vote to be granted for a strictly more up-to-date candidate")
	}
}

// 13. A newly elected leader can bring a lagging follower up to date.
func TestNewLeaderCatchesUpLaggingFollower(t *testing.T) {
	nodes, network := newTestCluster(t, 3)
	startAll(nodes)
	leader := waitForLeader(t, nodes)

	var lagging *Node
	for _, n := range nodes {
		if n != leader {
			lagging = n
			break
		}
	}

	// Isolate one follower before any entries are proposed, so it starts
	// out behind.
	network.SetUnreachable(lagging.ID(), true)

	for i := 0; i < 3; i++ {
		if _, _, ok := leader.Propose([]byte{byte('a' + i)}); !ok {
			t.Fatalf("Propose %d failed", i)
		}
	}
	eventually(t, 2*time.Second, 5*time.Millisecond, func() bool {
		return leader.State().CommitIndex >= 3
	})

	if got := len(lagging.Log()); got != 1 {
		t.Fatalf("lagging follower log length = %d, want 1 (just the sentinel)", got)
	}

	// Reconnect it: the (possibly new, but likely the same) leader must
	// bring it fully up to date.
	network.SetUnreachable(lagging.ID(), false)

	eventually(t, 2*time.Second, 5*time.Millisecond, func() bool {
		return logsEqual(leader.Log(), lagging.Log())
	})
}

// 14. A minority partition cannot commit entries.
func TestMinorityPartitionCannotCommit(t *testing.T) {
	nodes, network := newTestCluster(t, 5) // majority = 3
	startAll(nodes)
	leader := waitForLeader(t, nodes)

	minority := []*Node{leader}
	var majority []*Node
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

	index, _, ok := leader.Propose([]byte("cmd"))
	if !ok {
		t.Fatal("expected Propose to succeed on the (still self-believed) leader")
	}

	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		if leader.State().CommitIndex >= index {
			t.Fatal("entry committed by a minority partition (2 of 5)")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// 15. After partition healing, logs converge (and the minority's
// uncommitted entry is discarded in favor of the majority's committed
// one).
func TestLogsConvergeAfterPartitionHeals(t *testing.T) {
	nodes, network := newTestCluster(t, 5)
	startAll(nodes)
	leader := waitForLeader(t, nodes)

	minority := []*Node{leader}
	var majority []*Node
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

	if _, _, ok := leader.Propose([]byte("orphaned")); !ok {
		t.Fatal("expected Propose to succeed on the old leader")
	}

	newLeader := waitForLeader(t, majority)
	index, _, ok := newLeader.Propose([]byte("committed"))
	if !ok {
		t.Fatal("expected Propose to succeed on the majority-side leader")
	}
	eventually(t, 2*time.Second, 5*time.Millisecond, func() bool {
		return newLeader.State().CommitIndex >= index
	})

	network.Heal()

	eventually(t, 2*time.Second, 5*time.Millisecond, func() bool {
		first := nodes[0].Log()
		for _, n := range nodes[1:] {
			if !logsEqual(first, n.Log()) {
				return false
			}
		}
		return true
	})

	final := nodes[0].Log()
	foundCommitted := false
	for _, e := range final {
		if string(e.Command) == "orphaned" {
			t.Fatal("the old minority leader's uncommitted entry survived partition healing")
		}
		if string(e.Command) == "committed" {
			foundCommitted = true
		}
	}
	if !foundCommitted {
		t.Fatal("the majority side's committed entry did not survive partition healing")
	}
}

func idsOf(nodes []*Node) []string {
	ids := make([]string, len(nodes))
	for i, n := range nodes {
		ids[i] = n.ID()
	}
	return ids
}
