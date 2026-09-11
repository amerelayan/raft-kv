package raft

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// restartClusterElectionTimeoutMin/Max/HeartbeatInterval/RPCTimeout are
// deliberately wider than the package's usual testElectionTimeoutMin
// etc.: those are tuned for a purely in-memory FakeTransport with
// NoopPersister, where a full election/heartbeat round trip is
// essentially free. A FilePersister does real, synchronous disk I/O
// (write, fsync, rename, fsync the directory) on every term/vote/log
// change, including on every AppendEntries a leader sends once it's
// replicating anything -- occasional fsync latency of a few to several
// milliseconds is normal, and the tiny package-wide timing constants
// don't leave enough margin to absorb that without risking a spurious
// election. Only the tests that actually run a live, multi-node cluster
// with real timers need this; tests that drive a single Node directly
// through its RPC handlers never wait out a real timeout regardless of
// these values.
const (
	restartClusterElectionTimeoutMin = 300 * time.Millisecond
	restartClusterElectionTimeoutMax = 600 * time.Millisecond
	restartClusterHeartbeatInterval  = 50 * time.Millisecond
	restartClusterRPCTimeout         = 100 * time.Millisecond
)

// newRestartableNode creates a Node backed by a real FilePersister at
// statePath, using the package's small/fast test timing (fine for tests
// that only drive the node directly through its RPC handlers, never
// through a live election/heartbeat loop).
func newRestartableNode(t *testing.T, id string, peers []string, transport Transport, statePath string) *Node {
	t.Helper()
	return newRestartableNodeWithTiming(t, id, peers, transport, statePath,
		testElectionTimeoutMin, testElectionTimeoutMax, testHeartbeatInterval, testRPCTimeout)
}

func newRestartableNodeWithTiming(t *testing.T, id string, peers []string, transport Transport, statePath string, electionMin, electionMax, heartbeat, rpcTimeout time.Duration) *Node {
	t.Helper()
	persister, err := NewFilePersister(statePath)
	if err != nil {
		t.Fatalf("NewFilePersister: %v", err)
	}
	n, err := NewNode(Config{
		ID:                 id,
		Peers:              peers,
		Transport:          transport,
		Persister:          persister,
		ElectionTimeoutMin: electionMin,
		ElectionTimeoutMax: electionMax,
		HeartbeatInterval:  heartbeat,
		RPCTimeout:         rpcTimeout,
	})
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	return n
}

// 1 & 2. Persisted term and vote survive restart.
func TestPersistedTermAndVoteSurviveRestart(t *testing.T) {
	statePath := filepath.Join(t.TempDir(), "state.json")

	n1 := newRestartableNode(t, "a", nil, NewFakeTransport(NewNetwork(), "a"), statePath)
	n1.HandleRequestVote(&RequestVoteArgs{Term: 5, CandidateID: "b"})
	st1 := n1.State()
	if st1.Term != 5 || st1.VotedFor != "b" {
		t.Fatalf("setup: term=%d votedFor=%q, want term=5 votedFor=\"b\"", st1.Term, st1.VotedFor)
	}

	// "Restart": a brand new Node, new FilePersister instance, same file.
	n2 := newRestartableNode(t, "a", nil, NewFakeTransport(NewNetwork(), "a"), statePath)
	st2 := n2.State()
	if st2.Term != 5 {
		t.Fatalf("term after restart = %d, want 5", st2.Term)
	}
	if st2.VotedFor != "b" {
		t.Fatalf("votedFor after restart = %q, want %q", st2.VotedFor, "b")
	}
}

// 3 & 4. Log survives restart, with correct indexes/terms/commands.
func TestLogSurvivesRestartWithCorrectIndexesTermsCommands(t *testing.T) {
	statePath := filepath.Join(t.TempDir(), "state.json")

	n1 := newRestartableNode(t, "a", nil, NewFakeTransport(NewNetwork(), "a"), statePath)
	n1.HandleAppendEntries(&AppendEntriesArgs{
		Term: 1, LeaderID: "leader", PrevLogIndex: 0, PrevLogTerm: 0,
		Entries: []LogEntry{
			{Index: 1, Term: 1, Command: []byte("a")},
			{Index: 2, Term: 1, Command: []byte("b")},
		},
	})
	// A separate round at a higher term, matching how a real term
	// increase actually happens (an RPC's own Term always covers its
	// entries -- an entry can never claim a higher term than the RPC
	// carrying it).
	n1.HandleAppendEntries(&AppendEntriesArgs{
		Term: 2, LeaderID: "leader", PrevLogIndex: 2, PrevLogTerm: 1,
		Entries: []LogEntry{{Index: 3, Term: 2, Command: []byte("c")}},
	})
	wantLog := n1.Log()
	if len(wantLog) != 4 {
		t.Fatalf("setup: log length = %d, want 4", len(wantLog))
	}

	n2 := newRestartableNode(t, "a", nil, NewFakeTransport(NewNetwork(), "a"), statePath)
	gotLog := n2.Log()
	if !logsEqual(wantLog, gotLog) {
		t.Fatalf("log after restart = %+v, want %+v", gotLog, wantLog)
	}
	// Spell out the exact fields too, not just logsEqual's summary.
	for i, want := range wantLog {
		got := gotLog[i]
		if got.Index != want.Index || got.Term != want.Term || string(got.Command) != string(want.Command) {
			t.Fatalf("entry %d after restart = %+v, want %+v", i, got, want)
		}
	}
}

// 5. Conflicting-log truncation is persisted and survives restart.
func TestConflictingTruncationPersistsAcrossRestart(t *testing.T) {
	statePath := filepath.Join(t.TempDir(), "state.json")

	n1 := newRestartableNode(t, "a", nil, NewFakeTransport(NewNetwork(), "a"), statePath)
	n1.HandleAppendEntries(&AppendEntriesArgs{
		Term: 1, LeaderID: "old", PrevLogIndex: 0, PrevLogTerm: 0,
		Entries: []LogEntry{
			{Index: 1, Term: 1, Command: []byte("a")},
			{Index: 2, Term: 1, Command: []byte("b")},
			{Index: 3, Term: 1, Command: []byte("c")},
		},
	})
	// A new leader at term 2 overwrites from index 2 onward.
	n1.HandleAppendEntries(&AppendEntriesArgs{
		Term: 2, LeaderID: "new", PrevLogIndex: 1, PrevLogTerm: 1,
		Entries: []LogEntry{{Index: 2, Term: 2, Command: []byte("x")}},
	})
	wantLog := n1.Log()
	if len(wantLog) != 3 {
		t.Fatalf("setup: log length = %d, want 3", len(wantLog))
	}
	if string(wantLog[2].Command) != "x" {
		t.Fatalf("setup: entry 2 = %+v, want the overwritten entry", wantLog[2])
	}

	n2 := newRestartableNode(t, "a", nil, NewFakeTransport(NewNetwork(), "a"), statePath)
	if !logsEqual(wantLog, n2.Log()) {
		t.Fatalf("log after restart = %+v, want %+v (the truncated-and-overwritten version)", n2.Log(), wantLog)
	}
}

// 6. Restart does not restore volatile leader/follower state.
func TestRestartDoesNotRestoreVolatileState(t *testing.T) {
	statePath := filepath.Join(t.TempDir(), "state.json")
	network := NewNetwork()

	n1 := newRestartableNode(t, "a", []string{"b"}, NewFakeTransport(network, "a"), statePath)
	network.Register("a", n1)

	// Force it into Candidate state without a real cluster: "b" is never
	// registered, so this can never win, but it does bump the term,
	// vote for self, and set role=Candidate.
	n1.startElection()
	pre := n1.State()
	if pre.Role != Candidate {
		t.Fatalf("setup: role = %s, want Candidate", pre.Role)
	}

	n2 := newRestartableNode(t, "a", []string{"b"}, NewFakeTransport(NewNetwork(), "a"), statePath)
	post := n2.State()
	if post.Role != Follower {
		t.Fatalf("role after restart = %s, want Follower", post.Role)
	}
	if post.LeaderID != "" {
		t.Fatalf("leaderID after restart = %q, want empty", post.LeaderID)
	}
	if post.CommitIndex != 0 {
		t.Fatalf("commitIndex after restart = %d, want 0", post.CommitIndex)
	}
	if post.LastApplied != 0 {
		t.Fatalf("lastApplied after restart = %d, want 0", post.LastApplied)
	}
	// Term and votedFor, in contrast, ARE persistent and must survive.
	if post.Term != pre.Term {
		t.Fatalf("term after restart = %d, want %d (term is persistent)", post.Term, pre.Term)
	}
	if post.VotedFor != pre.VotedFor {
		t.Fatalf("votedFor after restart = %q, want %q (votedFor is persistent)", post.VotedFor, pre.VotedFor)
	}
}

// 7 & 8. A restarted node can rejoin a running cluster, and a lagging
// restarted node catches up from the current leader.
func TestRestartedNodeRejoinsAndCatchesUp(t *testing.T) {
	network := NewNetwork()
	ids := []string{"a", "b", "c"}
	peersOf := func(id string) []string {
		var peers []string
		for _, other := range ids {
			if other != id {
				peers = append(peers, other)
			}
		}
		return peers
	}

	statePaths := make(map[string]string, len(ids))
	byID := make(map[string]*Node, len(ids))
	all := make([]*Node, 0, len(ids))
	for _, id := range ids {
		statePaths[id] = filepath.Join(t.TempDir(), "state.json")
		n := newRestartableNodeWithTiming(t, id, peersOf(id), NewFakeTransport(network, id), statePaths[id],
			restartClusterElectionTimeoutMin, restartClusterElectionTimeoutMax, restartClusterHeartbeatInterval, restartClusterRPCTimeout)
		network.Register(id, n)
		byID[id] = n
		all = append(all, n)
	}

	for _, n := range all {
		n.Start()
	}
	t.Cleanup(func() {
		for _, n := range all {
			n.Stop()
		}
	})

	firstLeader := waitForLeader(t, all)

	// Pick a follower (not the leader, whichever node that happens to
	// be) to "crash" and restart, so a leader stays available among the
	// other two to propose the entries the restarted node needs to
	// catch up on.
	var victimID string
	for _, id := range ids {
		if byID[id] != firstLeader {
			victimID = id
			break
		}
	}
	victim := byID[victimID]

	var remaining []*Node
	for _, n := range all {
		if n != victim {
			remaining = append(remaining, n)
		}
	}

	// Isolate it (so nothing further reaches it) and stop it, simulating
	// a crash.
	network.SetUnreachable(victimID, true)
	victim.Stop()

	// The cluster keeps making progress without it (a majority of 2 of
	// 3 is enough). Propose against whichever of the two remaining nodes
	// currently holds leadership, retrying if that changes mid-test
	// (real disk I/O on every persisted change means an occasional
	// slower fsync can legitimately cost a node its lease to a rival
	// election; a real client would retry exactly like this too, rather
	// than assume a leader reference stays valid forever).
	proposeOnCurrentLeader := func(cmd []byte) {
		t.Helper()
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if ls := leaders(remaining); len(ls) == 1 {
				if _, _, ok := ls[0].Propose(cmd); ok {
					return
				}
			}
			time.Sleep(5 * time.Millisecond)
		}
		t.Fatal("no stable leader among the remaining nodes to propose to")
	}
	for i := 0; i < 3; i++ {
		proposeOnCurrentLeader([]byte{byte('x' + i)})
	}

	var finalLeader *Node
	eventually(t, 2*time.Second, 5*time.Millisecond, func() bool {
		ls := leaders(remaining)
		if len(ls) != 1 {
			return false
		}
		finalLeader = ls[0]
		return finalLeader.State().CommitIndex >= 3
	})

	// "Restart" the victim: a fresh Node reading the same on-disk state,
	// wired back into the network under the same ID.
	newVictim := newRestartableNodeWithTiming(t, victimID, peersOf(victimID), NewFakeTransport(network, victimID), statePaths[victimID],
		restartClusterElectionTimeoutMin, restartClusterElectionTimeoutMax, restartClusterHeartbeatInterval, restartClusterRPCTimeout)
	network.Register(victimID, newVictim) // replaces the old (stopped) handler
	network.SetUnreachable(victimID, false)
	newVictim.Start()
	t.Cleanup(newVictim.Stop)

	// It rejoins and catches all the way up with whichever node is
	// leader by the time convergence happens.
	eventually(t, 2*time.Second, 5*time.Millisecond, func() bool {
		ls := leaders(remaining)
		if len(ls) != 1 {
			return false
		}
		return logsEqual(ls[0].Log(), newVictim.Log())
	})
}

// 9. A node does not vote twice in the same term across a restart.
func TestNodeDoesNotVoteTwiceInSameTermAcrossRestart(t *testing.T) {
	statePath := filepath.Join(t.TempDir(), "state.json")

	n1 := newRestartableNode(t, "a", nil, NewFakeTransport(NewNetwork(), "a"), statePath)
	r1 := n1.HandleRequestVote(&RequestVoteArgs{Term: 3, CandidateID: "b"})
	if !r1.VoteGranted {
		t.Fatal("expected the first vote request in term 3 to be granted")
	}

	// Restart: a fresh in-memory Node, same persisted file.
	n2 := newRestartableNode(t, "a", nil, NewFakeTransport(NewNetwork(), "a"), statePath)

	// A different candidate asks for a vote in the SAME term after
	// restart -- must be rejected, because this node (across the
	// restart) already voted for "b" in term 3.
	r2 := n2.HandleRequestVote(&RequestVoteArgs{Term: 3, CandidateID: "c"})
	if r2.VoteGranted {
		t.Fatal("node voted twice in the same term across a restart")
	}

	// The original candidate asking again (e.g. a retried RPC) is still
	// correctly granted (idempotent).
	r3 := n2.HandleRequestVote(&RequestVoteArgs{Term: 3, CandidateID: "b"})
	if !r3.VoteGranted {
		t.Fatal("expected a repeated request from the already-voted-for candidate to still be granted")
	}
}

// 10. Corrupted persistence is detected (at Node construction, not
// silently downgraded to empty state).
func TestCorruptedPersistenceIsDetectedAtStartup(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	if err := os.WriteFile(path, []byte("this is not json"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	persister, err := NewFilePersister(path)
	if err != nil {
		t.Fatalf("NewFilePersister: %v", err)
	}

	_, err = NewNode(Config{ID: "a", Persister: persister})
	if err == nil {
		t.Fatal("expected NewNode to fail on corrupted persisted state, not silently start with empty state")
	}
}

// 11. Partially written/temp files do not cause silent state loss: a
// crash that leaves a stray, incomplete temp file behind (SaveState's
// own temp files are never renamed into place unless complete) must not
// affect what LoadState / NewNode sees, and NewNode must still recover
// the last good, fully-written state.
func TestStrayTempFileDoesNotCauseSilentStateLoss(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")

	persister, err := NewFilePersister(path)
	if err != nil {
		t.Fatalf("NewFilePersister: %v", err)
	}
	wantLog := []LogEntry{{Index: 0, Term: 0}, {Index: 1, Term: 1, Command: []byte("a")}}
	if err := persister.SaveState(4, "b", wantLog); err != nil {
		t.Fatalf("SaveState: %v", err)
	}

	// Simulate a crash mid-write on a LATER save: a stray, incomplete
	// temp file left behind in the same directory, never renamed over
	// the real state file.
	strayPath := filepath.Join(dir, ".raft-state-stray.tmp")
	if err := os.WriteFile(strayPath, []byte(`{"version":1,"current_term":99`), 0o644); err != nil {
		t.Fatalf("WriteFile (stray temp): %v", err)
	}

	n, err := NewNode(Config{ID: "a", Persister: persister})
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	st := n.State()
	if st.Term != 4 {
		t.Fatalf("term = %d, want 4 (the last good save, unaffected by the stray temp file)", st.Term)
	}
	if st.VotedFor != "b" {
		t.Fatalf("votedFor = %q, want %q", st.VotedFor, "b")
	}
	if !logsEqual(n.Log(), wantLog) {
		t.Fatalf("log = %+v, want %+v", n.Log(), wantLog)
	}
}

// 12. Repeated save/reload cycles preserve identical state (at the Node
// level; internal/raft/file_persister_test.go covers the same property
// directly at the Persister level).
func TestNodeRepeatedRestartCyclesPreserveIdenticalState(t *testing.T) {
	statePath := filepath.Join(t.TempDir(), "state.json")

	var lastLog []LogEntry
	for i := 0; i < 5; i++ {
		n := newRestartableNode(t, "a", nil, NewFakeTransport(NewNetwork(), "a"), statePath)

		if lastLog != nil && !logsEqual(n.Log(), lastLog) {
			t.Fatalf("cycle %d: log at startup = %+v, want %+v (from the previous cycle)", i, n.Log(), lastLog)
		}

		last := n.Log()[len(n.Log())-1]
		n.HandleAppendEntries(&AppendEntriesArgs{
			Term:         uint64(i + 1),
			LeaderID:     "leader",
			PrevLogIndex: last.Index,
			PrevLogTerm:  last.Term,
			Entries:      []LogEntry{{Index: last.Index + 1, Term: uint64(i + 1), Command: []byte{byte(i)}}},
		})
		lastLog = n.Log()
	}

	final := newRestartableNode(t, "a", nil, NewFakeTransport(NewNetwork(), "a"), statePath)
	if !logsEqual(final.Log(), lastLog) {
		t.Fatalf("final log = %+v, want %+v", final.Log(), lastLog)
	}
	if got := final.State().Term; got != 5 {
		t.Fatalf("final term = %d, want 5", got)
	}
}
