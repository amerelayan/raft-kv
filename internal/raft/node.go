// Package raft implements Raft consensus: follower, candidate, and
// leader roles, randomized-timeout leader election, term handling,
// heartbeats, and replicated-log AppendEntries with majority-based
// commit. It does not implement client write routing, snapshots, log
// compaction, membership changes, or durable persistence — those are
// later stages.
package raft

import (
	"context"
	"hash/fnv"
	"math/rand"
	"sync"
	"time"
)

// Config configures a new Node.
type Config struct {
	// ID is this node's own identifier.
	ID string
	// Peers lists the IDs of the other nodes in the cluster. It must not
	// include ID.
	Peers []string

	// Transport sends RPCs to peers, identified by their ID.
	Transport Transport
	// Persister persists currentTerm, votedFor, and the log. Defaults to
	// NoopPersister if nil.
	Persister Persister

	// ElectionTimeoutMin/Max bound the randomized election timeout.
	// Default 150ms/300ms.
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	// HeartbeatInterval is how often a leader sends AppendEntries
	// (heartbeats, and replication when there are new entries). Default
	// 50ms.
	HeartbeatInterval time.Duration
	// RPCTimeout bounds a single RequestVote or AppendEntries call.
	// Default 100ms.
	RPCTimeout time.Duration
}

// Node is a single participant in a Raft cluster.
type Node struct {
	id        string
	peers     []string
	transport Transport
	persister Persister

	electionTimeoutMin time.Duration
	electionTimeoutMax time.Duration
	heartbeatInterval  time.Duration
	rpcTimeout         time.Duration

	rngMu sync.Mutex
	rng   *rand.Rand

	mu           sync.Mutex
	currentTerm  uint64
	votedFor     string
	role         Role
	leaderID     string
	leaderStopCh chan struct{}
	stopped      bool

	// log[i].Index == i for every i; log[0] is a sentinel entry
	// (Index 0, Term 0) representing "before the log begins". There is
	// no snapshotting/compaction this stage, so that invariant holds for
	// the Node's whole lifetime.
	log []LogEntry

	// commitIndex/lastApplied are volatile on every node. nextIndex/
	// matchIndex are leader-only, reinitialized on every election win.
	commitIndex uint64
	lastApplied uint64
	nextIndex   map[string]uint64
	matchIndex  map[string]uint64

	applyCh       chan AppliedEntry
	applyNotifyCh chan struct{}
	replicateCh   chan struct{}

	stopCh          chan struct{}
	resetElectionCh chan struct{}
	wg              sync.WaitGroup
}

// defaultRPCTimeout bounds a single RequestVote or AppendEntries call. It
// is also used by TCPTransport as the read/write deadline on the server
// side of an accepted RPC connection, so the two stay consistent: a
// connection that can't complete within one RPC's worth of time is
// abandoned on both ends.
const defaultRPCTimeout = 100 * time.Millisecond

// applyChannelBuffer bounds how many committed entries can queue up
// before a consumer of ApplyChannel has read them. Once full, applyLoop
// blocks (interruptibly, via Stop) rather than dropping entries — commit
// tracking (commitIndex) itself is never held up by a slow consumer,
// only delivery on the channel is.
const applyChannelBuffer = 64

// NewNode creates a Node from cfg. Call Start to begin participating in
// elections and applying committed entries.
func NewNode(cfg Config) *Node {
	if cfg.ElectionTimeoutMin == 0 {
		cfg.ElectionTimeoutMin = 150 * time.Millisecond
	}
	if cfg.ElectionTimeoutMax == 0 {
		cfg.ElectionTimeoutMax = 300 * time.Millisecond
	}
	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = 50 * time.Millisecond
	}
	if cfg.RPCTimeout == 0 {
		cfg.RPCTimeout = defaultRPCTimeout
	}
	if cfg.Persister == nil {
		cfg.Persister = NoopPersister{}
	}

	n := &Node{
		id:                 cfg.ID,
		peers:              append([]string(nil), cfg.Peers...),
		transport:          cfg.Transport,
		persister:          cfg.Persister,
		electionTimeoutMin: cfg.ElectionTimeoutMin,
		electionTimeoutMax: cfg.ElectionTimeoutMax,
		heartbeatInterval:  cfg.HeartbeatInterval,
		rpcTimeout:         cfg.RPCTimeout,
		rng:                rand.New(rand.NewSource(seedFor(cfg.ID))),
		role:               Follower,
		log:                []LogEntry{{}}, // index-0 sentinel
		applyCh:            make(chan AppliedEntry, applyChannelBuffer),
		applyNotifyCh:      make(chan struct{}, 1),
		replicateCh:        make(chan struct{}, 1),
		stopCh:             make(chan struct{}),
		resetElectionCh:    make(chan struct{}, 1),
	}

	if term, votedFor, log, err := cfg.Persister.LoadState(); err == nil {
		n.currentTerm = term
		n.votedFor = votedFor
		if len(log) > 0 {
			n.log = log
		}
	}

	return n
}

func seedFor(id string) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(id))
	return int64(h.Sum64()) ^ time.Now().UnixNano()
}

// ID returns the node's own ID.
func (n *Node) ID() string { return n.id }

// State returns a snapshot of the node's current term, role, known
// leader, and replication progress.
func (n *Node) State() State {
	n.mu.Lock()
	defer n.mu.Unlock()
	return State{
		ID:          n.id,
		Term:        n.currentTerm,
		Role:        n.role,
		LeaderID:    n.leaderID,
		VotedFor:    n.votedFor,
		CommitIndex: n.commitIndex,
		LastApplied: n.lastApplied,
	}
}

// Log returns a copy of the node's current log, including the index-0
// sentinel entry. Primarily useful for tests and observability.
func (n *Node) Log() []LogEntry {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]LogEntry(nil), n.log...)
}

// ApplyChannel returns the channel committed entries are delivered on, in
// order, exactly once, as lastApplied advances. Nothing in this stage
// reads from it — it is the hook a future state machine (e.g. the KV
// store) will consume.
func (n *Node) ApplyChannel() <-chan AppliedEntry {
	return n.applyCh
}

// Propose appends command to the log if this node currently believes
// itself to be the leader, returning the index and term the entry
// occupies and true. It returns false if this node is not the leader; it
// does not wait for the entry to be committed — callers that need that
// should watch ApplyChannel or poll State().CommitIndex.
//
// Nothing calls Propose from cmd/node yet; it exists so the replication
// machinery has a real entry point, matching the one a future client
// write-routing stage will use.
func (n *Node) Propose(command []byte) (index uint64, term uint64, isLeader bool) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.role != Leader {
		return 0, 0, false
	}

	index = n.lastLogIndexLocked() + 1
	term = n.currentTerm
	entry := LogEntry{Term: term, Index: index, Command: append([]byte(nil), command...)}
	n.log = append(n.log, entry)
	n.persistStateLocked()

	// A single-node cluster (or one already at majority via matchIndex)
	// can commit immediately; replicateToPeer's reply handler covers the
	// normal multi-node case.
	n.maybeAdvanceCommitIndexLocked()

	n.signalReplicate()
	return index, term, true
}

// Start begins the election timer and the apply loop, both in background
// goroutines. Call it once.
func (n *Node) Start() {
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		n.electionLoop()
	}()
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		n.applyLoop()
	}()
}

// Stop terminates the election timer, the apply loop, and any active
// leader replication loop, and waits for all of them to exit. Safe to
// call more than once.
func (n *Node) Stop() {
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return
	}
	n.stopped = true
	if n.role == Leader && n.leaderStopCh != nil {
		close(n.leaderStopCh)
		n.leaderStopCh = nil
	}
	n.mu.Unlock()

	close(n.stopCh)
	n.wg.Wait()
}

func (n *Node) electionLoop() {
	for {
		timeout := n.randomElectionTimeout()
		select {
		case <-n.stopCh:
			return
		case <-time.After(timeout):
			n.mu.Lock()
			isLeader := n.role == Leader
			n.mu.Unlock()
			if !isLeader {
				n.startElection()
			}
		case <-n.resetElectionCh:
			// Loop again with a freshly randomized timeout.
		}
	}
}

// applyLoop delivers committed-but-not-yet-applied entries on applyCh, in
// order, exactly once. It never holds n.mu while sending, so a slow or
// absent consumer can never block an RPC handler.
func (n *Node) applyLoop() {
	for {
		select {
		case <-n.stopCh:
			return
		case <-n.applyNotifyCh:
		}

		for {
			n.mu.Lock()
			if n.lastApplied >= n.commitIndex {
				n.mu.Unlock()
				break
			}
			n.lastApplied++
			entry := n.log[n.lastApplied]
			n.mu.Unlock()

			select {
			case n.applyCh <- AppliedEntry{Index: entry.Index, Term: entry.Term, Command: entry.Command}:
			case <-n.stopCh:
				return
			}
		}
	}
}

func (n *Node) randomElectionTimeout() time.Duration {
	span := int64(n.electionTimeoutMax - n.electionTimeoutMin)
	if span <= 0 {
		return n.electionTimeoutMin
	}
	n.rngMu.Lock()
	jitter := n.rng.Int63n(span)
	n.rngMu.Unlock()
	return n.electionTimeoutMin + time.Duration(jitter)
}

func (n *Node) signalElectionReset() {
	select {
	case n.resetElectionCh <- struct{}{}:
	default:
	}
}

// signalReplicate wakes the leader's replication loop immediately rather
// than waiting for the next heartbeat tick. Safe to call whether or not
// this node is currently leading (harmless no-op if there is no active
// replication loop to consume it, or if it arrives after a step-down).
func (n *Node) signalReplicate() {
	select {
	case n.replicateCh <- struct{}{}:
	default:
	}
}

// maybeSignalApplyLocked wakes the apply loop. Non-blocking, so it is
// safe to call while holding n.mu.
func (n *Node) maybeSignalApplyLocked() {
	select {
	case n.applyNotifyCh <- struct{}{}:
	default:
	}
}

func (n *Node) persistStateLocked() {
	_ = n.persister.SaveState(n.currentTerm, n.votedFor, n.log)
}

// lastLogIndexLocked, lastLogTermLocked, and logTermAtLocked all rely on
// the log[i].Index == i invariant (true for this stage, with no
// snapshotting), so they can index directly rather than searching.

func (n *Node) lastLogIndexLocked() uint64 {
	return n.log[len(n.log)-1].Index
}

func (n *Node) lastLogTermLocked() uint64 {
	return n.log[len(n.log)-1].Term
}

func (n *Node) logTermAtLocked(index uint64) (term uint64, ok bool) {
	if index >= uint64(len(n.log)) {
		return 0, false
	}
	return n.log[index].Term, true
}

func (n *Node) majorityLocked() int {
	return (len(n.peers)+1)/2 + 1
}

// logIsUpToDateLocked implements the Raft paper §5.4.1 freshness
// comparison: a later term wins outright; equal terms fall back to
// comparing log length.
func (n *Node) logIsUpToDateLocked(candidateLastTerm, candidateLastIndex uint64) bool {
	myLastTerm := n.lastLogTermLocked()
	if candidateLastTerm != myLastTerm {
		return candidateLastTerm > myLastTerm
	}
	return candidateLastIndex >= n.lastLogIndexLocked()
}

// becomeFollowerLocked steps down to Follower for a newly observed,
// strictly higher term, resetting votedFor for the new term. Callers
// must hold n.mu and must only call this when term > n.currentTerm.
func (n *Node) becomeFollowerLocked(term uint64) {
	n.stepDownToFollowerLocked()
	n.currentTerm = term
	n.votedFor = ""
	n.persistStateLocked()
}

// stepDownToFollowerLocked demotes the node to Follower without changing
// its term — used when a same-term AppendEntries reveals a legitimate
// leader while this node is a Candidate (Raft paper §5.2). Callers must
// hold n.mu.
func (n *Node) stepDownToFollowerLocked() {
	if n.role == Leader && n.leaderStopCh != nil {
		close(n.leaderStopCh)
		n.leaderStopCh = nil
	}
	n.role = Follower
}

// becomeLeaderLocked transitions to Leader for the current term,
// reinitializes nextIndex/matchIndex for every peer, and starts the
// replication loop. Callers must hold n.mu.
func (n *Node) becomeLeaderLocked() {
	if n.stopped {
		return
	}
	n.role = Leader
	n.leaderID = n.id
	term := n.currentTerm

	lastIndex := n.lastLogIndexLocked()
	n.nextIndex = make(map[string]uint64, len(n.peers))
	n.matchIndex = make(map[string]uint64, len(n.peers))
	for _, peer := range n.peers {
		n.nextIndex[peer] = lastIndex + 1
		n.matchIndex[peer] = 0
	}

	stopCh := make(chan struct{})
	n.leaderStopCh = stopCh

	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		n.runReplicationLoop(term, stopCh)
	}()
}

// startElection increments the term, votes for self, and requests votes
// from every peer concurrently.
func (n *Node) startElection() {
	n.mu.Lock()
	n.currentTerm++
	term := n.currentTerm
	n.role = Candidate
	n.votedFor = n.id
	n.persistStateLocked()
	lastLogIndex := n.lastLogIndexLocked()
	lastLogTerm := n.lastLogTermLocked()
	peers := append([]string(nil), n.peers...)
	total := len(peers) + 1
	majority := total/2 + 1
	votes := 1 // self
	if votes >= majority {
		n.becomeLeaderLocked()
	}
	n.mu.Unlock()

	if len(peers) == 0 {
		return
	}

	remaining := votes
	votesPtr := &remaining

	for _, peer := range peers {
		go func(peer string) {
			args := &RequestVoteArgs{
				Term:         term,
				CandidateID:  n.id,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			ctx, cancel := context.WithTimeout(context.Background(), n.rpcTimeout)
			defer cancel()

			reply, err := n.transport.RequestVote(ctx, peer, args)
			if err != nil {
				return
			}

			n.mu.Lock()
			if reply.Term > n.currentTerm {
				n.becomeFollowerLocked(reply.Term)
				n.mu.Unlock()
				n.signalElectionReset()
				return
			}
			if n.role != Candidate || n.currentTerm != term || !reply.VoteGranted {
				n.mu.Unlock()
				return
			}
			*votesPtr++
			if *votesPtr >= majority {
				n.becomeLeaderLocked()
			}
			n.mu.Unlock()
		}(peer)
	}
}

func (n *Node) runReplicationLoop(term uint64, stopCh chan struct{}) {
	n.replicate(term)

	ticker := time.NewTicker(n.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-stopCh:
			return
		case <-n.stopCh:
			return
		case <-ticker.C:
			n.replicate(term)
		case <-n.replicateCh:
			n.replicate(term)
		}
	}
}

// replicate fans out one round of AppendEntries to every peer, each
// carrying whatever entries that peer's nextIndex says it still needs
// (possibly none, i.e. a pure heartbeat).
func (n *Node) replicate(term uint64) {
	n.mu.Lock()
	if n.role != Leader || n.currentTerm != term {
		n.mu.Unlock()
		return
	}
	peers := append([]string(nil), n.peers...)
	leaderID := n.id
	leaderCommit := n.commitIndex
	n.mu.Unlock()

	for _, peer := range peers {
		go n.replicateToPeer(term, peer, leaderID, leaderCommit)
	}
}

func (n *Node) replicateToPeer(term uint64, peer, leaderID string, leaderCommit uint64) {
	n.mu.Lock()
	if n.role != Leader || n.currentTerm != term {
		n.mu.Unlock()
		return
	}
	nextIdx := n.nextIndex[peer]
	prevLogIndex := nextIdx - 1
	prevLogTerm, _ := n.logTermAtLocked(prevLogIndex) // always ok: no compaction
	var entries []LogEntry
	if nextIdx < uint64(len(n.log)) {
		entries = append([]LogEntry(nil), n.log[nextIdx:]...)
	}
	n.mu.Unlock()

	args := &AppendEntriesArgs{
		Term:         term,
		LeaderID:     leaderID,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: leaderCommit,
	}
	ctx, cancel := context.WithTimeout(context.Background(), n.rpcTimeout)
	defer cancel()

	reply, err := n.transport.AppendEntries(ctx, peer, args)
	if err != nil {
		return
	}

	n.mu.Lock()
	if reply.Term > n.currentTerm {
		n.becomeFollowerLocked(reply.Term)
		n.mu.Unlock()
		n.signalElectionReset()
		return
	}
	if n.role != Leader || n.currentTerm != term {
		n.mu.Unlock()
		return
	}

	if reply.Success {
		newMatch := prevLogIndex + uint64(len(entries))
		// Guard against an out-of-order reply (e.g. a retried request's
		// reply arriving after a newer one already advanced progress)
		// regressing state that's already ahead.
		if newMatch > n.matchIndex[peer] {
			n.matchIndex[peer] = newMatch
			if newMatch+1 > n.nextIndex[peer] {
				n.nextIndex[peer] = newMatch + 1
			}
			n.maybeAdvanceCommitIndexLocked()
		}
	} else if n.nextIndex[peer] > n.matchIndex[peer]+1 {
		// A stale, out-of-order rejection (e.g. from an older in-flight
		// request superseded by a newer one that already succeeded) must
		// never push nextIndex back down to or below matchIndex: that's
		// already-confirmed progress, and the decrement below it would
		// only ever be wrong.
		n.nextIndex[peer]--
	}
	n.mu.Unlock()
}

// maybeAdvanceCommitIndexLocked implements Raft's commit rule (§5.4.2):
// advance commitIndex to the highest N for which a majority's matchIndex
// is >= N and log[N].Term == currentTerm. Only counting current-term
// entries directly is deliberate — Raft never commits an entry from a
// previous term purely by counting replicas; such entries are committed
// only as a side effect of a later current-term entry committing, since
// the Log Matching Property guarantees anything below a replicated N is
// already identical across that same majority. Callers must hold n.mu.
func (n *Node) maybeAdvanceCommitIndexLocked() {
	if n.role != Leader {
		return
	}
	majority := n.majorityLocked()
	for N := n.lastLogIndexLocked(); N > n.commitIndex; N-- {
		term, ok := n.logTermAtLocked(N)
		if !ok || term != n.currentTerm {
			continue
		}
		count := 1 // the leader itself has this entry
		for _, peer := range n.peers {
			if n.matchIndex[peer] >= N {
				count++
			}
		}
		if count >= majority {
			n.commitIndex = N
			n.maybeSignalApplyLocked()
			return
		}
	}
}

// HandleRequestVote processes an incoming RequestVote RPC.
func (n *Node) HandleRequestVote(args *RequestVoteArgs) *RequestVoteReply {
	n.mu.Lock()

	if args.Term < n.currentTerm {
		reply := &RequestVoteReply{Term: n.currentTerm, VoteGranted: false}
		n.mu.Unlock()
		return reply
	}

	if args.Term > n.currentTerm {
		n.becomeFollowerLocked(args.Term)
	}

	grant := (n.votedFor == "" || n.votedFor == args.CandidateID) &&
		n.logIsUpToDateLocked(args.LastLogTerm, args.LastLogIndex)
	if grant {
		n.votedFor = args.CandidateID
		n.persistStateLocked()
	}
	reply := &RequestVoteReply{Term: n.currentTerm, VoteGranted: grant}
	n.mu.Unlock()

	if grant {
		n.signalElectionReset()
	}
	return reply
}

// HandleAppendEntries processes an incoming AppendEntries RPC: term and
// leadership handling, the PrevLogIndex/PrevLogTerm consistency check,
// merging new entries (deleting any conflicting suffix first), and
// advancing commitIndex from LeaderCommit.
func (n *Node) HandleAppendEntries(args *AppendEntriesArgs) *AppendEntriesReply {
	n.mu.Lock()

	if args.Term < n.currentTerm {
		reply := &AppendEntriesReply{Term: n.currentTerm, Success: false}
		n.mu.Unlock()
		return reply
	}

	if args.Term > n.currentTerm {
		n.becomeFollowerLocked(args.Term)
	} else if n.role != Follower {
		n.stepDownToFollowerLocked()
	}
	n.leaderID = args.LeaderID

	term, ok := n.logTermAtLocked(args.PrevLogIndex)
	if !ok || term != args.PrevLogTerm {
		reply := &AppendEntriesReply{Term: n.currentTerm, Success: false}
		n.mu.Unlock()
		n.signalElectionReset() // still a legitimate leader; just log-inconsistent
		return reply
	}

	insertAt := args.PrevLogIndex + 1
	i := 0
	for ; i < len(args.Entries); i++ {
		idx := insertAt + uint64(i)
		if idx >= uint64(len(n.log)) {
			break
		}
		if n.log[idx].Term != args.Entries[i].Term {
			n.log = n.log[:idx] // conflict: delete this entry and everything after
			break
		}
		// Entry already present and matching; skip (idempotent retry).
	}
	if i < len(args.Entries) {
		n.log = append(n.log, args.Entries[i:]...)
		n.persistStateLocked()
	}

	if args.LeaderCommit > n.commitIndex {
		lastNew := args.PrevLogIndex + uint64(len(args.Entries))
		if args.LeaderCommit < lastNew {
			n.commitIndex = args.LeaderCommit
		} else {
			n.commitIndex = lastNew
		}
		n.maybeSignalApplyLocked()
	}

	reply := &AppendEntriesReply{Term: n.currentTerm, Success: true}
	n.mu.Unlock()

	n.signalElectionReset()
	return reply
}
