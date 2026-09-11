// Package raft implements the Raft node state machine: follower,
// candidate, and leader roles, randomized-timeout leader election, term
// handling, and heartbeats. It does not implement log replication,
// client write routing, or durable persistence — those are later stages.
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
	// Persister persists currentTerm and votedFor. Defaults to
	// NoopPersister if nil.
	Persister Persister

	// ElectionTimeoutMin/Max bound the randomized election timeout.
	// Default 150ms/300ms.
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	// HeartbeatInterval is how often a leader sends heartbeats. Default
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

// NewNode creates a Node from cfg. Call Start to begin participating in
// elections.
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
		stopCh:             make(chan struct{}),
		resetElectionCh:    make(chan struct{}, 1),
	}

	if term, votedFor, err := cfg.Persister.LoadState(); err == nil {
		n.currentTerm = term
		n.votedFor = votedFor
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

// State returns a snapshot of the node's current term, role, and known
// leader.
func (n *Node) State() State {
	n.mu.Lock()
	defer n.mu.Unlock()
	return State{
		ID:       n.id,
		Term:     n.currentTerm,
		Role:     n.role,
		LeaderID: n.leaderID,
		VotedFor: n.votedFor,
	}
}

// Start begins the election timer in a background goroutine. Call it
// once.
func (n *Node) Start() {
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		n.electionLoop()
	}()
}

// Stop terminates the election timer and any active leader heartbeat
// loop, and waits for both to exit. Safe to call more than once.
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

func (n *Node) persistStateLocked() {
	_ = n.persister.SaveState(n.currentTerm, n.votedFor)
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

// becomeLeaderLocked transitions to Leader for the current term and
// starts sending heartbeats. Callers must hold n.mu.
func (n *Node) becomeLeaderLocked() {
	if n.stopped {
		return
	}
	n.role = Leader
	n.leaderID = n.id
	term := n.currentTerm
	stopCh := make(chan struct{})
	n.leaderStopCh = stopCh

	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		n.runHeartbeats(term, stopCh)
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
			args := &RequestVoteArgs{Term: term, CandidateID: n.id}
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

func (n *Node) runHeartbeats(term uint64, stopCh chan struct{}) {
	n.sendHeartbeats(term)

	ticker := time.NewTicker(n.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-stopCh:
			return
		case <-n.stopCh:
			return
		case <-ticker.C:
			n.sendHeartbeats(term)
		}
	}
}

func (n *Node) sendHeartbeats(term uint64) {
	n.mu.Lock()
	if n.role != Leader || n.currentTerm != term {
		n.mu.Unlock()
		return
	}
	peers := append([]string(nil), n.peers...)
	leaderID := n.id
	n.mu.Unlock()

	for _, peer := range peers {
		go func(peer string) {
			args := &AppendEntriesArgs{Term: term, LeaderID: leaderID}
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
			n.mu.Unlock()
		}(peer)
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

	grant := n.votedFor == "" || n.votedFor == args.CandidateID
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

// HandleAppendEntries processes an incoming AppendEntries RPC. This stage
// only implements the heartbeat case: args.Entries is always empty.
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
	reply := &AppendEntriesReply{Term: n.currentTerm, Success: true}
	n.mu.Unlock()

	n.signalElectionReset()
	return reply
}
