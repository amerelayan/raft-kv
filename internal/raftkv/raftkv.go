// Package raftkv implements server.KV on top of Raft consensus: writes
// are proposed to the Raft log and only acknowledged once committed and
// applied to the local store; reads are served from local state only
// when this node currently believes itself to be the leader.
package raftkv

import (
	"context"
	"sync"
	"time"

	"raftkv/internal/raft"
)

// localStore is the minimal local storage interface RaftKV applies
// committed commands to and serves leader reads from. *store.Store
// satisfies it. It's defined here — structurally identical to
// server.KV — rather than imported from internal/server, so this
// package doesn't need to depend on the client-facing server package
// just to describe its own dependency.
type localStore interface {
	Put(key, value string) error
	Get(key string) (value string, ok bool, err error)
	Delete(key string) error
}

// defaultProposalTimeout bounds how long a client write waits for its
// proposal to be committed and applied, if Config doesn't specify one.
const defaultProposalTimeout = 2 * time.Second

// appliedResultCacheSize bounds how many unclaimed applied-result
// records RaftKV retains (see appliedResult). Most recorded entries are
// never claimed at all — a follower applying a remote leader's entries
// has no local waiter for almost anything it applies — so this is a
// small, cheap, self-pruning cache, not a log: a record is removed the
// instant a matching proposeAndWait call claims it, and the cache is
// capped so the far more common unclaimed case can never accumulate
// unbounded. The cap only needs to comfortably outlast the handful of
// instructions between Node.Propose returning and proposeAndWait
// acquiring kv.mu to check for a race-induced early result; it is not a
// tuned limit.
const appliedResultCacheSize = 64

// Config configures a RaftKV.
type Config struct {
	// Node is the Raft node writes are proposed to and committed entries
	// are consumed from.
	Node *raft.Node
	// Store is the local state machine committed commands are applied
	// to, and leader reads are served from.
	Store localStore
	// ProposalTimeout bounds how long a write waits for its proposal to
	// be committed and applied before returning ErrTimeout. Defaults to
	// 2s.
	ProposalTimeout time.Duration
}

// RaftKV implements server.KV: PUT/DELETE are proposed to Raft and only
// acknowledged after they commit and are applied to the local store; GET
// is served from the local store, but only when this node currently
// believes itself to be the Raft leader.
//
// This is a simple leader-read model, not a full Raft ReadIndex or
// leader-lease implementation: a node can serve a read for a brief
// window after it has actually lost leadership but before it has
// discovered that (e.g. mid-partition, before its next election
// timeout fires). That window is bounded by the cluster's election
// timeout, but it is not eliminated. Linearizable reads under
// partition would require ReadIndex or leases; this implementation
// deliberately prioritizes correctness-of-writes and simplicity over
// that (see the README's Known Limitations section).
type RaftKV struct {
	node  *raft.Node
	store localStore

	timeout time.Duration

	mu      sync.Mutex
	waiters map[uint64]waiter
	applied map[uint64]appliedResult
	closed  bool

	stopCh chan struct{}
	wg     sync.WaitGroup
}

type waiter struct {
	term   uint64
	result chan error
}

// appliedResult is what applyOne records, under kv.mu, when it applies
// an entry but finds no local waiter registered for its index yet. term
// is carried alongside the outcome so a later check (proposeAndWait,
// racing to register right as this was recorded) can tell whether this
// is really its own entry or a different one that happens to reuse the
// same index from a different term.
type appliedResult struct {
	term uint64
	err  error
}

// New creates a RaftKV. Call Start once, after node.Start(), to begin
// consuming committed entries.
func New(cfg Config) *RaftKV {
	timeout := cfg.ProposalTimeout
	if timeout == 0 {
		timeout = defaultProposalTimeout
	}
	return &RaftKV{
		node:    cfg.Node,
		store:   cfg.Store,
		timeout: timeout,
		waiters: make(map[uint64]waiter),
		applied: make(map[uint64]appliedResult),
		stopCh:  make(chan struct{}),
	}
}

// Start begins consuming committed entries from the Raft node's apply
// channel in a background goroutine, applying them to the local store in
// order. Call it once.
func (kv *RaftKV) Start() {
	kv.wg.Add(1)
	go func() {
		defer kv.wg.Done()
		kv.applyConsumer()
	}()
}

// Stop terminates the apply consumer, fails every still-pending waiter,
// and waits for the consumer goroutine to exit. Safe to call more than
// once.
func (kv *RaftKV) Stop() {
	kv.mu.Lock()
	if kv.closed {
		kv.mu.Unlock()
		return
	}
	kv.closed = true
	close(kv.stopCh)
	waiters := kv.waiters
	kv.waiters = make(map[uint64]waiter)
	kv.mu.Unlock()

	for _, w := range waiters {
		w.result <- errStopped
	}

	kv.wg.Wait()
}

// Put proposes a PUT command to Raft and blocks until it has been
// committed by a majority and applied to this node's local store, then
// returns nil — or returns an error (NotLeaderError, LeadershipLostError,
// ErrTimeout) without ever mutating the local store if it doesn't.
func (kv *RaftKV) Put(key, value string) error {
	return kv.proposeAndWait(command{Op: opPut, Key: key, Value: value})
}

// Delete proposes a DELETE command to Raft and blocks until it has been
// committed and applied, exactly like Put.
func (kv *RaftKV) Delete(key string) error {
	return kv.proposeAndWait(command{Op: opDelete, Key: key})
}

// Get serves a read from the local store, but only if this node
// currently believes itself to be the Raft leader — see RaftKV's doc
// comment for the consistency model this provides. GET is never
// replicated.
func (kv *RaftKV) Get(key string) (string, bool, error) {
	st := kv.node.State()
	if st.Role != raft.Leader {
		return "", false, &NotLeaderError{LeaderID: st.LeaderID}
	}
	return kv.store.Get(key)
}

// proposeAndWait proposes cmd to Raft and then waits for its outcome via
// waitForResult.
func (kv *RaftKV) proposeAndWait(cmd command) error {
	kv.mu.Lock()
	if kv.closed {
		kv.mu.Unlock()
		return errStopped
	}
	kv.mu.Unlock()

	encoded, err := encodeCommand(cmd)
	if err != nil {
		return err
	}

	index, term, isLeader := kv.node.Propose(encoded)
	if !isLeader {
		return &NotLeaderError{LeaderID: kv.node.State().LeaderID}
	}

	return kv.waitForResult(index, term)
}

// waitForResult waits for the outcome of the proposal at (index, term):
// commit-and-apply (success), being superseded by a different entry at
// the same index (LeadershipLostError), or the configured timeout
// elapsing (ErrTimeout). Never a sleep or poll.
//
// It is possible for the entry to have already been committed and
// applied by the time this runs — most easily on a single-node cluster,
// where Node.Propose can advance commitIndex synchronously, so
// applyConsumer can process the entry before the proposing goroutine
// gets here at all. So the very first thing this does, atomically under
// the same lock as registering to be notified, is check whether
// applyOne already recorded an outcome for this exact (index, term) — if
// so, that outcome is consumed and returned immediately instead of
// waiting out the full timeout for an event that already happened.
func (kv *RaftKV) waitForResult(index, term uint64) error {
	resultCh := make(chan error, 1)

	kv.mu.Lock()
	if kv.closed {
		kv.mu.Unlock()
		return errStopped
	}
	if ar, ok := kv.applied[index]; ok {
		delete(kv.applied, index)
		kv.mu.Unlock()
		if ar.term != term {
			return &LeadershipLostError{LeaderID: kv.node.State().LeaderID}
		}
		return ar.err
	}
	kv.waiters[index] = waiter{term: term, result: resultCh}
	kv.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), kv.timeout)
	defer cancel()

	select {
	case err := <-resultCh:
		return err
	case <-ctx.Done():
		kv.mu.Lock()
		// Only remove if it's still our waiter: applyConsumer may have
		// already resolved and deleted it (benign race with the timeout
		// firing at the same moment), or — much less likely — a later
		// proposal could in principle reuse the same index after a
		// leadership change and log truncation, in which case this must
		// not delete that different, live waiter.
		if w, ok := kv.waiters[index]; ok && w.result == resultCh {
			delete(kv.waiters, index)
		}
		kv.mu.Unlock()
		return ErrTimeout
	case <-kv.stopCh:
		return errStopped
	}
}

// applyConsumer reads committed entries from the Raft node's apply
// channel and applies them in order until Stop is called.
func (kv *RaftKV) applyConsumer() {
	for {
		select {
		case <-kv.stopCh:
			return
		case entry := <-kv.node.ApplyChannel():
			kv.applyOne(entry)
		}
	}
}

// applyOne decodes and applies a single committed entry, then resolves
// any local waiter registered for its index. A command that fails to
// decode is safely skipped — never applied, never allowed to crash the
// loop — and any waiter for that index is still resolved (with the
// decode error) so it can never leak.
//
// If no waiter is registered yet, the outcome is recorded in kv.applied
// instead of simply discarded — see waitForResult for why (the race this
// closes) and appliedResultCacheSize for why this can never grow
// unbounded.
func (kv *RaftKV) applyOne(entry raft.AppliedEntry) {
	cmd, applyErr := decodeCommand(entry.Command)
	if applyErr == nil {
		switch cmd.Op {
		case opPut:
			applyErr = kv.store.Put(cmd.Key, cmd.Value)
		case opDelete:
			applyErr = kv.store.Delete(cmd.Key)
		}
	}

	kv.mu.Lock()
	w, ok := kv.waiters[entry.Index]
	if ok {
		delete(kv.waiters, entry.Index)
	} else {
		if len(kv.applied) >= appliedResultCacheSize {
			for k := range kv.applied {
				delete(kv.applied, k)
				break
			}
		}
		kv.applied[entry.Index] = appliedResult{term: entry.Term, err: applyErr}
	}
	kv.mu.Unlock()

	if !ok {
		return
	}

	switch {
	case applyErr != nil:
		w.result <- applyErr
	case w.term != entry.Term:
		w.result <- &LeadershipLostError{LeaderID: kv.node.State().LeaderID}
	default:
		w.result <- nil
	}
}
