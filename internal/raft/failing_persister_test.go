package raft

import (
	"errors"
	"sync"
)

// errInjectedSaveFailure is returned by failingPersister when it's set
// to fail, so tests can recognize it if needed (e.g. via errors.Is).
var errInjectedSaveFailure = errors.New("raft: injected SaveState failure")

// failingPersister wraps another Persister (defaulting to NoopPersister)
// and lets a test make SaveState fail deterministically, on demand,
// rather than relying on any real, probabilistic I/O failure. LoadState
// always just delegates to the inner Persister.
type failingPersister struct {
	mu        sync.Mutex
	inner     Persister
	fail      bool
	saveCalls int
}

// newFailingPersister creates a failingPersister wrapping inner (or an
// in-memory NoopPersister if inner is nil). SaveState succeeds by
// default; call SetFail(true) to start failing it.
func newFailingPersister(inner Persister) *failingPersister {
	if inner == nil {
		inner = NoopPersister{}
	}
	return &failingPersister{inner: inner}
}

// SetFail controls whether future SaveState calls fail. Safe to toggle
// at any time; it does not affect calls already in flight.
func (p *failingPersister) SetFail(fail bool) {
	p.mu.Lock()
	p.fail = fail
	p.mu.Unlock()
}

// SaveCalls reports how many times SaveState has been called (whether
// or not it was made to fail), for tests that need to confirm something
// did or didn't attempt to persist.
func (p *failingPersister) SaveCalls() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.saveCalls
}

func (p *failingPersister) SaveState(term uint64, votedFor string, log []LogEntry) error {
	p.mu.Lock()
	p.saveCalls++
	fail := p.fail
	p.mu.Unlock()

	if fail {
		return errInjectedSaveFailure
	}
	return p.inner.SaveState(term, votedFor, log)
}

func (p *failingPersister) LoadState() (uint64, string, []LogEntry, error) {
	return p.inner.LoadState()
}
