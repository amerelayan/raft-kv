// Package store provides a thread-safe in-memory key-value store.
package store

import "sync"

// Store is a thread-safe in-memory key-value store.
type Store struct {
	mu   sync.RWMutex
	data map[string]string
}

// New creates an empty Store.
func New() *Store {
	return &Store{
		data: make(map[string]string),
	}
}

// Put sets the value for a key, creating or overwriting it. The error
// return exists to satisfy server.KV: a local, single-node store cannot
// fail a write, so it is always nil here. internal/raftkv.RaftKV
// satisfies the same interface and does fail a write (not leader,
// proposal timeout, lost leadership) without either interface needing to
// change.
func (s *Store) Put(key, value string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
	return nil
}

// Get returns the value for a key and whether it was found. The error
// return exists to satisfy server.KV; see Put for why.
func (s *Store) Get(key string) (string, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, ok := s.data[key]
	return value, ok, nil
}

// Delete removes a key. It is a no-op if the key does not exist. The
// error return exists to satisfy server.KV; see Put for why.
func (s *Store) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, key)
	return nil
}
