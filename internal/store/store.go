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

// Put sets the value for a key, creating or overwriting it.
func (s *Store) Put(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
}

// Get returns the value for a key and whether it was found.
func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, ok := s.data[key]
	return value, ok
}

// Delete removes a key. It is a no-op if the key does not exist.
func (s *Store) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, key)
}
