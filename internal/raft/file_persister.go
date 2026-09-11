package raft

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// persistedStateVersion identifies the on-disk schema written by
// FilePersister. A file whose version doesn't match is reported as an
// error rather than silently misinterpreted — this is the seam that
// lets the format evolve later.
const persistedStateVersion = 1

// persistedState is the on-disk representation of a Node's durable Raft
// state. It deliberately contains nothing but what Raft requires to be
// durable: currentTerm, votedFor, and the full log. LogEntry's fields
// have no explicit json tags, so they round-trip under their Go names
// ("Term", "Index", "Command") — Command ([]byte) is base64-encoded by
// encoding/json, keeping the rest of the file plain, inspectable text.
type persistedState struct {
	Version     int        `json:"version"`
	CurrentTerm uint64     `json:"current_term"`
	VotedFor    string     `json:"voted_for"`
	Log         []LogEntry `json:"log"`
}

// FilePersister is a Persister backed by a single JSON file, written
// atomically (temp file + fsync + rename). See SaveState and LoadState
// for the exact durability guarantees and validation performed.
type FilePersister struct {
	path string
	mu   sync.Mutex
}

// NewFilePersister creates a FilePersister that reads from and writes to
// path. path's parent directory is created if it doesn't already exist,
// since a node's data directory won't exist on its very first run.
func NewFilePersister(path string) (*FilePersister, error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("raft: create data directory %s: %w", dir, err)
	}
	return &FilePersister{path: path}, nil
}

// SaveState atomically replaces the state file with term, votedFor, and
// log. Three distinct guarantees combine to make a successful (nil
// error) return mean the new state is fully durable:
//
//  1. temp-file fsync: the new content is written to a temp file in the
//     same directory and fsynced before anything else happens, so the
//     bytes themselves are on stable storage.
//  2. atomic rename: only then is the temp file renamed over the
//     target. POSIX rename(2) is atomic with respect to any reader —
//     including a concurrent crash-and-restart — so the target can only
//     ever be observed as the complete old file or the complete new
//     one, never a torn mix of both.
//  3. directory fsync: the rename itself is a change to the containing
//     directory's own metadata, which is a separate durability domain
//     from the file's bytes. Renaming without also fsyncing the
//     directory means a crash immediately after could, on some
//     filesystems, forget the rename happened at all on the next mount
//     (reverting to the previous — still fully valid — file, not
//     corruption, but not the update the caller was just told
//     succeeded). So the directory is fsynced too, and — unlike an
//     earlier version of this method — a failure here is a real error,
//     not silently ignored: SaveState must not report success unless
//     all three of these actually held.
func (p *FilePersister) SaveState(term uint64, votedFor string, log []LogEntry) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	state := persistedState{
		Version:     persistedStateVersion,
		CurrentTerm: term,
		VotedFor:    votedFor,
		Log:         log,
	}

	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("raft: marshal state: %w", err)
	}
	data = append(data, '\n')

	dir := filepath.Dir(p.path)
	tmp, err := os.CreateTemp(dir, ".raft-state-*.tmp")
	if err != nil {
		return fmt.Errorf("raft: create temp state file: %w", err)
	}
	tmpPath := tmp.Name()
	renamed := false
	defer func() {
		if !renamed {
			os.Remove(tmpPath)
		}
	}()

	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return fmt.Errorf("raft: write temp state file: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("raft: sync temp state file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("raft: close temp state file: %w", err)
	}

	if err := os.Rename(tmpPath, p.path); err != nil {
		return fmt.Errorf("raft: rename temp state file into place: %w", err)
	}
	renamed = true

	// The file content is durably saved and correctly in place at this
	// point regardless of what happens below — but the rename's own
	// durability (guarantee 3 above) isn't confirmed until the
	// directory fsync succeeds, so a failure here is real: SaveState
	// must not claim full durability it didn't actually confirm.
	dirFile, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("raft: open directory %s to fsync the rename: %w", dir, err)
	}
	syncErr := dirFile.Sync()
	closeErr := dirFile.Close()
	if syncErr != nil {
		return fmt.Errorf("raft: sync directory %s to durably record the rename: %w", dir, syncErr)
	}
	if closeErr != nil {
		return fmt.Errorf("raft: close directory %s after fsync: %w", dir, closeErr)
	}

	return nil
}

// LoadState reads and validates the state file. A missing file is not
// an error — it's the normal first-run case, and returns the zero state
// exactly like NoopPersister. Anything else wrong (invalid JSON, an
// unrecognized version, or a structurally invalid log) is reported as an
// error rather than silently discarded: the caller (Node construction)
// must decide what to do about corrupted durable state, not have it
// quietly replaced with an empty one.
func (p *FilePersister) LoadState() (term uint64, votedFor string, log []LogEntry, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	data, err := os.ReadFile(p.path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, "", nil, nil
	}
	if err != nil {
		return 0, "", nil, fmt.Errorf("raft: read state file %s: %w", p.path, err)
	}

	var state persistedState
	if err := json.Unmarshal(data, &state); err != nil {
		return 0, "", nil, fmt.Errorf("raft: state file %s is corrupted (invalid JSON): %w", p.path, err)
	}

	if state.Version != persistedStateVersion {
		return 0, "", nil, fmt.Errorf("raft: state file %s has unsupported version %d (want %d)", p.path, state.Version, persistedStateVersion)
	}

	if err := validatePersistedState(state); err != nil {
		return 0, "", nil, fmt.Errorf("raft: state file %s is invalid: %w", p.path, err)
	}

	return state.CurrentTerm, state.VotedFor, state.Log, nil
}

// validatePersistedState checks the structural invariants Node relies
// on: log[i].Index == i for every i, log[0] (if present) is the
// zero-value sentinel (index 0, term 0), terms never decrease along the
// log, and currentTerm is never lower than the log's last entry's term
// (a log entry can only ever have been appended at or below the term
// the node believed it was in at the time). An empty log is valid — a
// freshly-initialized node's persisted state, before its first RPC.
func validatePersistedState(state persistedState) error {
	log := state.Log
	if len(log) == 0 {
		return nil
	}
	if log[0].Index != 0 || log[0].Term != 0 {
		return fmt.Errorf("entry 0 must be the zero-value sentinel (index 0, term 0), got index %d term %d", log[0].Index, log[0].Term)
	}
	var lastTerm uint64
	for i, entry := range log {
		if entry.Index != uint64(i) {
			return fmt.Errorf("entry %d has index %d, want %d", i, entry.Index, i)
		}
		if entry.Term < lastTerm {
			return fmt.Errorf("entry %d has term %d, lower than the previous entry's term %d", i, entry.Term, lastTerm)
		}
		lastTerm = entry.Term
	}
	if lastTerm > state.CurrentTerm {
		return fmt.Errorf("currentTerm %d is lower than the last log entry's term %d", state.CurrentTerm, lastTerm)
	}
	return nil
}
