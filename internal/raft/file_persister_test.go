package raft

import (
	"os"
	"path/filepath"
	"testing"
)

func TestFilePersisterLoadStateOnMissingFileIsEmptyNotError(t *testing.T) {
	dir := t.TempDir()
	p, err := NewFilePersister(filepath.Join(dir, "state.json"))
	if err != nil {
		t.Fatalf("NewFilePersister: %v", err)
	}

	term, votedFor, log, err := p.LoadState()
	if err != nil {
		t.Fatalf("LoadState on a never-written file returned an error: %v", err)
	}
	if term != 0 || votedFor != "" || log != nil {
		t.Fatalf("LoadState = (%d, %q, %v), want zero state", term, votedFor, log)
	}
}

func TestFilePersisterSaveThenLoadRoundTrips(t *testing.T) {
	dir := t.TempDir()
	p, err := NewFilePersister(filepath.Join(dir, "state.json"))
	if err != nil {
		t.Fatalf("NewFilePersister: %v", err)
	}

	wantLog := []LogEntry{
		{Index: 0, Term: 0},
		{Index: 1, Term: 1, Command: []byte("a")},
		{Index: 2, Term: 2, Command: []byte("b")},
	}
	if err := p.SaveState(7, "node-3", wantLog); err != nil {
		t.Fatalf("SaveState: %v", err)
	}

	term, votedFor, log, err := p.LoadState()
	if err != nil {
		t.Fatalf("LoadState: %v", err)
	}
	if term != 7 {
		t.Fatalf("term = %d, want 7", term)
	}
	if votedFor != "node-3" {
		t.Fatalf("votedFor = %q, want %q", votedFor, "node-3")
	}
	if !logsEqual(log, wantLog) {
		t.Fatalf("log = %+v, want %+v", log, wantLog)
	}
}

func TestFilePersisterRepeatedSaveReloadCyclesPreserveIdenticalState(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")

	log := []LogEntry{{Index: 0, Term: 0}}
	for i := 0; i < 5; i++ {
		p, err := NewFilePersister(path)
		if err != nil {
			t.Fatalf("cycle %d: NewFilePersister: %v", i, err)
		}
		term, votedFor, loaded, err := p.LoadState()
		if err != nil {
			t.Fatalf("cycle %d: LoadState: %v", i, err)
		}
		if i > 0 {
			if term != uint64(i) {
				t.Fatalf("cycle %d: term = %d, want %d", i, term, i)
			}
			if votedFor != "candidate" {
				t.Fatalf("cycle %d: votedFor = %q, want %q", i, votedFor, "candidate")
			}
			if !logsEqual(loaded, log) {
				t.Fatalf("cycle %d: log = %+v, want %+v", i, loaded, log)
			}
		}

		log = append(log, LogEntry{Index: uint64(len(log)), Term: uint64(i + 1), Command: []byte{byte(i)}})
		if err := p.SaveState(uint64(i+1), "candidate", log); err != nil {
			t.Fatalf("cycle %d: SaveState: %v", i, err)
		}
	}

	// One final independent load confirms the last cycle's save stuck.
	p, err := NewFilePersister(path)
	if err != nil {
		t.Fatalf("final NewFilePersister: %v", err)
	}
	term, votedFor, loaded, err := p.LoadState()
	if err != nil {
		t.Fatalf("final LoadState: %v", err)
	}
	if term != 5 || votedFor != "candidate" || !logsEqual(loaded, log) {
		t.Fatalf("final state = (%d, %q, %+v), want (5, %q, %+v)", term, votedFor, loaded, "candidate", log)
	}
}

func TestFilePersisterSaveIsAtomicNoTempFilesLeftBehind(t *testing.T) {
	dir := t.TempDir()
	p, err := NewFilePersister(filepath.Join(dir, "state.json"))
	if err != nil {
		t.Fatalf("NewFilePersister: %v", err)
	}

	for i := 0; i < 3; i++ {
		if err := p.SaveState(uint64(i), "x", []LogEntry{{Index: 0, Term: 0}}); err != nil {
			t.Fatalf("SaveState %d: %v", i, err)
		}
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	if len(names) != 1 || names[0] != "state.json" {
		t.Fatalf("directory contains %v, want exactly [\"state.json\"] (no leftover temp files)", names)
	}
}

func TestFilePersisterDetectsInvalidJSON(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	if err := os.WriteFile(path, []byte("{not valid json"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	p, err := NewFilePersister(path)
	if err != nil {
		t.Fatalf("NewFilePersister: %v", err)
	}
	if _, _, _, err := p.LoadState(); err == nil {
		t.Fatal("expected an error loading a file containing invalid JSON")
	}
}

func TestFilePersisterDetectsUnsupportedVersion(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	if err := os.WriteFile(path, []byte(`{"version":999,"current_term":1,"voted_for":"","log":[]}`), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	p, err := NewFilePersister(path)
	if err != nil {
		t.Fatalf("NewFilePersister: %v", err)
	}
	if _, _, _, err := p.LoadState(); err == nil {
		t.Fatal("expected an error loading a file with an unsupported version")
	}
}

func TestFilePersisterDetectsBadSentinel(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	// log[0] must be the zero-value sentinel (index 0, term 0).
	body := `{"version":1,"current_term":1,"voted_for":"","log":[{"Term":1,"Index":0,"Command":null}]}`
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	p, err := NewFilePersister(path)
	if err != nil {
		t.Fatalf("NewFilePersister: %v", err)
	}
	if _, _, _, err := p.LoadState(); err == nil {
		t.Fatal("expected an error loading a file with a non-sentinel entry 0")
	}
}

func TestFilePersisterDetectsIndexGap(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	// entry 1 has Index 2, skipping 1 -- violates log[i].Index == i.
	body := `{"version":1,"current_term":1,"voted_for":"","log":[{"Term":0,"Index":0,"Command":null},{"Term":1,"Index":2,"Command":null}]}`
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	p, err := NewFilePersister(path)
	if err != nil {
		t.Fatalf("NewFilePersister: %v", err)
	}
	if _, _, _, err := p.LoadState(); err == nil {
		t.Fatal("expected an error loading a file with a log index gap")
	}
}

func TestFilePersisterDetectsCurrentTermBelowLogTerm(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	// currentTerm (1) is lower than the last log entry's term (5): a log
	// entry can never have been appended at a term higher than the node
	// believed it was in at the time.
	body := `{"version":1,"current_term":1,"voted_for":"","log":[{"Term":0,"Index":0,"Command":null},{"Term":5,"Index":1,"Command":null}]}`
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	p, err := NewFilePersister(path)
	if err != nil {
		t.Fatalf("NewFilePersister: %v", err)
	}
	if _, _, _, err := p.LoadState(); err == nil {
		t.Fatal("expected an error loading a file where currentTerm is below the log's last term")
	}
}

func TestFilePersisterPartiallyWrittenFileIsDetectedNotSilentlyEmpty(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	// Simulates a crash mid-write that somehow left a truncated file in
	// place directly (not via SaveState, which never does this itself --
	// this tests LoadState's reaction to that external condition).
	if err := os.WriteFile(path, []byte(`{"version":1,"current_term":3,"voted_for":"x","lo`), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	p, err := NewFilePersister(path)
	if err != nil {
		t.Fatalf("NewFilePersister: %v", err)
	}
	term, votedFor, log, err := p.LoadState()
	if err == nil {
		t.Fatalf("expected an error loading a truncated file, got state (%d, %q, %v) with no error", term, votedFor, log)
	}
}
