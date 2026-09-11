package raft

// Persister lets a Node's durable state survive a restart. SaveState is
// called every time currentTerm, votedFor, or the log changes; LoadState
// is called once, when a Node is constructed.
type Persister interface {
	SaveState(term uint64, votedFor string, log []LogEntry) error
	LoadState() (term uint64, votedFor string, log []LogEntry, err error)
}

// NoopPersister discards state: a Node using it always starts at term 0,
// with no vote and an empty log, even across restarts. This stage does
// not implement durable persistence — NoopPersister is the default
// specifically so that a real (e.g. file-based) Persister can be dropped
// into Config later without any change to Node.
type NoopPersister struct{}

func (NoopPersister) SaveState(uint64, string, []LogEntry) error { return nil }
func (NoopPersister) LoadState() (uint64, string, []LogEntry, error) {
	return 0, "", nil, nil
}
