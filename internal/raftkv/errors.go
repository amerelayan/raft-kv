package raftkv

import "errors"

// ErrTimeout is returned when a proposal does not commit and apply
// within the configured proposal timeout.
var ErrTimeout = errors.New("TIMEOUT")

// errStopped is returned internally when the RaftKV has been (or is
// concurrently being) stopped while a proposal was pending or starting.
var errStopped = errors.New("STOPPED")

// NotLeaderError is returned when a write or read is attempted on a node
// that does not currently believe itself to be the Raft leader.
type NotLeaderError struct {
	// LeaderID is the last known leader, best-effort — empty if not
	// currently known. Exposed for observability only; this package does
	// not implement automatic client redirection.
	LeaderID string
}

func (e *NotLeaderError) Error() string {
	if e.LeaderID == "" {
		return "NOT_LEADER"
	}
	return "NOT_LEADER leader=" + e.LeaderID
}

// LeadershipLostError is returned when a proposal's log index was
// eventually applied, but with a different term's entry: this node's
// leadership changed before its own proposal committed, and a different
// entry — from whichever node became the new leader — occupies that
// index instead.
type LeadershipLostError struct {
	LeaderID string
}

func (e *LeadershipLostError) Error() string {
	if e.LeaderID == "" {
		return "LEADERSHIP_LOST"
	}
	return "LEADERSHIP_LOST leader=" + e.LeaderID
}
