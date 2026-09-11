package raft

// Role is a Raft node's current position in the state machine.
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// LogEntry is a placeholder for Stage 4 (log replication). It is defined
// now, alongside the RPC fields that reference it, so the AppendEntries
// wire format does not need to change when log replication is added.
// This stage never populates a LogEntry or sends one over the wire.
type LogEntry struct {
	Term    uint64
	Index   uint64
	Command []byte
}

// RequestVoteArgs is the RequestVote RPC request.
type RequestVoteArgs struct {
	Term        uint64
	CandidateID string

	// LastLogIndex and LastLogTerm are reserved for Stage 4's election
	// restriction (a candidate must have a log at least as up to date as
	// the voter's log, per the Raft paper §5.4.1). Unused and always
	// zero until log replication exists.
	LastLogIndex uint64
	LastLogTerm  uint64
}

// RequestVoteReply is the RequestVote RPC response.
type RequestVoteReply struct {
	Term        uint64
	VoteGranted bool
}

// AppendEntriesArgs is the AppendEntries RPC request. This stage only
// ever sends the heartbeat form of this RPC: Entries is always empty and
// the log-related fields are always zero.
type AppendEntriesArgs struct {
	Term     uint64
	LeaderID string

	// PrevLogIndex, PrevLogTerm, Entries, and LeaderCommit are reserved
	// for Stage 4 log replication.
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []LogEntry
	LeaderCommit uint64
}

// AppendEntriesReply is the AppendEntries RPC response.
type AppendEntriesReply struct {
	Term    uint64
	Success bool
}

// State is a point-in-time snapshot of a Node, for observability and
// tests.
type State struct {
	ID       string
	Term     uint64
	Role     Role
	LeaderID string
	VotedFor string
}
