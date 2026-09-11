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

// LogEntry is one entry in a Node's replicated log.
type LogEntry struct {
	Term    uint64
	Index   uint64
	Command []byte
}

// RequestVoteArgs is the RequestVote RPC request.
type RequestVoteArgs struct {
	Term        uint64
	CandidateID string

	// LastLogIndex and LastLogTerm implement the Raft paper §5.4.1
	// election restriction: a candidate may only receive a vote if its
	// log is at least as up to date as the voter's own log.
	LastLogIndex uint64
	LastLogTerm  uint64
}

// RequestVoteReply is the RequestVote RPC response.
type RequestVoteReply struct {
	Term        uint64
	VoteGranted bool
}

// AppendEntriesArgs is the AppendEntries RPC request. Entries is empty
// for a pure heartbeat and non-empty when the leader is replicating log
// entries; both cases go through the same consistency check.
type AppendEntriesArgs struct {
	Term     uint64
	LeaderID string

	// PrevLogIndex/PrevLogTerm identify the entry immediately before
	// Entries in the leader's log, for the receiver's consistency check.
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []LogEntry
	// LeaderCommit is the leader's commitIndex, letting the follower
	// advance its own.
	LeaderCommit uint64
}

// AppendEntriesReply is the AppendEntries RPC response.
type AppendEntriesReply struct {
	Term    uint64
	Success bool
}

// AppliedEntry is delivered on a Node's apply channel once its command's
// index is known to be committed by a majority and has been applied, in
// order, after every lower index. A future state machine (e.g. the KV
// store) will read this channel to apply commands; nothing reads it yet.
type AppliedEntry struct {
	Index   uint64
	Term    uint64
	Command []byte
}

// State is a point-in-time snapshot of a Node, for observability and
// tests.
type State struct {
	ID          string
	Term        uint64
	Role        Role
	LeaderID    string
	VotedFor    string
	CommitIndex uint64
	LastApplied uint64
}
