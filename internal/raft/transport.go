package raft

import "context"

// Transport lets a Node send RPCs to a peer, identified by peer ID (never
// an address — resolving IDs to addresses, or to something else entirely
// like an in-memory registry, is the transport implementation's job).
type Transport interface {
	RequestVote(ctx context.Context, target string, args *RequestVoteArgs) (*RequestVoteReply, error)
	AppendEntries(ctx context.Context, target string, args *AppendEntriesArgs) (*AppendEntriesReply, error)
}

// RPCHandler processes inbound RPCs. *Node implements it; a transport's
// server side dispatches incoming requests to a registered RPCHandler.
type RPCHandler interface {
	HandleRequestVote(args *RequestVoteArgs) *RequestVoteReply
	HandleAppendEntries(args *AppendEntriesArgs) *AppendEntriesReply
}
