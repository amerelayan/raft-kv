package raft

import (
	"context"
	"fmt"
	"sync"
)

// Network is an in-memory message fabric connecting FakeTransports. It
// exists to make election logic deterministically testable: RPCs are
// delivered by a direct function call rather than a real socket, so there
// is no OS scheduling jitter or network latency to account for, and
// individual nodes can be marked unreachable to simulate a crash or a
// network partition without any real teardown.
type Network struct {
	mu          sync.Mutex
	handlers    map[string]RPCHandler
	unreachable map[string]bool
}

// NewNetwork creates an empty Network.
func NewNetwork() *Network {
	return &Network{
		handlers:    make(map[string]RPCHandler),
		unreachable: make(map[string]bool),
	}
}

// Register makes h reachable under id. Call it once per node, after the
// node (which implements RPCHandler) has been constructed.
func (net *Network) Register(id string, h RPCHandler) {
	net.mu.Lock()
	defer net.mu.Unlock()
	net.handlers[id] = h
}

// SetUnreachable simulates id being down or partitioned: while true, no
// RPC to or from id is delivered in either direction.
func (net *Network) SetUnreachable(id string, unreachable bool) {
	net.mu.Lock()
	defer net.mu.Unlock()
	net.unreachable[id] = unreachable
}

func (net *Network) deliverable(from, to string) (RPCHandler, bool) {
	net.mu.Lock()
	defer net.mu.Unlock()
	if net.unreachable[from] || net.unreachable[to] {
		return nil, false
	}
	h, ok := net.handlers[to]
	return h, ok
}

// FakeTransport is a Transport backed by a Network. Every Node in a test
// cluster gets its own FakeTransport sharing the same Network.
type FakeTransport struct {
	network *Network
	self    string
}

// NewFakeTransport creates a FakeTransport for node self on network.
func NewFakeTransport(network *Network, self string) *FakeTransport {
	return &FakeTransport{network: network, self: self}
}

func (t *FakeTransport) RequestVote(ctx context.Context, target string, args *RequestVoteArgs) (*RequestVoteReply, error) {
	h, ok := t.network.deliverable(t.self, target)
	if !ok {
		return nil, fmt.Errorf("raft: %s unreachable from %s", target, t.self)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return h.HandleRequestVote(args), nil
}

func (t *FakeTransport) AppendEntries(ctx context.Context, target string, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	h, ok := t.network.deliverable(t.self, target)
	if !ok {
		return nil, fmt.Errorf("raft: %s unreachable from %s", target, t.self)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return h.HandleAppendEntries(args), nil
}
