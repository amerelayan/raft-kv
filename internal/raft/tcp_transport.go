package raft

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"time"
)

type rpcKind string

const (
	kindRequestVote   rpcKind = "request_vote"
	kindAppendEntries rpcKind = "append_entries"
)

type rpcEnvelope struct {
	Kind          rpcKind            `json:"kind"`
	RequestVote   *RequestVoteArgs   `json:"request_vote,omitempty"`
	AppendEntries *AppendEntriesArgs `json:"append_entries,omitempty"`
}

type rpcResult struct {
	RequestVote   *RequestVoteReply   `json:"request_vote,omitempty"`
	AppendEntries *AppendEntriesReply `json:"append_entries,omitempty"`
}

// TCPTransport is a Transport, and the corresponding RPC server, that
// sends Raft RPCs over TCP connections entirely separate from the
// client-facing key-value protocol served by internal/server: a
// TCPTransport listens on its own address, never the client port.
//
// Each call dials a short-lived connection, sends one JSON request, reads
// one JSON response, and closes it. That's adequate at Raft's RPC volume
// (heartbeats every tens of milliseconds) and keeps the implementation
// free of persistent-connection bookkeeping.
type TCPTransport struct {
	self  string
	addrs map[string]string

	// rpcTimeout bounds how long an accepted connection has to deliver a
	// complete request and read its response, matching defaultRPCTimeout
	// so a stalled peer (crashed mid-write, black-holed connection) can
	// never block a server goroutine indefinitely.
	rpcTimeout time.Duration

	handlerMu sync.RWMutex
	handler   RPCHandler

	mu       sync.Mutex
	listener net.Listener
	conns    map[net.Conn]struct{}
	closed   bool
	wg       sync.WaitGroup
}

// NewTCPTransport creates a transport for node self. addrs must map every
// participating node ID (including self) to its Raft RPC TCP address.
func NewTCPTransport(self string, addrs map[string]string) *TCPTransport {
	a := make(map[string]string, len(addrs))
	for id, addr := range addrs {
		a[id] = addr
	}
	return &TCPTransport{
		self:       self,
		addrs:      a,
		rpcTimeout: defaultRPCTimeout,
		conns:      make(map[net.Conn]struct{}),
	}
}

// SetHandler wires the RPCHandler (normally the local Node) that inbound
// RPCs are dispatched to. Call it before Serve.
func (t *TCPTransport) SetHandler(h RPCHandler) {
	t.handlerMu.Lock()
	defer t.handlerMu.Unlock()
	t.handler = h
}

// Serve accepts Raft RPC connections on ln until Shutdown is called.
func (t *TCPTransport) Serve(ln net.Listener) error {
	t.mu.Lock()
	if t.closed {
		// Shutdown ran before Serve started (e.g. Serve was launched in a
		// goroutine and Shutdown was called immediately after). ln was
		// never handed to Shutdown, so nothing has closed it yet — do
		// that here, and report the same nil-means-shutdown result Serve
		// would give if Shutdown had raced in after the loop started.
		t.mu.Unlock()
		ln.Close()
		return nil
	}
	t.listener = ln
	t.mu.Unlock()

	defer ln.Close()

	for {
		conn, err := ln.Accept()
		if err != nil {
			t.mu.Lock()
			closed := t.closed
			t.mu.Unlock()
			if closed {
				return nil
			}
			return err
		}

		t.mu.Lock()
		t.conns[conn] = struct{}{}
		t.mu.Unlock()

		t.wg.Add(1)
		go func() {
			defer t.wg.Done()
			defer t.forgetConn(conn)
			t.serveConn(conn)
		}()
	}
}

func (t *TCPTransport) forgetConn(conn net.Conn) {
	t.mu.Lock()
	delete(t.conns, conn)
	t.mu.Unlock()
}

func (t *TCPTransport) serveConn(conn net.Conn) {
	defer conn.Close()

	// Bound the whole request/response exchange: without this, a peer
	// that opens a connection and then stalls (crashes mid-write, or is
	// cut off by a network fault) leaves this goroutine blocked in
	// Decode forever.
	if err := conn.SetDeadline(time.Now().Add(t.rpcTimeout)); err != nil {
		return
	}

	var req rpcEnvelope
	if err := json.NewDecoder(conn).Decode(&req); err != nil {
		return
	}

	t.handlerMu.RLock()
	h := t.handler
	t.handlerMu.RUnlock()
	if h == nil {
		return
	}

	var resp rpcResult
	switch req.Kind {
	case kindRequestVote:
		if req.RequestVote == nil {
			return
		}
		resp.RequestVote = h.HandleRequestVote(req.RequestVote)
	case kindAppendEntries:
		if req.AppendEntries == nil {
			return
		}
		resp.AppendEntries = h.HandleAppendEntries(req.AppendEntries)
	default:
		return
	}

	_ = json.NewEncoder(conn).Encode(resp)
}

// Shutdown stops accepting connections, closes every currently open one,
// and waits for their handler goroutines to exit. Safe to call more than
// once.
func (t *TCPTransport) Shutdown() {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return
	}
	t.closed = true
	if t.listener != nil {
		t.listener.Close()
	}
	conns := make([]net.Conn, 0, len(t.conns))
	for c := range t.conns {
		conns = append(conns, c)
	}
	t.mu.Unlock()

	for _, c := range conns {
		c.Close()
	}
	t.wg.Wait()
}

func (t *TCPTransport) RequestVote(ctx context.Context, target string, args *RequestVoteArgs) (*RequestVoteReply, error) {
	var resp rpcResult
	if err := t.call(ctx, target, rpcEnvelope{Kind: kindRequestVote, RequestVote: args}, &resp); err != nil {
		return nil, err
	}
	if resp.RequestVote == nil {
		return nil, fmt.Errorf("raft: empty RequestVote response from %s", target)
	}
	return resp.RequestVote, nil
}

func (t *TCPTransport) AppendEntries(ctx context.Context, target string, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	var resp rpcResult
	if err := t.call(ctx, target, rpcEnvelope{Kind: kindAppendEntries, AppendEntries: args}, &resp); err != nil {
		return nil, err
	}
	if resp.AppendEntries == nil {
		return nil, fmt.Errorf("raft: empty AppendEntries response from %s", target)
	}
	return resp.AppendEntries, nil
}

func (t *TCPTransport) call(ctx context.Context, target string, req rpcEnvelope, resp *rpcResult) error {
	addr, ok := t.addrs[target]
	if !ok {
		return fmt.Errorf("raft: unknown peer %q", target)
	}

	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return err
	}
	defer conn.Close()

	if dl, ok := ctx.Deadline(); ok {
		conn.SetDeadline(dl)
	}

	if err := json.NewEncoder(conn).Encode(req); err != nil {
		return err
	}
	return json.NewDecoder(conn).Decode(resp)
}
