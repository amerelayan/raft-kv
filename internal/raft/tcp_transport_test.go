package raft

import (
	"context"
	"net"
	"testing"
	"time"
)

// stubHandler is a minimal RPCHandler for exercising TCPTransport without
// a full Node.
type stubHandler struct {
	voteReply   *RequestVoteReply
	appendReply *AppendEntriesReply
}

func (s *stubHandler) HandleRequestVote(args *RequestVoteArgs) *RequestVoteReply {
	return s.voteReply
}

func (s *stubHandler) HandleAppendEntries(args *AppendEntriesArgs) *AppendEntriesReply {
	return s.appendReply
}

func TestTCPTransportRoundTrip(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()

	addrs := map[string]string{"server": addr}
	serverTransport := NewTCPTransport("server", addrs)
	serverTransport.SetHandler(&stubHandler{
		voteReply:   &RequestVoteReply{Term: 7, VoteGranted: true},
		appendReply: &AppendEntriesReply{Term: 7, Success: true},
	})

	done := make(chan error, 1)
	go func() { done <- serverTransport.Serve(ln) }()
	t.Cleanup(func() {
		serverTransport.Shutdown()
		<-done
	})

	clientTransport := NewTCPTransport("client", addrs)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	voteReply, err := clientTransport.RequestVote(ctx, "server", &RequestVoteArgs{Term: 7, CandidateID: "client"})
	if err != nil {
		t.Fatalf("RequestVote: %v", err)
	}
	if !voteReply.VoteGranted || voteReply.Term != 7 {
		t.Fatalf("got %+v", voteReply)
	}

	appendReply, err := clientTransport.AppendEntries(ctx, "server", &AppendEntriesArgs{Term: 7, LeaderID: "server"})
	if err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}
	if !appendReply.Success || appendReply.Term != 7 {
		t.Fatalf("got %+v", appendReply)
	}
}

func TestTCPTransportUnknownPeer(t *testing.T) {
	transport := NewTCPTransport("client", map[string]string{})
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	if _, err := transport.RequestVote(ctx, "nobody", &RequestVoteArgs{}); err == nil {
		t.Fatal("expected error for unknown peer")
	}
}

func TestTCPTransportShutdownStopsAcceptingConnections(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()

	transport := NewTCPTransport("server", map[string]string{"server": addr})
	transport.SetHandler(&stubHandler{
		voteReply:   &RequestVoteReply{},
		appendReply: &AppendEntriesReply{},
	})

	done := make(chan error, 1)
	go func() { done <- transport.Serve(ln) }()

	transport.Shutdown()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Serve returned error after Shutdown: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not return after Shutdown")
	}

	if _, err := net.Dial("tcp", addr); err == nil {
		t.Fatal("expected dial to fail after Shutdown")
	}
}
