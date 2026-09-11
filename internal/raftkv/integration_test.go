package raftkv

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"testing"

	"raftkv/internal/server"
)

// TestEndToEndTCPClientThroughRaft exercises the full stack a real
// client sees: a real TCP connection, the real line protocol
// (internal/server), RaftKV, and Raft consensus (over the in-memory
// transport, for determinism) — not just the Go API in isolation.
func TestEndToEndTCPClientThroughRaft(t *testing.T) {
	nodes, _ := newTestCluster(t, 3)
	startAll(nodes)
	leader := waitForLeader(t, nodes)

	type wired struct {
		node *testNode
		addr string
	}
	var wiredNodes []wired
	for _, n := range nodes {
		srv := server.New(n.kv)
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		go srv.Serve(ln)
		t.Cleanup(srv.Shutdown)
		wiredNodes = append(wiredNodes, wired{node: n, addr: ln.Addr().String()})
	}

	var leaderAddr, followerAddr string
	for _, w := range wiredNodes {
		if w.node == leader {
			leaderAddr = w.addr
		} else if followerAddr == "" {
			followerAddr = w.addr
		}
	}

	leaderConn, err := net.Dial("tcp", leaderAddr)
	if err != nil {
		t.Fatalf("dial leader: %v", err)
	}
	t.Cleanup(func() { leaderConn.Close() })
	leaderReader := bufio.NewReader(leaderConn)

	send := func(conn net.Conn, reader *bufio.Reader, line string) string {
		t.Helper()
		if _, err := fmt.Fprintf(conn, "%s\n", line); err != nil {
			t.Fatalf("write %q: %v", line, err)
		}
		resp, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("read response to %q: %v", line, err)
		}
		return strings.TrimSuffix(resp, "\n")
	}

	if got := send(leaderConn, leaderReader, "PUT foo bar"); got != "OK" {
		t.Fatalf("leader PUT response = %q, want OK", got)
	}
	if got := send(leaderConn, leaderReader, "GET foo"); got != "VALUE bar" {
		t.Fatalf("leader GET response = %q, want VALUE bar", got)
	}

	followerConn, err := net.Dial("tcp", followerAddr)
	if err != nil {
		t.Fatalf("dial follower: %v", err)
	}
	t.Cleanup(func() { followerConn.Close() })
	followerReader := bufio.NewReader(followerConn)

	if got := send(followerConn, followerReader, "PUT foo baz"); !strings.HasPrefix(got, "ERR NOT_LEADER") {
		t.Fatalf("follower PUT response = %q, want an ERR NOT_LEADER response", got)
	}
	if got := send(followerConn, followerReader, "GET foo"); !strings.HasPrefix(got, "ERR NOT_LEADER") {
		t.Fatalf("follower GET response = %q, want an ERR NOT_LEADER response", got)
	}
	if got := send(followerConn, followerReader, "DELETE foo"); !strings.HasPrefix(got, "ERR NOT_LEADER") {
		t.Fatalf("follower DELETE response = %q, want an ERR NOT_LEADER response", got)
	}

	// The follower's rejection must not have touched its own store even
	// though nothing above proved that structurally; confirm via the
	// leader that the value is unaffected by the rejected follower PUT.
	if got := send(leaderConn, leaderReader, "GET foo"); got != "VALUE bar" {
		t.Fatalf("leader GET after rejected follower PUT = %q, want VALUE bar (unchanged)", got)
	}
}
