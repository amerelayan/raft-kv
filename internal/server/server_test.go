package server

import (
	"bufio"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"raftkv/internal/store"
)

// startTestServer starts a Server on an ephemeral loopback port and
// arranges for it to be shut down when the test completes.
func startTestServer(t *testing.T) (addr string, srv *Server) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	srv = New(store.New())

	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := srv.Serve(ln); err != nil {
			t.Errorf("Serve returned error: %v", err)
		}
	}()

	t.Cleanup(func() {
		srv.Shutdown()
		<-done
	})

	return ln.Addr().String(), srv
}

// testClient is a small helper for sending one command and reading one
// response line over a real TCP connection.
type testClient struct {
	t    *testing.T
	conn net.Conn
	r    *bufio.Reader
}

func dialTestClient(t *testing.T, addr string) *testClient {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return &testClient{t: t, conn: conn, r: bufio.NewReader(conn)}
}

func (c *testClient) send(line string) string {
	c.t.Helper()
	if _, err := fmt.Fprintf(c.conn, "%s\n", line); err != nil {
		c.t.Fatalf("write: %v", err)
	}
	resp, err := c.r.ReadString('\n')
	if err != nil {
		c.t.Fatalf("read: %v", err)
	}
	return resp[:len(resp)-1] // strip trailing \n
}

func TestPutGetDelete(t *testing.T) {
	addr, _ := startTestServer(t)
	c := dialTestClient(t, addr)

	if got := c.send("PUT foo bar"); got != "OK" {
		t.Fatalf("PUT: got %q", got)
	}
	if got := c.send("GET foo"); got != "VALUE bar" {
		t.Fatalf("GET: got %q", got)
	}
	if got := c.send("DELETE foo"); got != "OK" {
		t.Fatalf("DELETE: got %q", got)
	}
	if got := c.send("GET foo"); got != "NOT_FOUND" {
		t.Fatalf("GET after DELETE: got %q", got)
	}
}

func TestGetMissingKey(t *testing.T) {
	addr, _ := startTestServer(t)
	c := dialTestClient(t, addr)

	if got := c.send("GET missing"); got != "NOT_FOUND" {
		t.Fatalf("got %q", got)
	}
}

func TestPutOverwrite(t *testing.T) {
	addr, _ := startTestServer(t)
	c := dialTestClient(t, addr)

	c.send("PUT foo bar")
	c.send("PUT foo baz")
	if got := c.send("GET foo"); got != "VALUE baz" {
		t.Fatalf("got %q", got)
	}
}

func TestPutValueWithSpaces(t *testing.T) {
	addr, _ := startTestServer(t)
	c := dialTestClient(t, addr)

	c.send("PUT greeting hello there world")
	if got := c.send("GET greeting"); got != "VALUE hello there world" {
		t.Fatalf("got %q", got)
	}
}

func TestMalformedCommandDoesNotCrashConnection(t *testing.T) {
	addr, _ := startTestServer(t)
	c := dialTestClient(t, addr)

	if got := c.send("NOTACOMMAND"); got[:3] != "ERR" {
		t.Fatalf("expected ERR response, got %q", got)
	}
	if got := c.send("GET"); got[:3] != "ERR" {
		t.Fatalf("expected ERR response, got %q", got)
	}

	// The connection, and the server, must still work afterward.
	if got := c.send("PUT foo bar"); got != "OK" {
		t.Fatalf("PUT after malformed command: got %q", got)
	}
	if got := c.send("GET foo"); got != "VALUE bar" {
		t.Fatalf("GET after malformed command: got %q", got)
	}
}

func TestMalformedCommandDoesNotCrashServer(t *testing.T) {
	addr, _ := startTestServer(t)

	bad := dialTestClient(t, addr)
	bad.send("garbage input !!!")

	// A second, independent client must still be served correctly.
	good := dialTestClient(t, addr)
	if got := good.send("PUT foo bar"); got != "OK" {
		t.Fatalf("got %q", got)
	}
}

func TestConcurrentClients(t *testing.T) {
	addr, _ := startTestServer(t)

	const numClients = 20
	var wg sync.WaitGroup
	wg.Add(numClients)

	for i := 0; i < numClients; i++ {
		go func(i int) {
			defer wg.Done()
			c := dialTestClient(t, addr)
			key := fmt.Sprintf("key-%d", i)
			value := fmt.Sprintf("value-%d", i)

			if got := c.send(fmt.Sprintf("PUT %s %s", key, value)); got != "OK" {
				t.Errorf("client %d PUT: got %q", i, got)
				return
			}
			want := "VALUE " + value
			if got := c.send(fmt.Sprintf("GET %s", key)); got != want {
				t.Errorf("client %d GET: got %q, want %q", i, got, want)
				return
			}
			if got := c.send(fmt.Sprintf("DELETE %s", key)); got != "OK" {
				t.Errorf("client %d DELETE: got %q", i, got)
			}
		}(i)
	}

	wg.Wait()
}

func TestShutdownStopsAcceptingConnections(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()

	srv := New(store.New())
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ln) }()

	// Make sure the server is actually accepting before shutting down.
	c := dialTestClient(t, addr)
	if got := c.send("PUT foo bar"); got != "OK" {
		t.Fatalf("got %q", got)
	}

	srv.Shutdown()

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

	// Calling Shutdown again must not panic or block.
	srv.Shutdown()
}
