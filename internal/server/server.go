// Package server exposes a key-value store over a simple line-based TCP
// protocol. It depends only on the small KV interface below, not on any
// concrete storage implementation, so the storage backing a Server can be
// swapped later (for example, for a Raft-replicated store) without any
// change to the networking code.
package server

import (
	"bufio"
	"net"
	"sync"
)

// KV is the storage interface the server needs. *store.Store satisfies it.
//
// Every method returns an error so that a future Raft-backed
// implementation can report a failed write or read (not leader, proposal
// timeout, no quorum) without changing this interface: *store.Store always
// returns a nil error today, but the networking layer already handles a
// non-nil one.
type KV interface {
	Put(key, value string) error
	Get(key string) (value string, ok bool, err error)
	Delete(key string) error
}

// Server accepts TCP connections and serves the line-based KV protocol.
type Server struct {
	kv KV

	mu       sync.Mutex
	listener net.Listener
	conns    map[net.Conn]struct{}
	closed   bool

	wg sync.WaitGroup
}

// New creates a Server backed by kv. Call Serve to start accepting
// connections.
func New(kv KV) *Server {
	return &Server{
		kv:    kv,
		conns: make(map[net.Conn]struct{}),
	}
}

// Serve accepts connections on ln until the listener errors or Shutdown is
// called. It blocks until the accept loop exits, and always closes ln
// before returning. A nil error means Shutdown caused the exit.
func (s *Server) Serve(ln net.Listener) error {
	s.mu.Lock()
	if s.closed {
		// Shutdown ran before Serve started (e.g. Serve was launched in a
		// goroutine and Shutdown was called immediately after). ln was
		// never handed to Shutdown, so nothing has closed it yet — do
		// that here, and report the same nil-means-shutdown result Serve
		// would give if Shutdown had raced in after the loop started.
		s.mu.Unlock()
		ln.Close()
		return nil
	}
	s.listener = ln
	s.mu.Unlock()

	defer ln.Close()

	for {
		conn, err := ln.Accept()
		if err != nil {
			s.mu.Lock()
			closed := s.closed
			s.mu.Unlock()
			if closed {
				return nil
			}
			return err
		}

		s.mu.Lock()
		s.conns[conn] = struct{}{}
		s.mu.Unlock()

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer s.forgetConn(conn)
			handleConn(conn, s.kv)
		}()
	}
}

// Shutdown stops the server: it closes the listener so no new connections
// are accepted, closes every currently open connection to unblock any
// goroutine waiting on a read, and then waits for all connection handler
// goroutines to exit. It is safe to call more than once.
func (s *Server) Shutdown() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	if s.listener != nil {
		s.listener.Close()
	}
	conns := make([]net.Conn, 0, len(s.conns))
	for conn := range s.conns {
		conns = append(conns, conn)
	}
	s.mu.Unlock()

	for _, conn := range conns {
		conn.Close()
	}

	s.wg.Wait()
}

func (s *Server) forgetConn(conn net.Conn) {
	s.mu.Lock()
	delete(s.conns, conn)
	s.mu.Unlock()
}

// handleConn serves the line-based protocol on a single connection until
// the client disconnects or the connection is closed by Shutdown.
func handleConn(conn net.Conn, kv KV) {
	defer conn.Close()

	scanner := bufio.NewScanner(conn)
	writer := bufio.NewWriter(conn)

	for scanner.Scan() {
		cmd, err := parseCommand(scanner.Text())
		if err != nil {
			writeLine(writer, "ERR "+err.Error())
			continue
		}

		switch cmd.name {
		case "PUT":
			if err := kv.Put(cmd.key, cmd.value); err != nil {
				writeLine(writer, "ERR "+err.Error())
				continue
			}
			writeLine(writer, "OK")
		case "GET":
			value, ok, err := kv.Get(cmd.key)
			if err != nil {
				writeLine(writer, "ERR "+err.Error())
				continue
			}
			if !ok {
				writeLine(writer, "NOT_FOUND")
				continue
			}
			writeLine(writer, "VALUE "+value)
		case "DELETE":
			if err := kv.Delete(cmd.key); err != nil {
				writeLine(writer, "ERR "+err.Error())
				continue
			}
			writeLine(writer, "OK")
		}
	}
}

func writeLine(w *bufio.Writer, line string) {
	w.WriteString(line)
	w.WriteByte('\n')
	w.Flush()
}
