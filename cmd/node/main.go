// Command node starts a single key-value store node listening for client
// connections over TCP, optionally participating in Raft consensus with
// other nodes over a separate TCP port. When Raft is enabled, client
// writes are replicated via Raft before being acknowledged; when it
// isn't, the node behaves exactly as it did in Stage 1/2 (a standalone,
// in-memory, single-node store).
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"raftkv/internal/raft"
	"raftkv/internal/raftkv"
	"raftkv/internal/server"
	"raftkv/internal/store"
)

func main() {
	addr := flag.String("addr", ":9000", "TCP address to listen on for client (KV) connections")
	id := flag.String("id", "", "Raft node ID; leave empty to run without Raft (Stage 1/2 behavior)")
	raftAddr := flag.String("raft-addr", ":9100", "TCP address to listen on for Raft RPCs")
	peers := flag.String("peers", "", "comma-separated peer list as id=raft-addr, e.g. node2=localhost:9101,node3=localhost:9102")
	flag.Parse()

	localStore := store.New()

	var kv server.KV = localStore
	var raftNode *raft.Node
	var raftTransport *raft.TCPTransport
	var raftKV *raftkv.RaftKV
	var raftServeErr chan error

	if *id != "" {
		peerAddrs, err := parsePeers(*peers)
		if err != nil {
			log.Fatalf("peers: %v", err)
		}

		raftLn, err := net.Listen("tcp", *raftAddr)
		if err != nil {
			log.Fatalf("listen on %s: %v", *raftAddr, err)
		}

		addrs := make(map[string]string, len(peerAddrs)+1)
		peerIDs := make([]string, 0, len(peerAddrs))
		for peerID, peerAddr := range peerAddrs {
			addrs[peerID] = peerAddr
			peerIDs = append(peerIDs, peerID)
		}
		addrs[*id] = raftLn.Addr().String()

		raftTransport = raft.NewTCPTransport(*id, addrs)
		raftNode = raft.NewNode(raft.Config{
			ID:        *id,
			Peers:     peerIDs,
			Transport: raftTransport,
		})
		raftTransport.SetHandler(raftNode)

		raftServeErr = make(chan error, 1)
		go func() {
			raftServeErr <- raftTransport.Serve(raftLn)
		}()
		log.Printf("raft node %q listening for peers on %s (peers: %v)", *id, raftLn.Addr(), peerIDs)

		raftKV = raftkv.New(raftkv.Config{Node: raftNode, Store: localStore})
		kv = raftKV

		raftNode.Start()
		raftKV.Start()
		go logRaftStateChanges(raftNode)
	}

	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("listen on %s: %v", *addr, err)
	}

	srv := server.New(kv)
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- srv.Serve(ln)
	}()
	log.Printf("node listening for clients on %s", ln.Addr())

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	select {
	case <-ctx.Done():
		log.Println("shutting down...")
		// RaftKV first, so any in-flight client write unblocks
		// immediately via its own stop signal rather than srv.Shutdown()
		// blocking on that write's proposal timeout.
		if raftKV != nil {
			raftKV.Stop()
		}
		if raftNode != nil {
			raftNode.Stop()
			raftTransport.Shutdown()
		}
		srv.Shutdown()
		log.Println("shutdown complete")
	case err := <-serveErr:
		if err != nil {
			log.Fatalf("serve: %v", err)
		}
	case err := <-raftServeErr:
		if err != nil {
			log.Fatalf("raft serve: %v", err)
		}
	}
}

// parsePeers parses a comma-separated "id=addr,id=addr" list.
func parsePeers(s string) (map[string]string, error) {
	peers := make(map[string]string)
	if s == "" {
		return peers, nil
	}
	for _, part := range strings.Split(s, ",") {
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 || kv[0] == "" || kv[1] == "" {
			return nil, fmt.Errorf("invalid peer %q, want id=host:port", part)
		}
		peers[kv[0]] = kv[1]
	}
	return peers, nil
}

// logRaftStateChanges polls the node's state and logs whenever its role,
// term, or known leader changes, for visibility when running a real
// cluster from the command line.
func logRaftStateChanges(n *raft.Node) {
	var last raft.State
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for range ticker.C {
		st := n.State()
		if st.Role != last.Role || st.Term != last.Term || st.LeaderID != last.LeaderID {
			log.Printf("raft: term=%d role=%s leader=%s", st.Term, st.Role, st.LeaderID)
			last = st
		}
	}
}
