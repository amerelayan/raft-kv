// Command node starts a single key-value store node listening for client
// connections over TCP.
package main

import (
	"context"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"raftkv/internal/server"
	"raftkv/internal/store"
)

func main() {
	addr := flag.String("addr", ":9000", "TCP address to listen on")
	flag.Parse()

	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("listen on %s: %v", *addr, err)
	}

	kv := store.New()
	srv := server.New(kv)

	serveErr := make(chan error, 1)
	go func() {
		serveErr <- srv.Serve(ln)
	}()

	log.Printf("node listening on %s", ln.Addr())

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	select {
	case <-ctx.Done():
		log.Println("shutting down...")
		srv.Shutdown()
		log.Println("shutdown complete")
	case err := <-serveErr:
		if err != nil {
			log.Fatalf("serve: %v", err)
		}
	}
}
