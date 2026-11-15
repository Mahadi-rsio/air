package main

import (
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"air-stage1/pkg/cell"
	"air-stage1/pkg/crypto"
)

func main() {
	volDir := flag.String("vol", "/vol/exp_a3_daemon", "volume directory")
	addr := flag.String("addr", "127.0.0.1:18081", "listen address")
	poolSize := flag.Int("pool", 2, "prewarm pool size")
	flag.Parse()

	kms, err := crypto.NewEnvelopeKMS()
	if err != nil {
		log.Fatalf("kms init: %v", err)
	}
	agent, err := cell.NewAgent(*volDir, *poolSize, kms)
	if err != nil {
		log.Fatalf("agent init: %v", err)
	}
	if *poolSize > 0 {
		go func() {
			_ = agent.PrewarmPool(*poolSize)
		}()
	}

	srv := &http.Server{
		Addr:    *addr,
		Handler: agent.HTTPHandler(),
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-stop
		_ = srv.Close()
		agent.Close()
		os.Exit(0)
	}()

	log.Printf("[pg-cell-agent] listening on %s (vol=%s, pid=%d)", *addr, *volDir, os.Getpid())
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("listen: %v", err)
	}
}
