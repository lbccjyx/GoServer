package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"GoServer/matchmaker"
	"GoServer/server"
)

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)

	mm := matchmaker.New()
	srv := server.New("0.0.0.0:8765", mm)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		if err := srv.Start(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[main] server error: %v", err)
		}
	}()

	log.Println("[main] matchmaking server started on ws://127.0.0.1:8765/ws")

	<-sigCh
	log.Println("[main] received shutdown signal")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("[main] shutdown error: %v", err)
	}
	log.Println("[main] server stopped")
}
