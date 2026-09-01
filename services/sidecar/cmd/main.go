package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"

	"github.com/AppsByZubin/botsquadron/services/sidecar/internal/app"
	"github.com/AppsByZubin/botsquadron/services/sidecar/internal/config"
)

func main() {
	settings, err := config.Load()
	if err != nil {
		log.Fatalf("load sidecar configuration: %v", err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := app.Run(ctx, settings); err != nil {
		log.Fatalf("run sidecar: %v", err)
	}
}
