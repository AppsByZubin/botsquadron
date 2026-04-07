package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/AppsByZubin/botsquadron/services/ordersystem/internal/app"
	"github.com/AppsByZubin/botsquadron/services/ordersystem/internal/config"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	ordersApp, err := app.New(ctx, cfg)
	if err != nil {
		log.Fatalf("failed to initialize ordersystem app: %v", err)
	}

	if err := ordersApp.Run(ctx); err != nil {
		log.Fatalf("ordersystem app stopped with error: %v", err)
	}
}
