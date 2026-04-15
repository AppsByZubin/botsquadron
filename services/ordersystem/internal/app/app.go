package app

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/AppsByZubin/botsquadron/services/ordersystem/internal/business"
	"github.com/AppsByZubin/botsquadron/services/ordersystem/internal/config"
	"github.com/AppsByZubin/botsquadron/services/ordersystem/internal/httpapi"
	"github.com/AppsByZubin/botsquadron/services/ordersystem/internal/service"
	"github.com/AppsByZubin/botsquadron/services/ordersystem/internal/store"
	"github.com/AppsByZubin/botsquadron/services/ordersystem/internal/upstox"
	"github.com/jackc/pgx/v5/pgxpool"
)

type App struct {
	cfg    config.Config
	logger *log.Logger
	store  *store.Store
	svc    *service.Service
	http   *http.Server
}

func New(ctx context.Context, cfg config.Config) (*App, error) {
	logger := log.Default()

	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("create postgres pool: %w", err)
	}

	pingCtx, pingCancel := context.WithTimeout(ctx, cfg.RequestTimeout)
	defer pingCancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}

	st, err := store.New(pool, cfg.AppTimezone, cfg.AccountInitialCash)
	if err != nil {
		pool.Close()
		return nil, err
	}

	schemaCtx, schemaCancel := context.WithTimeout(ctx, cfg.RequestTimeout)
	defer schemaCancel()
	if err := st.EnsureSchema(schemaCtx); err != nil {
		st.Close()
		return nil, err
	}

	var upClient *upstox.Client
	if strings.TrimSpace(cfg.UpstoxAccessToken) != "" {
		upClient = upstox.NewClient(cfg)
	}

	svc := service.New(cfg, st, upClient)
	biz := business.New(svc)
	handler := httpapi.New(biz, cfg.RequestTimeout)

	httpServer := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           requestLogger(handler.Routes(), logger),
		ReadHeaderTimeout: 5 * time.Second,
	}

	return &App{
		cfg:    cfg,
		logger: logger,
		store:  st,
		svc:    svc,
		http:   httpServer,
	}, nil
}

func (a *App) Run(ctx context.Context) error {
	errCh := make(chan error, 2)

	go func() {
		a.logger.Printf("ordersystem HTTP server listening on %s (mode=%s)", a.cfg.HTTPAddr, a.cfg.AppMode)
		if err := a.http.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("http server: %w", err)
		}
	}()

	go func() {
		ticker := time.NewTicker(a.cfg.SLPollInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				pollCtx, cancel := context.WithTimeout(ctx, a.cfg.RequestTimeout)
				err := a.svc.PollStopLossOrders(pollCtx)
				cancel()
				if err != nil {
					a.logger.Printf("sl poller error: %v", err)
				}
			}
		}
	}()

	select {
	case <-ctx.Done():
		a.logger.Printf("shutdown signal received")
	case err := <-errCh:
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer shutdownCancel()
		_ = a.http.Shutdown(shutdownCtx)
		a.store.Close()
		return err
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutdownCancel()

	if err := a.http.Shutdown(shutdownCtx); err != nil {
		a.logger.Printf("http shutdown error: %v", err)
	}
	a.store.Close()
	return nil
}

func requestLogger(next http.Handler, logger *log.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		logger.Printf("%s %s from=%s took=%s", r.Method, r.URL.Path, r.RemoteAddr, time.Since(start).Round(time.Millisecond))
	})
}
