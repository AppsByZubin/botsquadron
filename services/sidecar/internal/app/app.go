package app

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/AppsByZubin/botsquadron/services/sidecar/internal/calculator"
	"github.com/AppsByZubin/botsquadron/services/sidecar/internal/candle"
	"github.com/AppsByZubin/botsquadron/services/sidecar/internal/config"
	"github.com/AppsByZubin/botsquadron/services/sidecar/internal/httpapi"
	"github.com/AppsByZubin/botsquadron/services/sidecar/internal/model"
	"github.com/AppsByZubin/botsquadron/services/sidecar/internal/natsfeed"
	"github.com/AppsByZubin/botsquadron/services/sidecar/internal/store"
)

func Run(ctx context.Context, settings config.Config) error {
	constituents, err := calculator.LoadConstituents(settings.WeightsPath)
	if err != nil {
		return err
	}
	engine, err := candle.NewEngine(
		constituents,
		settings.IndexInstrumentKey,
		settings.Timezone,
		settings.MarketOpen,
		settings.MarketClose,
	)
	if err != nil {
		return fmt.Errorf("create candle engine: %w", err)
	}
	snapshotStore, err := store.NewJSONStore(settings.OutputPath, settings.MaximumSnapshots)
	if err != nil {
		return err
	}
	if latest, exists := snapshotStore.Latest(); exists && latest.ExecutionDate == time.Now().In(settings.Timezone).Format("2006-01-02") {
		if err := engine.Restore(*latest); err != nil {
			return fmt.Errorf("restore latest sidecar snapshot: %w", err)
		}
		log.Printf("restored sidecar state through %s", latest.Timestamp)
	}

	runContext, cancel := context.WithCancel(ctx)
	defer cancel()
	feedClient := natsfeed.New(natsfeed.Config{
		URL:                         settings.NATSURL,
		ClientName:                  "botsquadron-sidecar",
		BotID:                       settings.BotID,
		TickSubject:                 settings.NATSTickSubject,
		InstrumentSubject:           settings.NATSInstrumentSubject,
		ReconnectWait:               settings.NATSReconnectWait,
		ConnectTimeout:              settings.NATSConnectTimeout,
		SubscriptionRefreshInterval: settings.SubscriptionRefreshInterval,
	}, engine.InstrumentKeys(), func(tick model.Tick) {
		engine.AddTick(tick)
	})
	if err := feedClient.Start(runContext); err != nil {
		return err
	}
	defer feedClient.Close()

	handler := httpapi.NewHandler(snapshotStore, func() model.Readiness {
		return engine.Readiness(feedClient.Connected())
	})
	server := &http.Server{
		Addr:              settings.HTTPAddr,
		Handler:           handler.Routes(),
		ReadHeaderTimeout: settings.HTTPReadHeaderTimeout,
		ReadTimeout:       settings.HTTPReadTimeout,
		WriteTimeout:      settings.HTTPWriteTimeout,
		IdleTimeout:       settings.HTTPIdleTimeout,
	}
	serverErrors := make(chan error, 1)
	go func() {
		log.Printf("sidecar HTTP API listening on %s", settings.HTTPAddr)
		if listenErr := server.ListenAndServe(); listenErr != nil && !errors.Is(listenErr, http.ErrServerClosed) {
			serverErrors <- listenErr
		}
	}()

	finalizeTicker := time.NewTicker(500 * time.Millisecond)
	defer finalizeTicker.Stop()
	pendingPersistence := make([]model.Snapshot, 0)
	for {
		select {
		case <-ctx.Done():
			cancel()
			feedClient.Close()
			finalized, finalizeErr := engine.FinalizeBefore(time.Now().Add(-settings.FinalizeGrace))
			if finalizeErr != nil {
				log.Printf("finalize sidecar candles during shutdown: %v", finalizeErr)
			}
			pendingPersistence = append(pendingPersistence, finalized...)
			pendingPersistence = persist(snapshotStore, pendingPersistence)
			shutdownContext, shutdownCancel := context.WithTimeout(context.Background(), settings.ShutdownTimeout)
			defer shutdownCancel()
			if shutdownErr := server.Shutdown(shutdownContext); shutdownErr != nil {
				return fmt.Errorf("shut down sidecar HTTP server: %w", shutdownErr)
			}
			if len(pendingPersistence) > 0 {
				return fmt.Errorf("%d sidecar snapshots could not be persisted", len(pendingPersistence))
			}
			return nil
		case serverErr := <-serverErrors:
			return fmt.Errorf("serve sidecar HTTP API: %w", serverErr)
		case now := <-finalizeTicker.C:
			finalized, finalizeErr := engine.FinalizeBefore(now.Add(-settings.FinalizeGrace))
			if finalizeErr != nil {
				return fmt.Errorf("finalize sidecar candles: %w", finalizeErr)
			}
			pendingPersistence = append(pendingPersistence, finalized...)
			pendingPersistence = persist(snapshotStore, pendingPersistence)
		}
	}
}

func persist(snapshotStore *store.JSONStore, pending []model.Snapshot) []model.Snapshot {
	for len(pending) > 0 {
		snapshot := pending[0]
		if err := snapshotStore.Append(snapshot); err != nil {
			log.Printf("persist sidecar snapshot %s (will retry): %v", snapshot.Timestamp, err)
			return pending
		}
		log.Printf(
			"sidecar snapshot %s puller=%.4f dragger=%.4f net=%.4f classification=%s fresh=%d/%d",
			snapshot.Timestamp, snapshot.PullerValue, snapshot.DraggerValue, snapshot.NetValue,
			snapshot.MarketClassification, snapshot.FreshCount, snapshot.ExpectedCount,
		)
		pending = pending[1:]
	}
	return pending
}
