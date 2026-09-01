package natsfeed

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/AppsByZubin/botsquadron/services/sidecar/internal/model"
	"github.com/nats-io/nats.go"
)

type Config struct {
	URL                         string
	ClientName                  string
	BotID                       string
	TickSubject                 string
	InstrumentSubject           string
	ReconnectWait               time.Duration
	ConnectTimeout              time.Duration
	SubscriptionRefreshInterval time.Duration
}

type subscriptionMessage struct {
	BotID          string   `json:"bot_id"`
	InstrumentKeys []string `json:"instrument_keys,omitempty"`
	Action         string   `json:"action"`
}

// Client owns the NATS subscription and periodically refreshes marketfeeder's
// non-durable in-memory instrument registration.
type Client struct {
	config      Config
	instruments []string
	onTick      func(model.Tick)

	mu           sync.RWMutex
	connection   *nats.Conn
	subscription *nats.Subscription
	closeOnce    sync.Once
}

func New(config Config, instruments []string, onTick func(model.Tick)) *Client {
	return &Client{config: config, instruments: append([]string(nil), instruments...), onTick: onTick}
}

func (client *Client) Start(ctx context.Context) error {
	connection, err := nats.Connect(
		client.config.URL,
		nats.Name(client.config.ClientName),
		nats.Timeout(client.config.ConnectTimeout),
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(client.config.ReconnectWait),
		nats.DisconnectErrHandler(func(_ *nats.Conn, disconnectErr error) {
			log.Printf("sidecar NATS disconnected: %v", disconnectErr)
		}),
		nats.ReconnectHandler(func(_ *nats.Conn) {
			log.Printf("sidecar NATS reconnected")
			go func() {
				if publishErr := client.publishRegistration("subscribe"); publishErr != nil {
					log.Printf("refresh sidecar marketfeeder subscription after reconnect: %v", publishErr)
				}
			}()
		}),
		nats.ClosedHandler(func(connection *nats.Conn) {
			log.Printf("sidecar NATS connection closed: %v", connection.LastError())
		}),
	)
	if err != nil {
		return fmt.Errorf("connect to NATS: %w", err)
	}
	client.mu.Lock()
	client.connection = connection
	client.mu.Unlock()

	subscription, err := connection.Subscribe(client.config.TickSubject, func(message *nats.Msg) {
		ticks, decodeErr := DecodeTicks(message.Data)
		if decodeErr != nil {
			log.Printf("ignore invalid marketfeeder message: %v", decodeErr)
			return
		}
		for _, tick := range ticks {
			client.onTick(tick)
		}
	})
	if err != nil {
		connection.Close()
		return fmt.Errorf("subscribe to %s: %w", client.config.TickSubject, err)
	}
	client.mu.Lock()
	client.subscription = subscription
	client.mu.Unlock()
	if err := connection.FlushTimeout(client.config.ConnectTimeout); err != nil && connection.IsConnected() {
		connection.Close()
		return fmt.Errorf("flush NATS tick subscription: %w", err)
	}
	if connection.IsConnected() {
		if err := client.publishRegistration("subscribe"); err != nil {
			connection.Close()
			return err
		}
	}

	go client.refreshLoop(ctx)
	return nil
}

func (client *Client) refreshLoop(ctx context.Context) {
	ticker := time.NewTicker(client.config.SubscriptionRefreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !client.Connected() {
				continue
			}
			if err := client.publishRegistration("subscribe"); err != nil {
				log.Printf("refresh sidecar marketfeeder subscription: %v", err)
			}
		}
	}
}

func (client *Client) Connected() bool {
	client.mu.RLock()
	defer client.mu.RUnlock()
	return client.connection != nil && client.connection.IsConnected()
}

func (client *Client) Close() {
	client.closeOnce.Do(func() {
		client.mu.RLock()
		connection := client.connection
		subscription := client.subscription
		client.mu.RUnlock()
		if connection == nil {
			return
		}
		if connection.IsConnected() {
			if err := client.publishRegistration("unsubscribe"); err != nil {
				log.Printf("unregister sidecar instruments: %v", err)
			}
			_ = connection.FlushTimeout(client.config.ConnectTimeout)
		}
		if subscription != nil {
			_ = subscription.Unsubscribe()
		}
		connection.Close()
	})
}

func (client *Client) publishRegistration(action string) error {
	client.mu.RLock()
	connection := client.connection
	client.mu.RUnlock()
	if connection == nil || !connection.IsConnected() {
		return fmt.Errorf("NATS is not connected")
	}
	payload := subscriptionMessage{BotID: client.config.BotID, Action: action}
	if action != "unsubscribe" {
		payload.InstrumentKeys = client.instruments
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("encode instrument registration: %w", err)
	}
	if err := connection.Publish(client.config.InstrumentSubject, encoded); err != nil {
		return fmt.Errorf("publish instrument registration: %w", err)
	}
	return connection.FlushTimeout(client.config.ConnectTimeout)
}
