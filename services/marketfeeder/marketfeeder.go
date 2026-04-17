package marketfeeder

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
)

type InstrumentSubscription struct {
	BotID          string   `json:"bot_id"`
	InstrumentKeys []string `json:"instrument_keys"`
	Action         string   `json:"action"`
}

type TickData struct {
	InstrumentKey string    `json:"instrument_key"`
	Price         float64   `json:"price"`
	Volume        int64     `json:"volume"`
	Timestamp     time.Time `json:"timestamp"`
}

type LiveFeedPayload struct {
	Type      string               `json:"type"`
	Feeds     map[string]FeedEntry `json:"feeds"`
	CurrentTs string               `json:"currentTs"`
}

type FeedEntry struct {
	FullFeed *FullFeed `json:"fullFeed,omitempty"`
}

type BotManager struct {
	mu          sync.RWMutex
	bots        map[string]*BotHandler
	natsConn    *nats.Conn
	upstoxToken string
}

type BotHandler struct {
	botID          string
	instrumentKeys map[string]bool
	conn           *websocket.Conn
	ctx            context.Context
	cancel         context.CancelFunc
	natsConn       *nats.Conn
	mu             sync.RWMutex
}

func NewBotManager(natsConn *nats.Conn) *BotManager {
	token := os.Getenv("UPSTOX_API_ACCESS_TOKEN")
	if token == "" {
		log.Fatal("UPSTOX_API_ACCESS_TOKEN environment variable not set")
	}

	return &BotManager{
		bots:        make(map[string]*BotHandler),
		natsConn:    natsConn,
		upstoxToken: token,
	}
}

func (bm *BotManager) handleInstrumentSubscription(msg *nats.Msg) {
	var sub InstrumentSubscription
	if err := json.Unmarshal(msg.Data, &sub); err != nil {
		log.Printf("Error unmarshaling instrument subscription: %v", err)
		return
	}

	log.Printf("Received instrument subscription for bot %s: %d instruments, action: %s",
		sub.BotID, len(sub.InstrumentKeys), sub.Action)

	bm.mu.Lock()
	defer bm.mu.Unlock()

	switch sub.Action {
	case "subscribe":
		bm.addOrUpdateBot(sub.BotID, sub.InstrumentKeys)
	case "add":
		if bot, exists := bm.bots[sub.BotID]; exists {
			bot.addInstruments(sub.InstrumentKeys)
		} else {
			bm.addOrUpdateBot(sub.BotID, sub.InstrumentKeys)
		}
	case "remove":
		if bot, exists := bm.bots[sub.BotID]; exists {
			bot.removeInstruments(sub.InstrumentKeys)
		}
	}
}

func (bm *BotManager) addOrUpdateBot(botID string, instrumentKeys []string) {
	if bot, exists := bm.bots[botID]; exists {
		bot.updateInstruments(instrumentKeys)
	} else {
		ctx, cancel := context.WithCancel(context.Background())
		bot := &BotHandler{
			botID:          botID,
			instrumentKeys: make(map[string]bool),
			ctx:            ctx,
			cancel:         cancel,
			natsConn:       bm.natsConn,
		}

		for _, key := range instrumentKeys {
			bot.instrumentKeys[key] = true
		}

		bm.bots[botID] = bot
		go bot.startWebSocket(bm.upstoxToken)
		log.Printf("Started new bot handler for %s with %d instruments", botID, len(instrumentKeys))
	}
}

func (bm *BotManager) shutdown() {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	for botID, bot := range bm.bots {
		log.Printf("Shutting down bot %s", botID)
		bot.cancel()
		if bot.conn != nil {
			bot.conn.Close()
		}
	}
}

func (bh *BotHandler) updateInstruments(instrumentKeys []string) {
	bh.mu.Lock()
	defer bh.mu.Unlock()

	bh.instrumentKeys = make(map[string]bool)
	for _, key := range instrumentKeys {
		bh.instrumentKeys[key] = true
	}

	// Send updated subscription to websocket
	if bh.conn != nil {
		if err := bh.sendSubscription(); err != nil {
			log.Printf("Error sending updated subscription for bot %s: %v", bh.botID, err)
		}
	}

	log.Printf("Updated instruments for bot %s: %d instruments", bh.botID, len(instrumentKeys))
}

func (bh *BotHandler) addInstruments(instrumentKeys []string) {
	bh.mu.Lock()
	defer bh.mu.Unlock()

	for _, key := range instrumentKeys {
		bh.instrumentKeys[key] = true
	}

	// Send updated subscription to websocket
	if bh.conn != nil {
		if err := bh.sendSubscription(); err != nil {
			log.Printf("Error sending updated subscription for bot %s: %v", bh.botID, err)
		}
	}

	log.Printf("Added instruments to bot %s: %d instruments", bh.botID, len(instrumentKeys))
}

func (bh *BotHandler) removeInstruments(instrumentKeys []string) {
	bh.mu.Lock()
	defer bh.mu.Unlock()

	for _, key := range instrumentKeys {
		delete(bh.instrumentKeys, key)
	}

	// Send updated subscription to websocket
	if bh.conn != nil {
		if err := bh.sendSubscription(); err != nil {
			log.Printf("Error sending updated subscription for bot %s: %v", bh.botID, err)
		}
	}

	log.Printf("Removed instruments from bot %s: %d instruments", bh.botID, len(instrumentKeys))
}

func (bh *BotHandler) startWebSocket(token string) {
	authorizedWSURL, err := getAuthorizedWSURL(token)
	if err != nil {
		log.Printf("Failed to get authorized websocket URL for bot %s: %v", bh.botID, err)
		return
	}

	headers := http.Header{}
	headers.Set("Authorization", fmt.Sprintf("Bearer %s", token))
	headers.Set("Accept", "*/*")

	dialer := websocket.Dialer{HandshakeTimeout: 20 * time.Second}
	conn, resp, err := dialer.Dial(authorizedWSURL, headers)
	if err != nil {
		log.Printf("Failed to connect websocket for bot %s: %v", bh.botID, err)
		if resp != nil {
			body, _ := io.ReadAll(resp.Body)
			log.Printf("response status: %s", resp.Status)
			log.Printf("response body: %s", string(body))
		}
		return
	}

	bh.conn = conn
	defer conn.Close()

	if err := bh.sendSubscription(); err != nil {
		log.Printf("Failed to send subscription for bot %s: %v", bh.botID, err)
		return
	}

	go bh.pingHandler()

	for {
		select {
		case <-bh.ctx.Done():
			return
		default:
			_, message, err := conn.ReadMessage()
			if err != nil {
				log.Printf("WebSocket read error for bot %s: %v", bh.botID, err)
				return
			}

			bh.handleTickData(message)
		}
	}
}

func (bh *BotHandler) sendSubscription() error {
	bh.mu.RLock()
	instrumentKeys := make([]string, 0, len(bh.instrumentKeys))
	for key := range bh.instrumentKeys {
		instrumentKeys = append(instrumentKeys, key)
	}
	bh.mu.RUnlock()

	req := subscriptionRequest{
		GUID:   fmt.Sprintf("go-%d", time.Now().UnixNano()),
		Method: "sub",
	}
	req.Data.Mode = "full" // ltpc | full | option_greeks | full_d30
	req.Data.InstrumentKeys = instrumentKeys

	payload, err := json.Marshal(req)
	if err != nil {
		return err
	}

	if bh.conn == nil {
		return fmt.Errorf("websocket connection is not established")
	}

	if err := bh.conn.WriteMessage(websocket.BinaryMessage, payload); err != nil {
		return err
	}

	log.Printf("Sent subscription for bot %s: %d instruments", bh.botID, len(instrumentKeys))
	return nil
}

func (bh *BotHandler) pingHandler() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-bh.ctx.Done():
			return
		case <-ticker.C:
			if bh.conn != nil {
				err := bh.conn.WriteMessage(websocket.PingMessage, []byte{})
				if err != nil {
					log.Printf("Ping failed for bot %s: %v", bh.botID, err)
					return
				}
			}
		}
	}
}

func (bh *BotHandler) handleTickData(message []byte) {
	// Parse the Protobuf-encoded tick data from Upstox v3 websocket
	var feedResponse FeedResponse
	if err := proto.Unmarshal(message, &feedResponse); err != nil {
		log.Printf("Error unmarshaling Protobuf tick data for bot %s: %v", bh.botID, err)
		log.Printf("Raw message length: %d bytes", len(message))
		return
	}

	// Process each feed in the response
	for instrumentKey, feed := range feedResponse.Feeds {
		// Check if this bot is subscribed to this instrument
		bh.mu.RLock()
		subscribed := bh.instrumentKeys[instrumentKey]
		bh.mu.RUnlock()

		if !subscribed {
			continue
		}

		// Extract tick data based on feed type
		var tick TickData
		tick.InstrumentKey = instrumentKey
		tick.Timestamp = time.Unix(feedResponse.CurrentTs/1000, (feedResponse.CurrentTs%1000)*1000000)

		// Handle different feed types
		switch feedUnion := feed.FeedUnion.(type) {
		case *Feed_Ltpc:
			if feedUnion.Ltpc != nil {
				tick.Price = feedUnion.Ltpc.Ltp
				tick.Volume = feedUnion.Ltpc.Ltq
			}
		case *Feed_FullFeed:
			if feedUnion.FullFeed != nil {
				if marketFF := feedUnion.FullFeed.GetMarketFF(); marketFF != nil {
					tick.Price = marketFF.Ltpc.GetLtp()
					tick.Volume = marketFF.GetVtt()
				} else if indexFF := feedUnion.FullFeed.GetIndexFF(); indexFF != nil {
					tick.Price = indexFF.Ltpc.GetLtp()
					tick.Volume = 0 // Index feeds may not have volume
				}

				payload := LiveFeedPayload{
					Type: "live_feed",
					Feeds: map[string]FeedEntry{
						instrumentKey: {FullFeed: feedUnion.FullFeed},
					},
					CurrentTs: fmt.Sprintf("%d", feedResponse.CurrentTs),
				}

				payloadJSON, err := json.Marshal(payload)
				if err != nil {
					log.Printf("Error marshaling full feed payload for bot %s: %v", bh.botID, err)
					continue
				}

				log.Printf("Full feed payload for bot %s: %s", bh.botID, string(payloadJSON))

				err = bh.natsConn.Publish("marketfeeder.tick_data", payloadJSON)
				if err != nil {
					log.Printf("Error publishing full feed payload for bot %s: %v", bh.botID, err)
				} else {
					log.Printf("Published full feed payload for bot %s: %s", bh.botID, instrumentKey)
				}
				continue
			}
		case *Feed_FirstLevelWithGreeks:
			if feedUnion.FirstLevelWithGreeks != nil && feedUnion.FirstLevelWithGreeks.Ltpc != nil {
				tick.Price = feedUnion.FirstLevelWithGreeks.Ltpc.Ltp
				tick.Volume = feedUnion.FirstLevelWithGreeks.Vtt
			}
		default:
			log.Printf("Unknown feed type for instrument %s", instrumentKey)
			continue
		}

		// Publish tick data to NATS
		tickJSON, err := json.Marshal(tick)
		if err != nil {
			log.Printf("Error marshaling tick data for bot %s: %v", bh.botID, err)
			continue
		}

		log.Printf("Tick payload for bot %s: %s", bh.botID, string(tickJSON))

		err = bh.natsConn.Publish("marketfeeder.tick_data", tickJSON)
		if err != nil {
			log.Printf("Error publishing tick data for bot %s: %v", bh.botID, err)
		} else {
			log.Printf("Published tick data for bot %s: %s @ %.2f (vol: %d)", bh.botID, tick.InstrumentKey, tick.Price, tick.Volume)
		}
	}

	// Handle market info if present
	if feedResponse.MarketInfo != nil {
		log.Printf("Received market info for bot %s: %v", bh.botID, feedResponse.MarketInfo.SegmentStatus)
	}
}

func getAuthorizedWSURL(token string) (string, error) {
	const authorizeURL = "https://api.upstox.com/v3/feed/market-data-feed/authorize"

	req, err := http.NewRequest(http.MethodGet, authorizeURL, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
	req.Header.Set("Accept", "application/json")

	client := &http.Client{Timeout: 20 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("authorize API returned %s: %s", resp.Status, string(body))
	}

	var out authorizeResponse
	if err := json.Unmarshal(body, &out); err != nil {
		return "", fmt.Errorf("failed to parse authorize response: %w; body=%s", err, string(body))
	}
	if out.Data.AuthorizedRedirectURI == "" {
		return "", fmt.Errorf("authorize response missing authorized_redirect_uri: %s", string(body))
	}

	return out.Data.AuthorizedRedirectURI, nil
}

func getenvDefault(key, fallback string) string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	return v
}

func getenvInt(key string, fallback int) int {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(v)
	if err != nil {
		log.Printf("Invalid %s=%q, using %d", key, v, fallback)
		return fallback
	}
	return parsed
}

func getenvSeconds(key string, fallback time.Duration) time.Duration {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	seconds, err := strconv.ParseFloat(v, 64)
	if err != nil || seconds <= 0 {
		log.Printf("Invalid %s=%q, using %s", key, v, fallback)
		return fallback
	}
	return time.Duration(seconds * float64(time.Second))
}

func connectNATSWithRetry(natsURL string) (*nats.Conn, error) {
	maxAttempts := getenvInt("NATS_CONNECT_RETRY_MAX", 0)
	retryWait := getenvSeconds("NATS_CONNECT_RETRY_WAIT_SEC", 2*time.Second)
	connectTimeout := getenvSeconds("NATS_CONNECT_TIMEOUT_SEC", 5*time.Second)

	for attempt := 1; ; attempt++ {
		nc, err := nats.Connect(
			natsURL,
			nats.Name("marketfeeder"),
			nats.Timeout(connectTimeout),
			nats.MaxReconnects(-1),
			nats.ReconnectWait(retryWait),
			nats.DisconnectErrHandler(func(conn *nats.Conn, err error) {
				log.Printf("NATS disconnected: %v", err)
			}),
			nats.ReconnectHandler(func(conn *nats.Conn) {
				log.Printf("NATS reconnected to %s", conn.ConnectedUrl())
			}),
			nats.ClosedHandler(func(conn *nats.Conn) {
				log.Printf("NATS connection closed: %v", conn.LastError())
			}),
		)
		if err == nil {
			return nc, nil
		}

		if maxAttempts > 0 && attempt >= maxAttempts {
			return nil, fmt.Errorf("connect to NATS at %s after %d attempts: %w", natsURL, attempt, err)
		}

		if maxAttempts > 0 {
			log.Printf("NATS connect to %s failed on attempt %d/%d: %v; retrying in %s", natsURL, attempt, maxAttempts, err, retryWait)
		} else {
			log.Printf("NATS connect to %s failed on attempt %d: %v; retrying in %s", natsURL, attempt, err, retryWait)
		}
		time.Sleep(retryWait)
	}
}

type authorizeResponse struct {
	Status string `json:"status"`
	Data   struct {
		AuthorizedRedirectURI string `json:"authorized_redirect_uri"`
	} `json:"data"`
}

type subscriptionRequest struct {
	GUID   string `json:"guid"`
	Method string `json:"method"`
	Data   struct {
		Mode           string   `json:"mode,omitempty"`
		InstrumentKeys []string `json:"instrumentKeys"`
	} `json:"data"`
}

func Run() {
	// NATS connection
	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = nats.DefaultURL
	}

	nc, err := connectNATSWithRetry(natsURL)
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	log.Println("Connected to NATS")

	// Create bot manager
	bm := NewBotManager(nc)

	// Subscribe to instrument key updates
	_, err = nc.Subscribe("marketfeeder.instrument_keys", bm.handleInstrumentSubscription)
	if err != nil {
		log.Fatal(err)
	}

	_, err = nc.Subscribe("marketfeeder.add_instruments", bm.handleInstrumentSubscription)
	if err != nil {
		log.Fatal(err)
	}

	_, err = nc.Subscribe("marketfeeder.remove_instruments", bm.handleInstrumentSubscription)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Marketfeeder started. Listening for instrument subscriptions...")

	// Wait indefinitely
	select {}
}
