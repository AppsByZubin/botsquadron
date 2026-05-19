package marketfeeder

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
)

type InstrumentSubscription struct {
	BotID          string   `json:"bot_id"`
	InstrumentKeys []string `json:"instrument_keys"`
	Action         string   `json:"action"`
	ForceReconnect bool     `json:"force_reconnect,omitempty"`
}

type TickData struct {
	BotID         string    `json:"bot_id,omitempty"`
	InstrumentKey string    `json:"instrument_key"`
	Price         float64   `json:"price"`
	Volume        int64     `json:"volume"`
	Timestamp     time.Time `json:"timestamp"`
}

type LiveFeedPayload struct {
	BotID     string               `json:"bot_id,omitempty"`
	Type      string               `json:"type"`
	Feeds     map[string]FeedEntry `json:"feeds"`
	CurrentTs string               `json:"currentTs"`
}

type FeedEntry struct {
	FullFeed map[string]interface{} `json:"fullFeed,omitempty"`
}

type BotManager struct {
	mu            sync.RWMutex
	subscriptions map[string]map[string]bool
	handler       *BotHandler
	natsConn      *nats.Conn
	upstoxToken   string
}

type BotHandler struct {
	botID             string
	instrumentKeys    map[string]bool
	conn              *websocket.Conn
	ctx               context.Context
	cancel            context.CancelFunc
	natsConn          *nats.Conn
	mu                sync.RWMutex
	writeMu           sync.Mutex
	wsRunning         bool
	lastWSReadNano    int64
	lastPublishNano   int64
	reconnectAfter    time.Time
	reconnectFailures int
}

type authorizeError struct {
	statusCode int
	status     string
	body       string
	retryAfter time.Duration
}

func (e *authorizeError) Error() string {
	return fmt.Sprintf("authorize API returned %s: %s", e.status, e.body)
}

func (e *authorizeError) rateLimited() bool {
	return e.statusCode == http.StatusTooManyRequests ||
		strings.Contains(strings.ToLower(e.body), "udapi10005") ||
		strings.Contains(strings.ToLower(e.body), "too many")
}

func NewBotManager(natsConn *nats.Conn) *BotManager {
	token := os.Getenv("UPSTOX_API_ACCESS_TOKEN")
	if token == "" {
		log.Fatal("UPSTOX_API_ACCESS_TOKEN environment variable not set")
	}

	return &BotManager{
		subscriptions: make(map[string]map[string]bool),
		natsConn:      natsConn,
		upstoxToken:   token,
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

	botID := strings.TrimSpace(sub.BotID)
	if botID == "" {
		log.Printf("Ignoring instrument subscription with empty bot_id")
		return
	}

	switch sub.Action {
	case "subscribe":
		bm.subscriptions[botID] = instrumentSet(sub.InstrumentKeys)
	case "add":
		current := bm.subscriptions[botID]
		if current == nil {
			current = make(map[string]bool)
			bm.subscriptions[botID] = current
		}
		for _, key := range sub.InstrumentKeys {
			if clean := strings.TrimSpace(key); clean != "" {
				current[clean] = true
			}
		}
	case "remove":
		current := bm.subscriptions[botID]
		for _, key := range sub.InstrumentKeys {
			delete(current, strings.TrimSpace(key))
		}
		if len(current) == 0 {
			delete(bm.subscriptions, botID)
		}
	case "unsubscribe":
		delete(bm.subscriptions, botID)
	default:
		log.Printf("Ignoring unknown subscription action for bot %s: %s", botID, sub.Action)
		return
	}

	bm.syncSharedHandlerLocked(sub.ForceReconnect)
}

func instrumentSet(instrumentKeys []string) map[string]bool {
	out := make(map[string]bool)
	for _, key := range instrumentKeys {
		if clean := strings.TrimSpace(key); clean != "" {
			out[clean] = true
		}
	}
	return out
}

func (bm *BotManager) unionInstrumentKeysLocked() []string {
	seen := make(map[string]bool)
	out := make([]string, 0)
	for _, keys := range bm.subscriptions {
		for key := range keys {
			if !seen[key] {
				seen[key] = true
				out = append(out, key)
			}
		}
	}
	return out
}

func (bm *BotManager) syncSharedHandlerLocked(forceReconnect bool) {
	unionKeys := bm.unionInstrumentKeysLocked()
	if len(unionKeys) == 0 {
		bm.stopSharedHandlerLocked("no active bot subscriptions")
		return
	}

	if bm.handler == nil {
		ctx, cancel := context.WithCancel(context.Background())
		bm.handler = &BotHandler{
			botID:          "shared-marketfeeder",
			instrumentKeys: make(map[string]bool),
			ctx:            ctx,
			cancel:         cancel,
			natsConn:       bm.natsConn,
		}
		log.Printf("Started shared marketfeeder handler")
	}

	bm.handler.updateInstruments(unionKeys, bm.upstoxToken, forceReconnect)
	log.Printf("Shared marketfeeder subscriptions: %d bots, %d union instruments", len(bm.subscriptions), len(unionKeys))
}

func (bm *BotManager) stopSharedHandlerLocked(reason string) {
	if bm.handler == nil {
		return
	}

	log.Printf("Stopping shared marketfeeder handler: %s", reason)
	bm.handler.cancel()
	bm.handler.mu.Lock()
	conn := bm.handler.conn
	bm.handler.conn = nil
	bm.handler.mu.Unlock()
	if conn != nil {
		if err := conn.Close(); err != nil {
			log.Printf("Error closing shared websocket: %v", err)
		}
	}
	bm.handler = nil
}

func (bm *BotManager) shutdown() {
	bm.mu.Lock()
	defer bm.mu.Unlock()
	bm.stopSharedHandlerLocked("shutdown")
}

func (bh *BotHandler) updateInstruments(instrumentKeys []string, token string, forceReconnect bool) {
	bh.mu.Lock()
	bh.instrumentKeys = make(map[string]bool)
	for _, key := range instrumentKeys {
		bh.instrumentKeys[key] = true
	}
	conn := bh.conn
	bh.mu.Unlock()

	if forceReconnect && conn != nil {
		log.Printf("Force reconnect requested for bot %s; recycling Upstox websocket", bh.botID)
		bh.closeWebSocket(conn, "force reconnect requested by subscriber")
		return
	}

	bh.ensureWebSocket(token)
	if conn != nil {
		if err := bh.sendSubscription(); err != nil {
			log.Printf("Error sending updated subscription for bot %s: %v", bh.botID, err)
		}
	}

	log.Printf("Updated instruments for bot %s: %d instruments", bh.botID, len(instrumentKeys))
}

func (bh *BotHandler) addInstruments(instrumentKeys []string, token string) {
	bh.mu.Lock()
	for _, key := range instrumentKeys {
		bh.instrumentKeys[key] = true
	}
	conn := bh.conn
	bh.mu.Unlock()

	bh.ensureWebSocket(token)
	if conn != nil {
		if err := bh.sendSubscription(); err != nil {
			log.Printf("Error sending updated subscription for bot %s: %v", bh.botID, err)
		}
	}

	log.Printf("Added instruments to bot %s: %d instruments", bh.botID, len(instrumentKeys))
}

func (bh *BotHandler) removeInstruments(instrumentKeys []string, token string) {
	bh.mu.Lock()
	for _, key := range instrumentKeys {
		delete(bh.instrumentKeys, key)
	}
	conn := bh.conn
	bh.mu.Unlock()

	bh.ensureWebSocket(token)
	if conn != nil {
		if err := bh.sendSubscription(); err != nil {
			log.Printf("Error sending updated subscription for bot %s: %v", bh.botID, err)
		}
	}

	log.Printf("Removed instruments from bot %s: %d instruments", bh.botID, len(instrumentKeys))
}

func (bh *BotHandler) ensureWebSocket(token string) {
	bh.mu.Lock()
	if bh.wsRunning {
		bh.mu.Unlock()
		return
	}
	if bh.ctx.Err() != nil {
		bh.mu.Unlock()
		return
	}
	if !bh.reconnectAfter.IsZero() && time.Now().Before(bh.reconnectAfter) {
		wait := time.Until(bh.reconnectAfter)
		bh.mu.Unlock()
		log.Printf("Upstox websocket reconnect for bot %s is backing off for %s", bh.botID, wait.Round(time.Second))
		return
	}
	bh.wsRunning = true
	bh.mu.Unlock()

	go bh.startWebSocket(token)
}

func (bh *BotHandler) rateLimitReconnectDelay(err error) time.Duration {
	base := getenvSeconds("UPSTOX_WS_RATE_LIMIT_RECONNECT_WAIT_SEC", 60*time.Second)
	maxWait := getenvSeconds("UPSTOX_WS_RATE_LIMIT_RECONNECT_MAX_WAIT_SEC", 5*time.Minute)
	if maxWait < base {
		maxWait = base
	}

	var authErr *authorizeError
	if errors.As(err, &authErr) && authErr.retryAfter > 0 {
		base = authErr.retryAfter
		if base > maxWait {
			maxWait = base
		}
	}

	bh.mu.Lock()
	defer bh.mu.Unlock()

	bh.reconnectFailures++
	delay := base
	for attempt := 1; attempt < bh.reconnectFailures && delay < maxWait; attempt++ {
		delay *= 2
		if delay > maxWait {
			delay = maxWait
		}
	}
	return delay
}

func isAuthorizeRateLimit(err error) bool {
	var authErr *authorizeError
	return errors.As(err, &authErr) && authErr.rateLimited()
}

func (bh *BotHandler) markWSRead() {
	atomic.StoreInt64(&bh.lastWSReadNano, time.Now().UnixNano())
}

func (bh *BotHandler) markPublished() {
	now := time.Now().UnixNano()
	atomic.StoreInt64(&bh.lastWSReadNano, now)
	atomic.StoreInt64(&bh.lastPublishNano, now)
}

func idleForSince(nano int64, now time.Time) time.Duration {
	if nano <= 0 {
		return 0
	}
	return now.Sub(time.Unix(0, nano))
}

func watchdogCheckInterval(timeout time.Duration) time.Duration {
	interval := timeout / 3
	if interval < 5*time.Second {
		return 5 * time.Second
	}
	if interval > 30*time.Second {
		return 30 * time.Second
	}
	return interval
}

func (bh *BotHandler) closeWebSocket(conn *websocket.Conn, reason string) {
	if conn == nil {
		return
	}
	log.Printf("Closing Upstox websocket for bot %s: %s", bh.botID, reason)
	bh.writeMu.Lock()
	_ = conn.WriteControl(
		websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, reason),
		time.Now().Add(5*time.Second),
	)
	bh.writeMu.Unlock()
	_ = conn.Close()
}

func (bh *BotHandler) watchPublishIdle(conn *websocket.Conn, idleTimeout time.Duration, done <-chan struct{}) {
	if idleTimeout <= 0 {
		return
	}

	ticker := time.NewTicker(watchdogCheckInterval(idleTimeout))
	defer ticker.Stop()

	for {
		select {
		case <-bh.ctx.Done():
			return
		case <-done:
			return
		case <-ticker.C:
			bh.mu.RLock()
			activeConn := bh.conn
			instrumentCount := len(bh.instrumentKeys)
			bh.mu.RUnlock()
			if activeConn != conn || instrumentCount == 0 {
				return
			}

			now := time.Now()
			lastPublish := atomic.LoadInt64(&bh.lastPublishNano)
			idleFor := idleForSince(lastPublish, now)
			if idleFor < idleTimeout {
				continue
			}

			bh.closeWebSocket(
				conn,
				fmt.Sprintf("no NATS tick publish for %s (threshold %s)", idleFor.Round(time.Second), idleTimeout),
			)
			return
		}
	}
}

func (bh *BotHandler) startWebSocket(token string) {
	log.Printf("Starting Upstox websocket for bot %s", bh.botID)

	var conn *websocket.Conn
	retryWait := getenvSeconds("UPSTOX_WS_RECONNECT_WAIT_SEC", 5*time.Second)
	defer func() {
		bh.mu.Lock()
		if conn != nil && bh.conn == conn {
			bh.conn = nil
		}
		shouldReconnect := bh.ctx.Err() == nil && len(bh.instrumentKeys) > 0
		if shouldReconnect {
			bh.reconnectAfter = time.Now().Add(retryWait)
		} else {
			bh.reconnectAfter = time.Time{}
		}
		bh.wsRunning = false
		bh.mu.Unlock()

		if shouldReconnect {
			log.Printf("Upstox websocket stopped for bot %s; reconnecting in %s", bh.botID, retryWait)
			time.AfterFunc(retryWait, func() {
				bh.ensureWebSocket(token)
			})
		}
	}()

	authorizedWSURL, err := getAuthorizedWSURL(token)
	if err != nil {
		if isAuthorizeRateLimit(err) {
			retryWait = bh.rateLimitReconnectDelay(err)
		}
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

	log.Printf("Connected Upstox websocket for bot %s", bh.botID)
	nowNano := time.Now().UnixNano()
	atomic.StoreInt64(&bh.lastWSReadNano, nowNano)
	atomic.StoreInt64(&bh.lastPublishNano, nowNano)
	bh.mu.Lock()
	bh.conn = conn
	bh.reconnectAfter = time.Time{}
	bh.reconnectFailures = 0
	bh.mu.Unlock()
	defer conn.Close()

	readTimeout := getenvSeconds("UPSTOX_WS_READ_TIMEOUT_SEC", 90*time.Second)
	publishIdleTimeout := getenvSeconds("UPSTOX_WS_PUBLISH_IDLE_TIMEOUT_SEC", 45*time.Second)
	conn.SetReadLimit(16 * 1024 * 1024)
	if err := conn.SetReadDeadline(time.Now().Add(readTimeout)); err != nil {
		log.Printf("Failed to set websocket read deadline for bot %s: %v", bh.botID, err)
	}
	conn.SetPongHandler(func(appData string) error {
		bh.markWSRead()
		return conn.SetReadDeadline(time.Now().Add(readTimeout))
	})
	conn.SetPingHandler(func(appData string) error {
		bh.markWSRead()
		if err := conn.SetReadDeadline(time.Now().Add(readTimeout)); err != nil {
			return err
		}
		bh.writeMu.Lock()
		defer bh.writeMu.Unlock()
		return conn.WriteControl(websocket.PongMessage, []byte(appData), time.Now().Add(5*time.Second))
	})

	if err := bh.sendSubscription(); err != nil {
		log.Printf("Failed to send subscription for bot %s: %v", bh.botID, err)
		return
	}

	watchdogDone := make(chan struct{})
	defer close(watchdogDone)
	go bh.pingHandler(conn, watchdogDone)
	go bh.watchPublishIdle(conn, publishIdleTimeout, watchdogDone)

	for {
		select {
		case <-bh.ctx.Done():
			return
		default:
			messageType, message, err := conn.ReadMessage()
			if err != nil {
				log.Printf("WebSocket read error for bot %s: %v", bh.botID, err)
				return
			}

			switch messageType {
			case websocket.BinaryMessage:
				bh.markWSRead()
				if err := conn.SetReadDeadline(time.Now().Add(readTimeout)); err != nil {
					log.Printf("Failed to refresh websocket read deadline for bot %s: %v", bh.botID, err)
				}
				bh.handleTickData(message)
			case websocket.TextMessage:
				bh.markWSRead()
				log.Printf("Text websocket message for bot %s: %s", bh.botID, string(message))
			default:
				bh.markWSRead()
				log.Printf("Unhandled websocket message type for bot %s: type=%d bytes=%d", bh.botID, messageType, len(message))
			}
		}
	}
}

func (bh *BotHandler) sendSubscription() error {
	bh.mu.RLock()
	instrumentKeys := make([]string, 0, len(bh.instrumentKeys))
	for key := range bh.instrumentKeys {
		instrumentKeys = append(instrumentKeys, key)
	}
	conn := bh.conn
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

	if conn == nil {
		return fmt.Errorf("websocket connection is not established")
	}

	bh.writeMu.Lock()
	defer bh.writeMu.Unlock()
	if err := conn.WriteMessage(websocket.BinaryMessage, payload); err != nil {
		return err
	}

	log.Printf("Sent subscription for bot %s: %d instruments", bh.botID, len(instrumentKeys))
	return nil
}

func (bh *BotHandler) pingHandler(conn *websocket.Conn, done <-chan struct{}) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-bh.ctx.Done():
			return
		case <-done:
			return
		case <-ticker.C:
			bh.writeMu.Lock()
			err := conn.WriteMessage(websocket.PingMessage, []byte{})
			bh.writeMu.Unlock()
			if err != nil {
				log.Printf("Ping failed for bot %s: %v", bh.botID, err)
				bh.closeWebSocket(conn, "ping failed")
				return
			}
		}
	}
}

func ltpcJSON(ltpc *LTPC) map[string]interface{} {
	if ltpc == nil {
		return nil
	}

	return map[string]interface{}{
		"ltp": ltpc.GetLtp(),
		"ltt": ltpc.GetLtt(),
		"ltq": ltpc.GetLtq(),
		"cp":  ltpc.GetCp(),
	}
}

func optionGreeksJSON(greeks *OptionGreeks) map[string]interface{} {
	if greeks == nil {
		return nil
	}

	return map[string]interface{}{
		"delta": greeks.GetDelta(),
		"theta": greeks.GetTheta(),
		"gamma": greeks.GetGamma(),
		"vega":  greeks.GetVega(),
		"rho":   greeks.GetRho(),
	}
}

func marketFullFeedJSON(feed *MarketFullFeed) map[string]interface{} {
	if feed == nil {
		return nil
	}

	out := map[string]interface{}{
		"vtt": feed.GetVtt(),
		"oi":  feed.GetOi(),
		"iv":  feed.GetIv(),
	}

	if ltpc := ltpcJSON(feed.Ltpc); ltpc != nil {
		out["ltpc"] = ltpc
	}
	if greeks := optionGreeksJSON(feed.OptionGreeks); greeks != nil {
		out["optionGreeks"] = greeks
	}

	return out
}

func indexFullFeedJSON(feed *IndexFullFeed) map[string]interface{} {
	if feed == nil {
		return nil
	}

	out := map[string]interface{}{}
	if ltpc := ltpcJSON(feed.Ltpc); ltpc != nil {
		out["ltpc"] = ltpc
	}

	return out
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
				fullFeed := make(map[string]interface{})
				if marketFF := feedUnion.FullFeed.GetMarketFF(); marketFF != nil {
					tick.Price = marketFF.Ltpc.GetLtp()
					tick.Volume = marketFF.GetVtt()
					fullFeed["marketFF"] = marketFullFeedJSON(marketFF)
				} else if indexFF := feedUnion.FullFeed.GetIndexFF(); indexFF != nil {
					tick.Price = indexFF.Ltpc.GetLtp()
					tick.Volume = 0 // Index feeds may not have volume
					fullFeed["indexFF"] = indexFullFeedJSON(indexFF)
				}

				if len(fullFeed) == 0 {
					log.Printf("Full feed for bot %s had no marketFF/indexFF for instrument %s", bh.botID, instrumentKey)
					continue
				}

				payload := LiveFeedPayload{
					Type: "live_feed",
					Feeds: map[string]FeedEntry{
						instrumentKey: {FullFeed: fullFeed},
					},
					CurrentTs: fmt.Sprintf("%d", feedResponse.CurrentTs),
				}

				payloadJSON, err := json.Marshal(payload)
				if err != nil {
					log.Printf("Error marshaling full feed payload for bot %s: %v", bh.botID, err)
					continue
				}

				err = bh.natsConn.Publish("marketfeeder.tick_data", payloadJSON)
				if err != nil {
					log.Printf("Error publishing full feed payload for bot %s: %v", bh.botID, err)
				} else {
					bh.markPublished()
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
			bh.markPublished()
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
		return "", &authorizeError{
			statusCode: resp.StatusCode,
			status:     resp.Status,
			body:       string(body),
			retryAfter: parseRetryAfter(resp.Header.Get("Retry-After")),
		}
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

func parseRetryAfter(value string) time.Duration {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}
	if seconds, err := strconv.ParseFloat(value, 64); err == nil && seconds > 0 {
		return time.Duration(seconds * float64(time.Second))
	}
	if retryAt, err := http.ParseTime(value); err == nil {
		delay := time.Until(retryAt)
		if delay > 0 {
			return delay
		}
	}
	return 0
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

	if err := nc.Flush(); err != nil {
		log.Fatalf("failed to flush NATS subscriptions: %v", err)
	}

	log.Println("Marketfeeder started. Listening for instrument subscriptions...")

	// Wait indefinitely
	select {}
}
