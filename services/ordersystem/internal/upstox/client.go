package upstox

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/AppsByZubin/botsquadron/services/ordersystem/internal/config"
)

type Client struct {
	httpClient       *http.Client
	baseURL          string
	accessToken      string
	orderPlacePath   string
	orderModifyPath  string
	orderDetailsPath string
	orderTradesPath  string
	apiVersion       string

	statusLimiter  *requestGate
	statusCacheTTL time.Duration
	brokerCacheMu  sync.Mutex
	brokerCache    map[string]brokerCacheEntry
	brokerNextOK   map[string]time.Time
	brokerBackoff  map[string]time.Duration
}

type PlaceOrderRequest struct {
	Quantity        int     `json:"quantity"`
	Product         string  `json:"product"`
	Validity        string  `json:"validity"`
	Price           float64 `json:"price"`
	Tag             string  `json:"tag,omitempty"`
	InstrumentToken string  `json:"instrument_token"`
	OrderType       string  `json:"order_type"`
	TransactionType string  `json:"transaction_type"`
	DisclosedQty    int     `json:"disclosed_quantity,omitempty"`
	TriggerPrice    float64 `json:"trigger_price,omitempty"`
	IsAMO           bool    `json:"is_amo,omitempty"`
	Slice           bool    `json:"slice"`
}

type PlaceOrderResult struct {
	OrderID  string
	OrderIDs []string
	RawData  json.RawMessage
}

type ModifyOrderRequest struct {
	Quantity     int     `json:"quantity"`
	Validity     string  `json:"validity"`
	Price        float64 `json:"price"`
	OrderID      string  `json:"order_id"`
	OrderType    string  `json:"order_type"`
	DisclosedQty int     `json:"disclosed_quantity,omitempty"`
	TriggerPrice float64 `json:"trigger_price,omitempty"`
}

type ModifyOrderResult struct {
	OrderID string
	RawData json.RawMessage
}

type OrderStatus struct {
	OrderID      string
	Status       string
	AveragePrice *float64
	RawData      json.RawMessage
}

type OrderTrades struct {
	OrderID      string
	Filled       bool
	Status       string
	AveragePrice *float64
	RawData      json.RawMessage
}

type envelope struct {
	Status string          `json:"status"`
	Data   json.RawMessage `json:"data"`
}

type brokerCacheEntry struct {
	fetchedAt time.Time
	data      json.RawMessage
}

type requestGate struct {
	mu       sync.Mutex
	next     time.Time
	interval time.Duration
}

type RateLimitedError struct {
	Operation string
	OrderID   string
	RetryAt   time.Time
}

func (e RateLimitedError) Error() string {
	if !e.RetryAt.IsZero() {
		return fmt.Sprintf("upstox %s rate limited for order_id=%s; retry after %s", e.Operation, e.OrderID, time.Until(e.RetryAt).Round(time.Millisecond))
	}
	return fmt.Sprintf("upstox %s rate limited for order_id=%s", e.Operation, e.OrderID)
}

func IsRateLimited(err error) bool {
	var rateErr RateLimitedError
	return errors.As(err, &rateErr)
}

func NewClient(cfg config.Config) *Client {
	return &Client{
		httpClient:       &http.Client{Timeout: 20 * time.Second},
		baseURL:          cfg.UpstoxBaseURL,
		accessToken:      cfg.UpstoxAccessToken,
		orderPlacePath:   cfg.UpstoxOrderPlacePath,
		orderModifyPath:  cfg.UpstoxOrderModifyPath,
		orderDetailsPath: cfg.UpstoxOrderDetailsPath,
		orderTradesPath:  cfg.UpstoxOrderTradesPath,
		apiVersion:       cfg.UpstoxAPIVersion,
		statusLimiter:    newRequestGate(cfg.UpstoxStatusRequestGap),
		statusCacheTTL:   cfg.UpstoxStatusCacheTTL,
		brokerCache:      make(map[string]brokerCacheEntry),
		brokerNextOK:     make(map[string]time.Time),
		brokerBackoff:    make(map[string]time.Duration),
	}
}

func (c *Client) Enabled() bool {
	return strings.TrimSpace(c.accessToken) != ""
}

func (c *Client) PlaceOrder(ctx context.Context, req PlaceOrderRequest) (PlaceOrderResult, error) {
	if !c.Enabled() {
		return PlaceOrderResult{}, fmt.Errorf("upstox client is not configured")
	}

	body, err := json.Marshal(req)
	if err != nil {
		return PlaceOrderResult{}, fmt.Errorf("marshal place order request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.buildURL(c.orderPlacePath, ""), bytes.NewReader(body))
	if err != nil {
		return PlaceOrderResult{}, fmt.Errorf("create place order request: %w", err)
	}

	c.setHeaders(httpReq)
	httpReq.Header.Set("Content-Type", "application/json")

	httpResp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return PlaceOrderResult{}, fmt.Errorf("place order request failed: %w", err)
	}
	defer httpResp.Body.Close()

	payload, err := io.ReadAll(io.LimitReader(httpResp.Body, 2<<20))
	if err != nil {
		return PlaceOrderResult{}, fmt.Errorf("read place order response: %w", err)
	}

	if httpResp.StatusCode >= 300 {
		return PlaceOrderResult{}, fmt.Errorf("upstox place order failed (%d): %s", httpResp.StatusCode, strings.TrimSpace(string(payload)))
	}

	status, data, err := decodeEnvelope(payload)
	if err != nil {
		return PlaceOrderResult{}, fmt.Errorf("decode place order response: %w", err)
	}
	if status != "" && !strings.EqualFold(status, "success") {
		return PlaceOrderResult{}, fmt.Errorf("upstox place order non-success status: %s", status)
	}

	orderIDs := extractOrderIDs(data)
	if len(orderIDs) == 0 {
		return PlaceOrderResult{}, fmt.Errorf("upstox place order succeeded but order id missing")
	}

	return PlaceOrderResult{OrderID: orderIDs[0], OrderIDs: orderIDs, RawData: data}, nil
}

func (c *Client) ModifyOrder(ctx context.Context, req ModifyOrderRequest) (ModifyOrderResult, error) {
	if !c.Enabled() {
		return ModifyOrderResult{}, fmt.Errorf("upstox client is not configured")
	}
	if strings.TrimSpace(req.OrderID) == "" {
		return ModifyOrderResult{}, fmt.Errorf("order id is required")
	}
	if req.Quantity <= 0 {
		return ModifyOrderResult{}, fmt.Errorf("quantity must be > 0")
	}

	body, err := json.Marshal(req)
	if err != nil {
		return ModifyOrderResult{}, fmt.Errorf("marshal modify order request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.buildURL(c.orderModifyPath, ""), bytes.NewReader(body))
	if err != nil {
		return ModifyOrderResult{}, fmt.Errorf("create modify order request: %w", err)
	}

	c.setHeaders(httpReq)
	httpReq.Header.Set("Content-Type", "application/json")

	httpResp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return ModifyOrderResult{}, fmt.Errorf("modify order request failed: %w", err)
	}
	defer httpResp.Body.Close()

	payload, err := io.ReadAll(io.LimitReader(httpResp.Body, 2<<20))
	if err != nil {
		return ModifyOrderResult{}, fmt.Errorf("read modify order response: %w", err)
	}

	if httpResp.StatusCode >= 300 {
		return ModifyOrderResult{}, fmt.Errorf("upstox modify order failed (%d): %s", httpResp.StatusCode, strings.TrimSpace(string(payload)))
	}

	status, data, err := decodeEnvelope(payload)
	if err != nil {
		return ModifyOrderResult{}, fmt.Errorf("decode modify order response: %w", err)
	}
	if status != "" && !strings.EqualFold(status, "success") {
		return ModifyOrderResult{}, fmt.Errorf("upstox modify order non-success status: %s", status)
	}

	orderID := extractOrderID(data)
	if orderID == "" {
		orderID = strings.TrimSpace(req.OrderID)
	}

	return ModifyOrderResult{OrderID: orderID, RawData: data}, nil
}

func (c *Client) GetOrderStatus(ctx context.Context, orderID string) (OrderStatus, error) {
	if !c.Enabled() {
		return OrderStatus{}, fmt.Errorf("upstox client is not configured")
	}
	if strings.TrimSpace(orderID) == "" {
		return OrderStatus{}, fmt.Errorf("order id is required")
	}

	data, err := c.getBrokerData(ctx, brokerGETRequest{
		path:      c.orderDetailsPath,
		orderID:   orderID,
		cacheKey:  "order-details:" + strings.TrimSpace(orderID),
		operation: "order status",
	})
	if err != nil {
		return OrderStatus{}, err
	}

	obj := firstObject(data)
	orderStatus := strings.TrimSpace(extractString(obj, "status", "order_status", "state"))
	avgPrice := extractFloat(obj, "average_price", "avg_price", "price")

	return OrderStatus{
		OrderID:      orderID,
		Status:       orderStatus,
		AveragePrice: avgPrice,
		RawData:      data,
	}, nil
}

func (c *Client) GetOrderTrades(ctx context.Context, orderID string) (OrderTrades, error) {
	if !c.Enabled() {
		return OrderTrades{}, fmt.Errorf("upstox client is not configured")
	}
	if strings.TrimSpace(orderID) == "" {
		return OrderTrades{}, fmt.Errorf("order id is required")
	}

	data, err := c.getBrokerData(ctx, brokerGETRequest{
		path:      c.orderTradesPath,
		orderID:   orderID,
		cacheKey:  "order-trades:" + strings.TrimSpace(orderID),
		operation: "order trades",
	})
	if err != nil {
		return OrderTrades{}, err
	}

	filled, avgPrice := extractTradesFill(data)
	obj := firstObject(data)
	status := strings.TrimSpace(extractString(obj, "status", "order_status", "state"))

	return OrderTrades{
		OrderID:      orderID,
		Filled:       filled,
		Status:       status,
		AveragePrice: avgPrice,
		RawData:      data,
	}, nil
}

func IsTerminalOrderStatus(status string) bool {
	s := normalizeStatus(status)
	switch s {
	case "complete", "completed", "rejected", "cancelled", "canceled", "not cancelled", "not modified":
		return true
	default:
		return false
	}
}

func IsFilledOrderStatus(status string) bool {
	switch normalizeStatus(status) {
	case "complete", "completed", "filled", "executed":
		return true
	default:
		return false
	}
}

func normalizeStatus(status string) string {
	return strings.ToLower(strings.TrimSpace(status))
}

type brokerGETRequest struct {
	path      string
	orderID   string
	cacheKey  string
	operation string
}

func newRequestGate(interval time.Duration) *requestGate {
	if interval <= 0 {
		return nil
	}
	return &requestGate{interval: interval}
}

func (g *requestGate) Wait(ctx context.Context) error {
	if g == nil || g.interval <= 0 {
		return nil
	}

	g.mu.Lock()
	now := time.Now()
	wait := g.next.Sub(now)
	if wait <= 0 {
		g.next = now.Add(g.interval)
		g.mu.Unlock()
		return nil
	}
	g.next = g.next.Add(g.interval)
	g.mu.Unlock()

	timer := time.NewTimer(wait)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (c *Client) getBrokerData(ctx context.Context, req brokerGETRequest) (json.RawMessage, error) {
	req.orderID = strings.TrimSpace(req.orderID)
	req.path = strings.TrimSpace(req.path)
	if req.orderID == "" {
		return nil, fmt.Errorf("order id is required")
	}
	if req.path == "" {
		return nil, fmt.Errorf("upstox %s path is not configured", req.operation)
	}
	if req.cacheKey == "" {
		req.cacheKey = req.operation + ":" + req.orderID
	}
	if req.operation == "" {
		req.operation = "broker data"
	}

	if data, ok, err := c.cachedBrokerData(req); ok || err != nil {
		return data, err
	}

	if c.statusLimiter != nil {
		if err := c.statusLimiter.Wait(ctx); err != nil {
			return nil, fmt.Errorf("wait for upstox %s rate limiter: %w", req.operation, err)
		}
	}

	requestURL := c.buildURL(req.path, req.orderID)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
	if err != nil {
		return nil, fmt.Errorf("create %s request: %w", req.operation, err)
	}

	c.setHeaders(httpReq)

	httpResp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("%s request failed: %w", req.operation, err)
	}
	defer httpResp.Body.Close()

	payload, err := io.ReadAll(io.LimitReader(httpResp.Body, 2<<20))
	if err != nil {
		return nil, fmt.Errorf("read %s response: %w", req.operation, err)
	}

	if httpResp.StatusCode >= 300 {
		if httpResp.StatusCode == http.StatusTooManyRequests {
			return c.rememberBrokerRateLimit(req, httpResp.Header.Get("Retry-After"))
		}
		return nil, fmt.Errorf("upstox %s failed (%d): %s", req.operation, httpResp.StatusCode, strings.TrimSpace(string(payload)))
	}

	statusEnvelope, data, err := decodeEnvelope(payload)
	if err != nil {
		return nil, fmt.Errorf("decode %s response: %w", req.operation, err)
	}
	if statusEnvelope != "" && !strings.EqualFold(statusEnvelope, "success") {
		return nil, fmt.Errorf("upstox %s non-success status: %s", req.operation, statusEnvelope)
	}

	c.rememberBrokerData(req.cacheKey, data)
	return cloneRawMessage(data), nil
}

func (c *Client) cachedBrokerData(req brokerGETRequest) (json.RawMessage, bool, error) {
	c.brokerCacheMu.Lock()
	defer c.brokerCacheMu.Unlock()

	now := time.Now()
	if retryAt, ok := c.brokerNextOK[req.cacheKey]; ok && now.Before(retryAt) {
		if cached, hasCached := c.brokerCache[req.cacheKey]; hasCached {
			return cloneRawMessage(cached.data), true, nil
		}
		return nil, true, RateLimitedError{Operation: req.operation, OrderID: req.orderID, RetryAt: retryAt}
	}

	if c.statusCacheTTL > 0 {
		if cached, ok := c.brokerCache[req.cacheKey]; ok && now.Sub(cached.fetchedAt) < c.statusCacheTTL {
			return cloneRawMessage(cached.data), true, nil
		}
	}

	return nil, false, nil
}

func (c *Client) rememberBrokerData(cacheKey string, data json.RawMessage) {
	c.brokerCacheMu.Lock()
	defer c.brokerCacheMu.Unlock()

	c.brokerCache[cacheKey] = brokerCacheEntry{
		fetchedAt: time.Now(),
		data:      cloneRawMessage(data),
	}
	delete(c.brokerBackoff, cacheKey)
	delete(c.brokerNextOK, cacheKey)
}

func (c *Client) rememberBrokerRateLimit(req brokerGETRequest, retryAfterHeader string) (json.RawMessage, error) {
	c.brokerCacheMu.Lock()
	defer c.brokerCacheMu.Unlock()

	now := time.Now()
	backoff := nextBackoff(c.brokerBackoff[req.cacheKey])
	if retryAfter := parseRetryAfter(retryAfterHeader, now); retryAfter > backoff {
		backoff = retryAfter
	}

	retryAt := now.Add(backoff)
	c.brokerBackoff[req.cacheKey] = backoff
	c.brokerNextOK[req.cacheKey] = retryAt

	if cached, ok := c.brokerCache[req.cacheKey]; ok {
		return cloneRawMessage(cached.data), nil
	}

	return nil, RateLimitedError{Operation: req.operation, OrderID: req.orderID, RetryAt: retryAt}
}

func nextBackoff(previous time.Duration) time.Duration {
	if previous <= 0 {
		return 2 * time.Second
	}
	next := previous * 2
	if next > time.Minute {
		return time.Minute
	}
	return next
}

func parseRetryAfter(header string, now time.Time) time.Duration {
	header = strings.TrimSpace(header)
	if header == "" {
		return 0
	}
	if seconds, err := strconv.Atoi(header); err == nil && seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	if retryAt, err := http.ParseTime(header); err == nil && retryAt.After(now) {
		return retryAt.Sub(now)
	}
	return 0
}

func cloneRawMessage(data json.RawMessage) json.RawMessage {
	if len(data) == 0 {
		return nil
	}
	cloned := make(json.RawMessage, len(data))
	copy(cloned, data)
	return cloned
}

func (c *Client) buildURL(path string, orderID string) string {
	if strings.HasPrefix(path, "http://") || strings.HasPrefix(path, "https://") {
		return c.withQuery(path, orderID)
	}
	return c.withQuery(c.baseURL+path, orderID)
}

func (c *Client) withQuery(rawURL string, orderID string) string {
	if orderID != "" && strings.Contains(rawURL, "{order_id}") {
		return strings.ReplaceAll(rawURL, "{order_id}", url.PathEscape(orderID))
	}

	parsed, err := url.Parse(rawURL)
	if err != nil {
		return rawURL
	}
	q := parsed.Query()
	if orderID != "" {
		q.Set("order_id", orderID)
	}
	if c.apiVersion != "" {
		q.Set("api_version", c.apiVersion)
	}
	parsed.RawQuery = q.Encode()
	return parsed.String()
}

func (c *Client) setHeaders(req *http.Request) {
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.accessToken)
	if c.apiVersion != "" {
		req.Header.Set("Api-Version", c.apiVersion)
	}
}

func decodeEnvelope(payload []byte) (string, json.RawMessage, error) {
	var env envelope
	if err := json.Unmarshal(payload, &env); err != nil {
		return "", nil, err
	}
	return strings.TrimSpace(env.Status), env.Data, nil
}

func extractOrderID(data json.RawMessage) string {
	orderIDs := extractOrderIDs(data)
	if len(orderIDs) == 0 {
		return ""
	}
	return orderIDs[0]
}

func extractOrderIDs(data json.RawMessage) []string {
	if len(data) == 0 {
		return nil
	}

	var obj map[string]any
	if err := json.Unmarshal(data, &obj); err == nil && obj != nil {
		return extractOrderIDsFromObject(obj)
	}

	var list []map[string]any
	if err := json.Unmarshal(data, &list); err == nil {
		orderIDs := make([]string, 0, len(list))
		for _, item := range list {
			orderIDs = appendOrderID(orderIDs, extractString(item, "order_id", "orderId", "id"))
		}
		return orderIDs
	}

	return nil
}

func extractOrderIDsFromObject(obj map[string]any) []string {
	orderIDs := make([]string, 0, 1)
	orderIDs = appendOrderID(orderIDs, extractString(obj, "order_id", "orderId", "id"))

	for _, key := range []string{"order_ids", "orderIds", "ids"} {
		raw, ok := obj[key]
		if !ok {
			continue
		}
		switch values := raw.(type) {
		case []any:
			for _, value := range values {
				if text, ok := value.(string); ok {
					orderIDs = appendOrderID(orderIDs, text)
				}
			}
		case []string:
			for _, value := range values {
				orderIDs = appendOrderID(orderIDs, value)
			}
		}
	}

	return orderIDs
}

func appendOrderID(orderIDs []string, value string) []string {
	value = strings.TrimSpace(value)
	if value == "" {
		return orderIDs
	}
	for _, existing := range orderIDs {
		if existing == value {
			return orderIDs
		}
	}
	return append(orderIDs, value)
}

func firstObject(data json.RawMessage) map[string]any {
	if len(data) == 0 {
		return nil
	}

	var obj map[string]any
	if err := json.Unmarshal(data, &obj); err == nil && obj != nil {
		return obj
	}

	var list []map[string]any
	if err := json.Unmarshal(data, &list); err == nil && len(list) > 0 {
		return list[0]
	}

	return nil
}

func extractTradesFill(data json.RawMessage) (bool, *float64) {
	trades := tradeObjects(data)
	if len(trades) == 0 {
		obj := firstObject(data)
		if obj == nil {
			return false, nil
		}
		if IsFilledOrderStatus(extractString(obj, "status", "order_status", "state")) {
			return true, extractFloat(obj, "average_price", "avg_price", "price")
		}
		return false, nil
	}

	totalQty := 0
	totalValue := 0.0
	sawTrade := false
	for _, trade := range trades {
		if len(trade) == 0 {
			continue
		}
		sawTrade = true
		qty := extractInt(trade, "quantity", "qty", "filled_quantity", "traded_quantity")
		price := extractFloat(trade, "traded_price", "trade_price", "average_price", "avg_price", "price")
		if qty <= 0 || price == nil || *price <= 0 {
			continue
		}
		totalQty += qty
		totalValue += *price * float64(qty)
	}

	if totalQty <= 0 {
		return sawTrade, nil
	}

	avg := totalValue / float64(totalQty)
	return true, &avg
}

func tradeObjects(data json.RawMessage) []map[string]any {
	if len(data) == 0 {
		return nil
	}

	var list []map[string]any
	if err := json.Unmarshal(data, &list); err == nil {
		return list
	}

	var obj map[string]any
	if err := json.Unmarshal(data, &obj); err != nil || obj == nil {
		return nil
	}

	rawTrades, ok := obj["trades"]
	if !ok {
		return nil
	}

	encoded, err := json.Marshal(rawTrades)
	if err != nil {
		return nil
	}
	if err := json.Unmarshal(encoded, &list); err == nil {
		return list
	}
	return nil
}

func extractString(obj map[string]any, keys ...string) string {
	for _, key := range keys {
		if v, ok := obj[key]; ok {
			switch tv := v.(type) {
			case string:
				if strings.TrimSpace(tv) != "" {
					return tv
				}
			case fmt.Stringer:
				if strings.TrimSpace(tv.String()) != "" {
					return tv.String()
				}
			}
		}
	}
	return ""
}

func extractFloat(obj map[string]any, keys ...string) *float64 {
	for _, key := range keys {
		if v, ok := obj[key]; ok {
			switch tv := v.(type) {
			case float64:
				vv := tv
				return &vv
			case float32:
				vv := float64(tv)
				return &vv
			case int:
				vv := float64(tv)
				return &vv
			case int64:
				vv := float64(tv)
				return &vv
			case json.Number:
				parsed, err := tv.Float64()
				if err == nil {
					vv := parsed
					return &vv
				}
			case string:
				parsed, err := strconv.ParseFloat(strings.TrimSpace(tv), 64)
				if err == nil {
					vv := parsed
					return &vv
				}
			}
		}
	}
	return nil
}

func extractInt(obj map[string]any, keys ...string) int {
	for _, key := range keys {
		if v, ok := obj[key]; ok {
			switch tv := v.(type) {
			case int:
				return tv
			case int64:
				return int(tv)
			case float64:
				return int(tv)
			case float32:
				return int(tv)
			case json.Number:
				parsed, err := tv.Int64()
				if err == nil {
					return int(parsed)
				}
			case string:
				parsed, err := strconv.Atoi(strings.TrimSpace(tv))
				if err == nil {
					return parsed
				}
			}
		}
	}
	return 0
}
