package upstox

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
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
	apiVersion       string
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
	OrderID string
	RawData json.RawMessage
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

type envelope struct {
	Status string          `json:"status"`
	Data   json.RawMessage `json:"data"`
}

func NewClient(cfg config.Config) *Client {
	return &Client{
		httpClient:       &http.Client{Timeout: 20 * time.Second},
		baseURL:          cfg.UpstoxBaseURL,
		accessToken:      cfg.UpstoxAccessToken,
		orderPlacePath:   cfg.UpstoxOrderPlacePath,
		orderModifyPath:  cfg.UpstoxOrderModifyPath,
		orderDetailsPath: cfg.UpstoxOrderDetailsPath,
		apiVersion:       cfg.UpstoxAPIVersion,
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

	orderID := extractOrderID(data)
	if orderID == "" {
		return PlaceOrderResult{}, fmt.Errorf("upstox place order succeeded but order id missing")
	}

	return PlaceOrderResult{OrderID: orderID, RawData: data}, nil
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

	requestURL := c.buildURL(c.orderDetailsPath, orderID)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
	if err != nil {
		return OrderStatus{}, fmt.Errorf("create order status request: %w", err)
	}

	c.setHeaders(httpReq)

	httpResp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return OrderStatus{}, fmt.Errorf("order status request failed: %w", err)
	}
	defer httpResp.Body.Close()

	payload, err := io.ReadAll(io.LimitReader(httpResp.Body, 2<<20))
	if err != nil {
		return OrderStatus{}, fmt.Errorf("read order status response: %w", err)
	}

	if httpResp.StatusCode >= 300 {
		return OrderStatus{}, fmt.Errorf("upstox order status failed (%d): %s", httpResp.StatusCode, strings.TrimSpace(string(payload)))
	}

	statusEnvelope, data, err := decodeEnvelope(payload)
	if err != nil {
		return OrderStatus{}, fmt.Errorf("decode order status response: %w", err)
	}
	if statusEnvelope != "" && !strings.EqualFold(statusEnvelope, "success") {
		return OrderStatus{}, fmt.Errorf("upstox order status non-success status: %s", statusEnvelope)
	}

	obj := firstObject(data)
	orderStatus := strings.TrimSpace(extractString(obj, "status", "order_status", "state"))
	if orderStatus == "" {
		orderStatus = strings.TrimSpace(statusEnvelope)
	}

	avgPrice := extractFloat(obj, "average_price", "avg_price", "price")

	return OrderStatus{
		OrderID:      orderID,
		Status:       orderStatus,
		AveragePrice: avgPrice,
		RawData:      data,
	}, nil
}

func IsTerminalOrderStatus(status string) bool {
	s := normalizeStatus(status)
	switch s {
	case "complete", "rejected", "cancelled", "canceled", "not cancelled", "not modified":
		return true
	default:
		return false
	}
}

func IsFilledOrderStatus(status string) bool {
	return normalizeStatus(status) == "complete"
}

func normalizeStatus(status string) string {
	return strings.ToLower(strings.TrimSpace(status))
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
	obj := firstObject(data)
	if obj == nil {
		return ""
	}
	return strings.TrimSpace(extractString(obj, "order_id", "orderId", "id"))
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
