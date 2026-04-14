package upstox

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/AppsByZubin/botsquadron/services/ordersystem/internal/config"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

func TestClientModifyOrderSendsExpectedPayload(t *testing.T) {
	t.Parallel()

	var gotMethod string
	var gotPath string
	var gotAuth string
	var gotAPIVersion string
	var gotQueryVersion string
	var gotReq ModifyOrderRequest

	client := NewClient(config.Config{
		UpstoxBaseURL:         "https://api.example.com",
		UpstoxAccessToken:     "test-token",
		UpstoxOrderModifyPath: "/v3/order/modify",
		UpstoxAPIVersion:      "2.0",
	})
	client.httpClient = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		gotAPIVersion = r.Header.Get("Api-Version")
		gotQueryVersion = r.URL.Query().Get("api_version")

		if err := json.NewDecoder(r.Body).Decode(&gotReq); err != nil {
			t.Fatalf("decode request: %v", err)
		}

		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(`{"status":"success","data":{}}`)),
		}, nil
	})}

	resp, err := client.ModifyOrder(context.Background(), ModifyOrderRequest{
		OrderID:      "order-123",
		Quantity:     75,
		Validity:     "DAY",
		OrderType:    "SL",
		TriggerPrice: 91,
		Price:        90.5,
	})
	if err != nil {
		t.Fatalf("ModifyOrder returned error: %v", err)
	}

	if gotMethod != http.MethodPost {
		t.Fatalf("method = %s, want %s", gotMethod, http.MethodPost)
	}
	if gotPath != "/v3/order/modify" {
		t.Fatalf("path = %s, want /v3/order/modify", gotPath)
	}
	if gotAuth != "Bearer test-token" {
		t.Fatalf("authorization = %s, want Bearer test-token", gotAuth)
	}
	if gotAPIVersion != "2.0" {
		t.Fatalf("Api-Version header = %s, want 2.0", gotAPIVersion)
	}
	if gotQueryVersion != "2.0" {
		t.Fatalf("api_version query = %s, want 2.0", gotQueryVersion)
	}
	if gotReq.OrderID != "order-123" {
		t.Fatalf("order_id = %s, want order-123", gotReq.OrderID)
	}
	if gotReq.Quantity != 75 {
		t.Fatalf("quantity = %d, want 75", gotReq.Quantity)
	}
	if gotReq.Validity != "DAY" {
		t.Fatalf("validity = %s, want DAY", gotReq.Validity)
	}
	if gotReq.OrderType != "SL" {
		t.Fatalf("order_type = %s, want SL", gotReq.OrderType)
	}
	if gotReq.TriggerPrice != 91 {
		t.Fatalf("trigger_price = %v, want 91", gotReq.TriggerPrice)
	}
	if gotReq.Price != 90.5 {
		t.Fatalf("price = %v, want 90.5", gotReq.Price)
	}
	if resp.OrderID != "order-123" {
		t.Fatalf("response order_id = %s, want fallback order-123", resp.OrderID)
	}
}

func TestClientPlaceOrderCapturesSlicedOrderIDs(t *testing.T) {
	t.Parallel()

	client := NewClient(config.Config{
		UpstoxBaseURL:        "https://api.example.com",
		UpstoxAccessToken:    "test-token",
		UpstoxOrderPlacePath: "/v3/order/place",
		UpstoxAPIVersion:     "2.0",
	})
	client.httpClient = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(`{"status":"success","data":[{"order_id":"order-1"},{"order_id":"order-2"}]}`)),
		}, nil
	})}

	resp, err := client.PlaceOrder(context.Background(), PlaceOrderRequest{
		OrderType:       "MARKET",
		TransactionType: "BUY",
		Quantity:        150,
		InstrumentToken: "NSE_FO|123",
		Product:         "D",
		Validity:        "DAY",
	})
	if err != nil {
		t.Fatalf("PlaceOrder returned error: %v", err)
	}
	if resp.OrderID != "order-1" {
		t.Fatalf("primary order_id = %s, want order-1", resp.OrderID)
	}
	if len(resp.OrderIDs) != 2 || resp.OrderIDs[0] != "order-1" || resp.OrderIDs[1] != "order-2" {
		t.Fatalf("order ids = %#v, want [order-1 order-2]", resp.OrderIDs)
	}
}

func TestClientGetOrderTradesComputesAveragePrice(t *testing.T) {
	t.Parallel()

	var gotMethod string
	var gotPath string
	var gotOrderID string
	var gotAPIVersion string

	client := NewClient(config.Config{
		UpstoxBaseURL:         "https://api.example.com",
		UpstoxAccessToken:     "test-token",
		UpstoxOrderTradesPath: "/v2/order/trades",
		UpstoxAPIVersion:      "2.0",
	})
	client.httpClient = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		gotOrderID = r.URL.Query().Get("order_id")
		gotAPIVersion = r.Header.Get("Api-Version")

		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(`{"status":"success","data":[{"quantity":25,"traded_price":100},{"quantity":75,"traded_price":102}]}`)),
		}, nil
	})}

	resp, err := client.GetOrderTrades(context.Background(), "sl-123")
	if err != nil {
		t.Fatalf("GetOrderTrades returned error: %v", err)
	}

	if gotMethod != http.MethodGet {
		t.Fatalf("method = %s, want %s", gotMethod, http.MethodGet)
	}
	if gotPath != "/v2/order/trades" {
		t.Fatalf("path = %s, want /v2/order/trades", gotPath)
	}
	if gotOrderID != "sl-123" {
		t.Fatalf("order_id query = %s, want sl-123", gotOrderID)
	}
	if gotAPIVersion != "2.0" {
		t.Fatalf("Api-Version header = %s, want 2.0", gotAPIVersion)
	}
	if !resp.Filled {
		t.Fatalf("filled = false, want true")
	}
	if resp.AveragePrice == nil || *resp.AveragePrice != 101.5 {
		t.Fatalf("average price = %v, want 101.5", resp.AveragePrice)
	}
}

func TestClientGetOrderStatusReturnsCachedDataOn429(t *testing.T) {
	t.Parallel()

	calls := 0
	client := NewClient(config.Config{
		UpstoxBaseURL:          "https://api.example.com",
		UpstoxAccessToken:      "test-token",
		UpstoxOrderDetailsPath: "/v2/order/details",
		UpstoxAPIVersion:       "2.0",
		UpstoxStatusCacheTTL:   0,
	})
	client.httpClient = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		calls++
		if calls == 1 {
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader(`{"status":"success","data":{"status":"open","average_price":0}}`)),
			}, nil
		}
		return &http.Response{
			StatusCode: http.StatusTooManyRequests,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(`{"errors":[{"message":"Too Many Requests"}]}`)),
		}, nil
	})}

	first, err := client.GetOrderStatus(context.Background(), "sl-123")
	if err != nil {
		t.Fatalf("first GetOrderStatus returned error: %v", err)
	}
	second, err := client.GetOrderStatus(context.Background(), "sl-123")
	if err != nil {
		t.Fatalf("second GetOrderStatus returned error: %v", err)
	}

	if calls != 2 {
		t.Fatalf("calls = %d, want 2", calls)
	}
	if first.Status != "open" || second.Status != "open" {
		t.Fatalf("statuses = %q/%q, want open/open", first.Status, second.Status)
	}
}
