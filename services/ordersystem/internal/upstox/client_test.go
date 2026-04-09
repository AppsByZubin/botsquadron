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
