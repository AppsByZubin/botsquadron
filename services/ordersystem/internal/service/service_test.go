package service

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/AppsByZubin/botsquadron/services/ordersystem/internal/config"
	"github.com/AppsByZubin/botsquadron/services/ordersystem/internal/model"
	"github.com/AppsByZubin/botsquadron/services/ordersystem/internal/store"
	"github.com/AppsByZubin/botsquadron/services/ordersystem/internal/upstox"
)

func TestThresholdKillSwitchExpiresAcrossTradingDays(t *testing.T) {
	t.Parallel()

	reason := "threshold_day_loss reached for 03-08-2026: day_loss=15047.29 threshold=10000.00 realized_pnl=-15047.29"
	if !thresholdKillSwitchExpired(reason, "19-08-2026") {
		t.Fatal("thresholdKillSwitchExpired returned false for a prior trading day")
	}
	if thresholdKillSwitchExpired(reason, "03-08-2026") {
		t.Fatal("thresholdKillSwitchExpired returned true for the same trading day")
	}
	if thresholdKillSwitchExpired("manual kill", "19-08-2026") {
		t.Fatal("manual kill was incorrectly treated as an expired threshold kill")
	}
}

func TestNormalizeServiceDate(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.August, 19, 14, 0, 0, 0, time.FixedZone("IST", 5*60*60+30*60))
	for input, want := range map[string]string{
		"2026-08-19": "19-08-2026",
		"19/08/2026": "19-08-2026",
		"19-08-2026": "19-08-2026",
		"":           "19-08-2026",
	} {
		if got := normalizeServiceDate(input, now); got != want {
			t.Fatalf("normalizeServiceDate(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestModifyTradeRequiresTradeID(t *testing.T) {
	t.Parallel()

	svc := New(config.Config{AppMode: config.ModeSandbox}, nil, nil)

	_, err := svc.ModifyTrade(context.Background(), "", model.ModifyTradeRequest{})
	if err == nil {
		t.Fatal("ModifyTrade returned nil error, want validation error")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "trade id is required") {
		t.Fatalf("error = %q, want trade id validation", err.Error())
	}
}

func TestIsStopLossFillAcceptsCompleteOrderDetails(t *testing.T) {
	t.Parallel()

	status := upstox.OrderStatus{
		Status:         "complete",
		Quantity:       65,
		FilledQuantity: 65,
	}

	if !isStopLossFill(status, upstox.OrderTrades{}, false) {
		t.Fatal("isStopLossFill returned false for complete order details")
	}
}

func TestIsStopLossFillAcceptsFullQuantityWhenStatusVaries(t *testing.T) {
	t.Parallel()

	status := upstox.OrderStatus{
		Status:         "validation pending",
		Quantity:       65,
		FilledQuantity: 65,
	}

	if !isStopLossFill(status, upstox.OrderTrades{}, false) {
		t.Fatal("isStopLossFill returned false for fully filled order details")
	}
}

func TestModifyTradeRequestAcceptsTradeStateFields(t *testing.T) {
	t.Parallel()

	var req model.ModifyTradeRequest
	decoder := json.NewDecoder(strings.NewReader(`{
		"stoploss": 91,
		"sl_limit": 90.5,
		"spot_trail_anchor": 22375,
		"force_trail": true
	}`))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		t.Fatalf("decode ModifyTradeRequest = %v", err)
	}

	requireFloatPtr(t, "stoploss", req.Stoploss, 91)
	requireFloatPtr(t, "slLimit", req.SLLimit, 90.5)
	requireFloatPtr(t, "spotTrailAnchor", req.SpotTrailAnchor, 22375)
	if !req.ForceTrail {
		t.Fatal("ForceTrail = false, want true")
	}
}

func TestModifyTradeRequestRejectsAliases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
	}{
		{
			name: "pascal case field names",
			body: `{
				"Stoploss": 91,
				"SLLimit": 90.5,
				"SpotTrailAnchor": 22375
			}`,
		},
		{
			name: "old order field names",
			body: `{
				"trigger_price": 91,
				"price": 90.5
			}`,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var req model.ModifyTradeRequest
			decoder := json.NewDecoder(strings.NewReader(tt.body))
			decoder.DisallowUnknownFields()
			if err := decoder.Decode(&req); err == nil || !strings.Contains(err.Error(), "unknown field") {
				t.Fatalf("decode error = %v, want unknown field error", err)
			}
		})
	}
}

func TestValidateModifyTradeRequestRejectsInvalidData(t *testing.T) {
	t.Parallel()

	validStoploss := 91.0
	validSLLimit := 90.5
	validSpotTrailAnchor := 22375.0
	zero := 0.0
	negative := -1.0

	tests := []struct {
		name            string
		req             model.ModifyTradeRequest
		stoploss        *float64
		slLimit         *float64
		spotTrailAnchor *float64
		validity        string
		orderType       string
		wantErr         string
	}{
		{
			name:      "negative disclosed quantity",
			req:       model.ModifyTradeRequest{DisclosedQty: -1},
			stoploss:  &validStoploss,
			slLimit:   &validSLLimit,
			validity:  "DAY",
			orderType: "SL",
			wantErr:   "disclosed_quantity must be >= 0",
		},
		{
			name:      "invalid validity",
			stoploss:  &validStoploss,
			slLimit:   &validSLLimit,
			validity:  "GTC",
			orderType: "SL",
			wantErr:   "validity must be DAY or IOC",
		},
		{
			name:      "invalid order type",
			stoploss:  &validStoploss,
			slLimit:   &validSLLimit,
			validity:  "DAY",
			orderType: "MARKET",
			wantErr:   "order_type must be SL or SL-M",
		},
		{
			name:      "no update fields",
			validity:  "DAY",
			orderType: "SL",
			wantErr:   "at least one of stoploss, sl_limit, or spot_trail_anchor is required",
		},
		{
			name:      "zero stoploss",
			stoploss:  &zero,
			validity:  "DAY",
			orderType: "SL",
			wantErr:   "stoploss must be > 0",
		},
		{
			name:      "negative sl limit",
			slLimit:   &negative,
			validity:  "DAY",
			orderType: "SL",
			wantErr:   "sl_limit must be > 0",
		},
		{
			name:            "zero spot trail anchor",
			spotTrailAnchor: &zero,
			validity:        "DAY",
			orderType:       "SL",
			wantErr:         "spot_trail_anchor must be > 0",
		},
		{
			name:            "valid spot trail anchor only",
			spotTrailAnchor: &validSpotTrailAnchor,
			validity:        "DAY",
			orderType:       "SL",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := validateModifyTradeRequest(tt.req, tt.stoploss, tt.slLimit, tt.spotTrailAnchor, tt.validity, tt.orderType)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("validateModifyTradeRequest returned error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error = %v, want %q", err, tt.wantErr)
			}
		})
	}
}

func TestValidateProductionModifyTradeRequestRequiresBrokerFields(t *testing.T) {
	t.Parallel()

	stoploss := 91.0
	slLimit := 90.5

	tests := []struct {
		name      string
		orderType string
		stoploss  *float64
		slLimit   *float64
		wantErr   string
	}{
		{
			name:      "missing stoploss",
			orderType: "SL",
			slLimit:   &slLimit,
			wantErr:   "stoploss is required in sandbox/production mode",
		},
		{
			name:      "missing sl limit for SL",
			orderType: "SL",
			stoploss:  &stoploss,
			wantErr:   "sl_limit is required for SL order modification in sandbox/production mode",
		},
		{
			name:      "SL-M allows missing sl limit",
			orderType: "SL-M",
			stoploss:  &stoploss,
		},
		{
			name:      "SL has required fields",
			orderType: "SL",
			stoploss:  &stoploss,
			slLimit:   &slLimit,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := validateProductionModifyTradeRequest(tt.orderType, tt.stoploss, tt.slLimit)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("validateProductionModifyTradeRequest returned error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error = %v, want %q", err, tt.wantErr)
			}
		})
	}
}

func TestValidateModifiedTradeAgainstTradeChecksSLLimitDirection(t *testing.T) {
	t.Parallel()

	stoploss := 91.0
	lowerLimit := 90.5
	higherLimit := 91.5

	tests := []struct {
		name    string
		trade   model.Trade
		slLimit *float64
		wantErr string
	}{
		{
			name:    "BUY allows limit below stoploss",
			trade:   model.Trade{Side: "BUY"},
			slLimit: &lowerLimit,
		},
		{
			name:    "BUY rejects limit above stoploss",
			trade:   model.Trade{Side: "BUY"},
			slLimit: &higherLimit,
			wantErr: "sl_limit must be less than stoploss for BUY trades",
		},
		{
			name:    "SELL allows limit above stoploss",
			trade:   model.Trade{Side: "SELL"},
			slLimit: &higherLimit,
		},
		{
			name:    "SELL rejects limit below stoploss",
			trade:   model.Trade{Side: "SELL"},
			slLimit: &lowerLimit,
			wantErr: "sl_limit must be greater than stoploss for SELL trades",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := validateModifiedTradeAgainstTrade(tt.trade, &stoploss, tt.slLimit)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("validateModifiedTradeAgainstTrade returned error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error = %v, want %q", err, tt.wantErr)
			}
		})
	}
}

func TestShouldSkipStoplossModify(t *testing.T) {
	t.Parallel()

	lowerStoploss := 95.0
	equalStoploss := 100.0
	higherStoploss := 105.0

	tests := []struct {
		name     string
		trade    model.Trade
		stoploss *float64
		want     bool
	}{
		{
			name:     "lower requested stoploss is skipped",
			trade:    model.Trade{Stoploss: 100},
			stoploss: &lowerStoploss,
			want:     true,
		},
		{
			name:     "equal requested stoploss is skipped",
			trade:    model.Trade{Stoploss: 100},
			stoploss: &equalStoploss,
			want:     true,
		},
		{
			name:     "higher requested stoploss is allowed",
			trade:    model.Trade{Stoploss: 100},
			stoploss: &higherStoploss,
		},
		{
			name:     "regular modify also skips lower requested stoploss",
			trade:    model.Trade{Stoploss: 100},
			stoploss: &lowerStoploss,
			want:     true,
		},
		{
			name:     "missing current stoploss does not skip",
			trade:    model.Trade{},
			stoploss: &lowerStoploss,
		},
		{
			name:  "missing requested stoploss does not skip",
			trade: model.Trade{Stoploss: 100},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := shouldSkipStoplossModify(tt.trade, tt.stoploss)
			if got != tt.want {
				t.Fatalf("shouldSkipStoplossModify() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSLOrderQuantityUsesTradeQty(t *testing.T) {
	t.Parallel()

	trade := model.Trade{
		Qty:        75,
		SLOrderIDs: []string{"sl-1", "sl-2"},
	}

	if got := slOrderQuantity(trade, "sl-1"); got != 75 {
		t.Fatalf("slOrderQuantity(sl-1) = %d, want 75", got)
	}
	if got := slOrderQuantity(trade, "sl-2"); got != 75 {
		t.Fatalf("slOrderQuantity(sl-2) = %d, want 75", got)
	}
}

func TestSLOrderQuantityRequiresOrderID(t *testing.T) {
	t.Parallel()

	trade := model.Trade{
		Qty:        75,
		SLOrderIDs: []string{"sl-1"},
	}

	if got := slOrderQuantity(trade, ""); got != 0 {
		t.Fatalf("slOrderQuantity(empty order id) = %d, want 0", got)
	}
}

func TestBuildOrderParamsCapturesExchangeOrderID(t *testing.T) {
	t.Parallel()

	orders := buildOrderParams(
		[]model.OrderRef{{OrderID: "order-1", ExchangeOrderID: "exchange-1"}},
		store.CreateOrderParams{OrderType: "entry"},
	)

	if len(orders) != 1 {
		t.Fatalf("orders length = %d, want 1", len(orders))
	}
	if orders[0].OrderID != "order-1" || orders[0].ExchangeOrderID != "exchange-1" {
		t.Fatalf("order params = %#v, want order/exchange ids", orders[0])
	}
}

func TestIsTerminalModifyOrderError(t *testing.T) {
	t.Parallel()

	terminalErr := `upstox modify order failed (400): {"errors":[{"errorCode":"UDAPI100041","message":"Modifications of already cancelled/rejected/completed orders is not allowed"}]}`
	if !isTerminalModifyOrderError(errors.New(terminalErr)) {
		t.Fatal("isTerminalModifyOrderError returned false for terminal Upstox modify error")
	}
	if isTerminalModifyOrderError(errors.New("temporary rate limit")) {
		t.Fatal("isTerminalModifyOrderError returned true for unrelated error")
	}
}

func TestSquareOffBrokerOrdersCancelsStopLossBeforeExitByTag(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	calls := make([]string, 0, 4)
	recordCall := func(r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		calls = append(calls, r.Method+" "+r.URL.Path+" order_id="+r.URL.Query().Get("order_id")+" tag="+r.URL.Query().Get("tag"))
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		recordCall(r)
		w.Header().Set("Content-Type", "application/json")

		switch r.URL.Path {
		case "/v2/order/details":
			if r.Method != http.MethodGet || r.URL.Query().Get("order_id") != "sl-123" {
				http.Error(w, "unexpected order details request", http.StatusBadRequest)
				return
			}
			_, _ = w.Write([]byte(`{"status":"success","data":{"order_id":"sl-123","status":"trigger pending","quantity":75,"filled_quantity":0}}`))
		case "/v2/order/trades":
			if r.Method != http.MethodGet || r.URL.Query().Get("order_id") != "sl-123" {
				http.Error(w, "unexpected order trades request", http.StatusBadRequest)
				return
			}
			_, _ = w.Write([]byte(`{"status":"success","data":[]}`))
		case "/v3/order/cancel":
			if r.Method != http.MethodDelete || r.URL.Query().Get("order_id") != "sl-123" {
				http.Error(w, "unexpected cancel request", http.StatusBadRequest)
				return
			}
			_, _ = w.Write([]byte(`{"status":"success","data":{"order_id":"sl-123"}}`))
		case "/v2/order/positions/exit":
			if r.Method != http.MethodPost || r.URL.Query().Get("tag") != "firebot-entry" {
				http.Error(w, "unexpected exit request", http.StatusBadRequest)
				return
			}
			_, _ = w.Write([]byte(`{"status":"success","data":{"order_ids":["exit-123"]}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client := upstox.NewClient(config.Config{
		UpstoxBaseURL:           server.URL,
		UpstoxAccessToken:       "test-token",
		UpstoxOrderCancelPath:   "/v3/order/cancel",
		UpstoxExitPositionsPath: "/v2/order/positions/exit",
		UpstoxOrderDetailsPath:  "/v2/order/details",
		UpstoxOrderTradesPath:   "/v2/order/trades",
	})
	svc := New(config.Config{AppMode: config.ModeProduction}, nil, client)

	exitOrderIDs, err := svc.squareOffBrokerOrders(context.Background(), model.Trade{
		ID:         "trade-1",
		BotName:    "firebot",
		Qty:        75,
		SLOrderIDs: []string{"sl-123"},
		TagEntry:   "firebot-entry",
	}, "DAY", 0)
	if err != nil {
		t.Fatalf("squareOffBrokerOrders returned error: %v", err)
	}
	if len(exitOrderIDs) != 1 || exitOrderIDs[0] != "exit-123" {
		t.Fatalf("exit order ids = %#v, want [exit-123]", exitOrderIDs)
	}

	mu.Lock()
	got := append([]string(nil), calls...)
	mu.Unlock()
	want := []string{
		"GET /v2/order/details order_id=sl-123 tag=",
		"GET /v2/order/trades order_id=sl-123 tag=",
		"DELETE /v3/order/cancel order_id=sl-123 tag=",
		"POST /v2/order/positions/exit order_id= tag=firebot-entry",
	}
	if len(got) != len(want) {
		t.Fatalf("broker calls = %#v, want %#v", got, want)
	}
	for idx := range want {
		if got[idx] != want[idx] {
			t.Fatalf("broker calls = %#v, want %#v", got, want)
		}
	}
}

func TestTradePositionExitTagFallsBackToBotEntryTag(t *testing.T) {
	t.Parallel()

	if got := tradePositionExitTag(model.Trade{BotName: "firebot"}); got != "firebot-entry" {
		t.Fatalf("tradePositionExitTag fallback = %q, want firebot-entry", got)
	}
	if got := tradePositionExitTag(model.Trade{BotName: "firebot", TagEntry: "custom-entry"}); got != "custom-entry" {
		t.Fatalf("tradePositionExitTag custom = %q, want custom-entry", got)
	}
}

func TestSquareOffBrokerOrderQuantityUsesOrderQtyWhenPresent(t *testing.T) {
	t.Parallel()

	trade := model.Trade{
		Qty:        150,
		SLOrderIDs: []string{"sl-1", "sl-2"},
		Orders: []model.Order{
			{OrderID: "sl-1", OrderType: "sl", Qty: 75},
			{OrderID: "sl-2", OrderType: "sl", Qty: 75},
		},
	}

	if got := squareOffBrokerOrderQuantity(trade, "sl-1"); got != 75 {
		t.Fatalf("squareOffBrokerOrderQuantity(sl-1) = %d, want 75", got)
	}
}

func TestSquareOffBrokerOrderQuantitySplitsTradeQty(t *testing.T) {
	t.Parallel()

	trade := model.Trade{
		Qty:        150,
		SLOrderIDs: []string{"sl-1", "sl-2"},
	}

	if got := squareOffBrokerOrderQuantity(trade, "sl-1"); got != 75 {
		t.Fatalf("squareOffBrokerOrderQuantity(sl-1) = %d, want 75", got)
	}
}

func TestKillPositionTagsUsesEntryTags(t *testing.T) {
	t.Parallel()

	trades := []model.Trade{
		{TagEntry: "bot-entry", TagSL: "bot-sl"},
		{TagEntry: "bot-entry"},
		{TagEntry: "custom-entry"},
	}

	got := killPositionTags("bot", "", trades)
	want := []string{"bot-entry", "custom-entry"}
	if len(got) != len(want) {
		t.Fatalf("tags = %#v, want %#v", got, want)
	}
	for idx := range want {
		if got[idx] != want[idx] {
			t.Fatalf("tags = %#v, want %#v", got, want)
		}
	}
}

func TestKillPositionTagsDefaultsToBotEntryTag(t *testing.T) {
	t.Parallel()

	got := killPositionTags("bot-a", "", []model.Trade{{TagSL: "bot-a-sl"}})
	if len(got) != 1 || got[0] != "bot-a-entry" {
		t.Fatalf("tags = %#v, want bot-a-entry", got)
	}
}

func TestDayLossThresholdReached(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		dayLoss   float64
		threshold float64
		want      bool
	}{
		{name: "disabled at zero", dayLoss: 100, threshold: 0},
		{name: "below threshold", dayLoss: 99.99, threshold: 100},
		{name: "at threshold", dayLoss: 100, threshold: 100, want: true},
		{name: "above threshold", dayLoss: 101, threshold: 100, want: true},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := dayLossThresholdReached(tt.dayLoss, tt.threshold); got != tt.want {
				t.Fatalf("dayLossThresholdReached(%v, %v) = %v, want %v", tt.dayLoss, tt.threshold, got, tt.want)
			}
		})
	}
}

func requireFloatPtr(t *testing.T, name string, got *float64, want float64) {
	t.Helper()
	if got == nil {
		t.Fatalf("%s = nil, want %v", name, want)
	}
	if *got != want {
		t.Fatalf("%s = %v, want %v", name, *got, want)
	}
}
