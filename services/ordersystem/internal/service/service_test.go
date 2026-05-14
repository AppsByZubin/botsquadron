package service

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/AppsByZubin/botsquadron/services/ordersystem/internal/config"
	"github.com/AppsByZubin/botsquadron/services/ordersystem/internal/model"
)

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

func TestModifyTradeRequestAcceptsTradeStateFields(t *testing.T) {
	t.Parallel()

	var req model.ModifyTradeRequest
	decoder := json.NewDecoder(strings.NewReader(`{
		"stoploss": 91,
		"sl_limit": 90.5,
		"spot_trail_anchor": 22375
	}`))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		t.Fatalf("decode ModifyTradeRequest = %v", err)
	}

	requireFloatPtr(t, "stoploss", req.Stoploss, 91)
	requireFloatPtr(t, "slLimit", req.SLLimit, 90.5)
	requireFloatPtr(t, "spotTrailAnchor", req.SpotTrailAnchor, 22375)
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

func requireFloatPtr(t *testing.T, name string, got *float64, want float64) {
	t.Helper()
	if got == nil {
		t.Fatalf("%s = nil, want %v", name, want)
	}
	if *got != want {
		t.Fatalf("%s = %v, want %v", name, *got, want)
	}
}
