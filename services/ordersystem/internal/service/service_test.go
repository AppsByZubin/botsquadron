package service

import (
	"context"
	"strings"
	"testing"

	"github.com/AppsByZubin/botsquadron/services/ordersystem/internal/config"
	"github.com/AppsByZubin/botsquadron/services/ordersystem/internal/model"
)

func TestModifyTradeRequiresTradeID(t *testing.T) {
	t.Parallel()

	svc := New(config.Config{AppMode: "mock"}, nil, nil)

	_, err := svc.ModifyTrade(context.Background(), "", model.ModifyTradeRequest{})
	if err == nil {
		t.Fatal("ModifyTrade returned nil error, want validation error")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "trade id is required") {
		t.Fatalf("error = %q, want trade id validation", err.Error())
	}
}

func TestSLOrderQuantityUsesSliceMapBeforeTradeQty(t *testing.T) {
	t.Parallel()

	trade := model.Trade{
		Qty:        75,
		SLOrderIDs: []string{"sl-1", "sl-2"},
		SLOrderQtyMap: map[string]int{
			"sl-1": 25,
			"sl-2": 50,
		},
	}

	if got := slOrderQuantity(trade, "sl-1"); got != 25 {
		t.Fatalf("slOrderQuantity(sl-1) = %d, want 25", got)
	}
	if got := slOrderQuantity(trade, "sl-missing"); got != 0 {
		t.Fatalf("slOrderQuantity(multi-slice fallback) = %d, want 0", got)
	}
}

func TestSLOrderQuantityFallsBackToTradeQtyForSingleOrder(t *testing.T) {
	t.Parallel()

	trade := model.Trade{
		Qty:        75,
		SLOrderIDs: []string{"sl-1"},
	}

	if got := slOrderQuantity(trade, "sl-1"); got != 75 {
		t.Fatalf("slOrderQuantity(single-order fallback) = %d, want 75", got)
	}
}
