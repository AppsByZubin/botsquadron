package httpapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/AppsByZubin/botsquadron/services/ordersystem/internal/model"
	"github.com/AppsByZubin/botsquadron/services/ordersystem/internal/upstox"
)

type fakeBusiness struct {
	modifyErr  error
	createResp model.CreateTradeResponse
}

func (f fakeBusiness) CreateAccount(context.Context, model.CreateAccountRequest) (model.AccountResponse, error) {
	return model.AccountResponse{}, nil
}

func (f fakeBusiness) CreateTrade(context.Context, model.CreateTradeRequest) (model.CreateTradeResponse, error) {
	return f.createResp, nil
}

func (f fakeBusiness) GetAccountDetails(context.Context, model.GetAccountDetailsRequest) (model.AccountDetailsResponse, error) {
	return model.AccountDetailsResponse{}, nil
}

func (f fakeBusiness) GetTradeByID(context.Context, string) (model.Trade, error) {
	return model.Trade{}, nil
}

func (f fakeBusiness) KillBot(context.Context, string, model.KillBotRequest) (model.BotKillSwitchResponse, error) {
	return model.BotKillSwitchResponse{}, nil
}

func (f fakeBusiness) ResumeBot(context.Context, string, model.ResumeBotRequest) (model.BotKillSwitchResponse, error) {
	return model.BotKillSwitchResponse{}, nil
}

func (f fakeBusiness) GetBotKillSwitch(context.Context, string) (model.BotKillSwitchResponse, error) {
	return model.BotKillSwitchResponse{}, nil
}

func (f fakeBusiness) ModifyTrade(context.Context, string, model.ModifyTradeRequest) (model.ModifyTradeResponse, error) {
	if f.modifyErr != nil {
		return model.ModifyTradeResponse{}, f.modifyErr
	}
	return model.ModifyTradeResponse{TradeID: "trade-1"}, nil
}

func (f fakeBusiness) SquareOffTrade(context.Context, string, model.SquareOffTradeRequest) (model.SquareOffTradeResponse, error) {
	return model.SquareOffTradeResponse{}, nil
}

func TestHandleModifyTradeReturns429ForRateLimit(t *testing.T) {
	t.Parallel()

	handler := New(fakeBusiness{
		modifyErr: upstox.RateLimitedError{
			Operation: "modify order",
			OrderID:   "sl-1",
			RetryAt:   time.Now().Add(2 * time.Second),
		},
	}, 5*time.Second)

	req := httptest.NewRequest(
		http.MethodPost,
		"/v1/trades/trade-1/modify",
		strings.NewReader(`{"stoploss":91,"sl_limit":90.5}`),
	)
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429; body=%s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Retry-After"); got == "" {
		t.Fatalf("Retry-After header is empty, want retry guidance")
	}
}

func TestHandleCreateTradeReturnsOKForKillMode(t *testing.T) {
	t.Parallel()

	handler := New(fakeBusiness{
		createResp: model.CreateTradeResponse{
			Status:  model.KillModeStatus,
			Message: model.KillModeMessage,
		},
	}, 5*time.Second)

	req := httptest.NewRequest(
		http.MethodPost,
		"/v1/trades",
		strings.NewReader(`{"bot_name":"bot-a","symbol":"NIFTY","instrument_token":"NSE_FO|1","qty":75}`),
	)
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), model.KillModeMessage) {
		t.Fatalf("body = %s, want kill-mode message", rec.Body.String())
	}
}
