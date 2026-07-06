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
	modifyErr     error
	createResp    model.CreateTradeResponse
	blockBotName  *string
	resumeBotName *string
	statusBotName *string
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

func (f fakeBusiness) RefreshTradeBrokerStatus(context.Context, string) (model.Trade, error) {
	return model.Trade{}, nil
}

func (f fakeBusiness) KillBot(context.Context, string, model.KillBotRequest) (model.BotKillSwitchResponse, error) {
	return model.BotKillSwitchResponse{}, nil
}

func (f fakeBusiness) BlockBotOrders(_ context.Context, botName string, _ model.BlockBotOrdersRequest) (model.BotKillSwitchResponse, error) {
	if f.blockBotName != nil {
		*f.blockBotName = botName
	}
	return model.BotKillSwitchResponse{BotName: botName, Status: model.OrderBlockStatus, Message: model.OrderBlockMessage}, nil
}

func (f fakeBusiness) ResumeBot(_ context.Context, botName string, _ model.ResumeBotRequest) (model.BotKillSwitchResponse, error) {
	if f.resumeBotName != nil {
		*f.resumeBotName = botName
	}
	return model.BotKillSwitchResponse{BotName: botName, Status: "RESUMED"}, nil
}

func (f fakeBusiness) GetBotKillSwitch(_ context.Context, botName string) (model.BotKillSwitchResponse, error) {
	if f.statusBotName != nil {
		*f.statusBotName = botName
	}
	return model.BotKillSwitchResponse{BotName: botName}, nil
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
			Reason:  "threshold_day_loss reached for 06-07-2026: day_loss=3500.75 threshold=3500.00 realized_pnl=-3500.75",
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
	if !strings.Contains(rec.Body.String(), "threshold_day_loss reached") {
		t.Fatalf("body = %s, want kill-mode reason", rec.Body.String())
	}
}

func TestHandleBlockBotOrders(t *testing.T) {
	t.Parallel()

	handler := New(fakeBusiness{}, 5*time.Second)
	req := httptest.NewRequest(
		http.MethodPost,
		"/v1/bots/bot-a/block-orders",
		strings.NewReader(`{"reason":"manual pause"}`),
	)
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), model.OrderBlockStatus) {
		t.Fatalf("body = %s, want order block status", rec.Body.String())
	}
}

func TestHandleFibobotOrderIntakeRoutes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		method    string
		path      string
		body      string
		captureFn func(*fakeBusiness, *string)
	}{
		{
			name:   "block",
			method: http.MethodPost,
			path:   "/v1/bots/fibobot/block-orders",
			body:   `{"reason":"manual pause"}`,
			captureFn: func(f *fakeBusiness, got *string) {
				f.blockBotName = got
			},
		},
		{
			name:   "status",
			method: http.MethodGet,
			path:   "/v1/bots/fibobot/block-orders",
			captureFn: func(f *fakeBusiness, got *string) {
				f.statusBotName = got
			},
		},
		{
			name:   "resume",
			method: http.MethodPost,
			path:   "/v1/bots/fibobot/resume",
			body:   `{"reason":"resume order intake"}`,
			captureFn: func(f *fakeBusiness, got *string) {
				f.resumeBotName = got
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			gotBotName := ""
			fake := fakeBusiness{}
			tt.captureFn(&fake, &gotBotName)
			handler := New(fake, 5*time.Second)

			req := httptest.NewRequest(tt.method, tt.path, strings.NewReader(tt.body))
			rec := httptest.NewRecorder()

			handler.Routes().ServeHTTP(rec, req)

			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
			}
			if gotBotName != "fibobot" {
				t.Fatalf("botName = %q, want fibobot", gotBotName)
			}
		})
	}
}
