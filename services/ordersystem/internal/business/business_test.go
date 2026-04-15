package business

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/AppsByZubin/botsquadron/services/ordersystem/internal/model"
)

type fakeService struct {
	accountReq         model.CreateAccountRequest
	accountCalls       int
	accountErr         error
	accountDetailsReq  model.GetAccountDetailsRequest
	accountDetailCalls int
	tradeReq           model.CreateTradeRequest
	tradeCalls         int
}

func (f *fakeService) CreateAccount(_ context.Context, req model.CreateAccountRequest) (model.AccountResponse, error) {
	f.accountCalls++
	f.accountReq = req
	if f.accountErr != nil {
		return model.AccountResponse{}, f.accountErr
	}
	return model.AccountResponse{AccountID: "acct-1"}, nil
}

func (f *fakeService) CreateTrade(_ context.Context, req model.CreateTradeRequest) (model.CreateTradeResponse, error) {
	f.tradeCalls++
	f.tradeReq = req
	return model.CreateTradeResponse{TradeID: "trade-1"}, nil
}

func (f *fakeService) GetAccountDetails(_ context.Context, req model.GetAccountDetailsRequest) (model.AccountDetailsResponse, error) {
	f.accountDetailCalls++
	f.accountDetailsReq = req
	return model.AccountDetailsResponse{AccountID: "acct-1"}, nil
}

func (f *fakeService) GetTradeByID(context.Context, string) (model.Trade, error) {
	return model.Trade{}, nil
}

func (f *fakeService) ModifyTrade(context.Context, string, model.ModifyTradeRequest) (model.ModifyTradeResponse, error) {
	return model.ModifyTradeResponse{}, nil
}

func TestCreateTradeEnsuresAccountFirst(t *testing.T) {
	t.Parallel()

	initCash := 100000.0
	svc := &fakeService{}
	biz := New(svc)

	resp, err := biz.CreateTrade(context.Background(), model.CreateTradeRequest{
		BotName:   "bot-a",
		CurrDate:  "14-04-2026",
		MonthYear: "042026",
		InitCash:  &initCash,
		Symbol:    "NIFTY",
	})
	if err != nil {
		t.Fatalf("CreateTrade returned error: %v", err)
	}
	if resp.TradeID != "trade-1" {
		t.Fatalf("trade id = %s, want trade-1", resp.TradeID)
	}
	if svc.accountCalls != 1 {
		t.Fatalf("account calls = %d, want 1", svc.accountCalls)
	}
	if svc.tradeCalls != 1 {
		t.Fatalf("trade calls = %d, want 1", svc.tradeCalls)
	}
	if svc.accountReq.BotName != "bot-a" || svc.accountReq.CurrDate != "14-04-2026" || svc.accountReq.MonthYear != "042026" || svc.accountReq.InitCash != &initCash {
		t.Fatalf("account request = %#v, want bot/date/month/init cash from trade request", svc.accountReq)
	}
}

func TestCreateTradeStopsWhenAccountEnsureFails(t *testing.T) {
	t.Parallel()

	svc := &fakeService{accountErr: errors.New("database offline")}
	biz := New(svc)

	_, err := biz.CreateTrade(context.Background(), model.CreateTradeRequest{BotName: "bot-a"})
	if err == nil || !strings.Contains(err.Error(), "ensure account for trade") {
		t.Fatalf("error = %v, want account ensure error", err)
	}
	if svc.tradeCalls != 0 {
		t.Fatalf("trade calls = %d, want 0", svc.tradeCalls)
	}
}

func TestGetAccountDetailsPassesThrough(t *testing.T) {
	t.Parallel()

	svc := &fakeService{}
	biz := New(svc)

	resp, err := biz.GetAccountDetails(context.Background(), model.GetAccountDetailsRequest{
		BotName:  "bot-a",
		CurrDate: "14-04-2026",
	})
	if err != nil {
		t.Fatalf("GetAccountDetails returned error: %v", err)
	}
	if resp.AccountID != "acct-1" {
		t.Fatalf("account id = %s, want acct-1", resp.AccountID)
	}
	if svc.accountDetailCalls != 1 {
		t.Fatalf("account detail calls = %d, want 1", svc.accountDetailCalls)
	}
	if svc.accountDetailsReq.BotName != "bot-a" || svc.accountDetailsReq.CurrDate != "14-04-2026" {
		t.Fatalf("account details request = %#v", svc.accountDetailsReq)
	}
}
