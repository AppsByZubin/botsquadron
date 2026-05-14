package business

import (
	"context"
	"fmt"
	"strings"

	"github.com/AppsByZubin/botsquadron/services/ordersystem/internal/model"
)

type Service interface {
	CreateAccount(context.Context, model.CreateAccountRequest) (model.AccountResponse, error)
	CreateTrade(context.Context, model.CreateTradeRequest) (model.CreateTradeResponse, error)
	GetAccountDetails(context.Context, model.GetAccountDetailsRequest) (model.AccountDetailsResponse, error)
	GetTradeByID(context.Context, string) (model.Trade, error)
	KillBot(context.Context, string, model.KillBotRequest) (model.BotKillSwitchResponse, error)
	ResumeBot(context.Context, string, model.ResumeBotRequest) (model.BotKillSwitchResponse, error)
	GetBotKillSwitch(context.Context, string) (model.BotKillSwitchResponse, error)
	ModifyTrade(context.Context, string, model.ModifyTradeRequest) (model.ModifyTradeResponse, error)
	SquareOffTrade(context.Context, string, model.SquareOffTradeRequest) (model.SquareOffTradeResponse, error)
}

type Business struct {
	svc Service
}

func New(svc Service) *Business {
	return &Business{svc: svc}
}

func (b *Business) CreateAccount(ctx context.Context, req model.CreateAccountRequest) (model.AccountResponse, error) {
	return b.svc.CreateAccount(ctx, req)
}

func (b *Business) CreateTrade(ctx context.Context, req model.CreateTradeRequest) (model.CreateTradeResponse, error) {
	if strings.TrimSpace(req.BotName) != "" {
		_, err := b.svc.CreateAccount(ctx, model.CreateAccountRequest{
			BotName:   req.BotName,
			CurrDate:  req.CurrDate,
			MonthYear: req.MonthYear,
			InitCash:  req.InitCash,
		})
		if err != nil {
			return model.CreateTradeResponse{}, fmt.Errorf("ensure account for trade: %w", err)
		}
	}

	return b.svc.CreateTrade(ctx, req)
}

func (b *Business) GetAccountDetails(ctx context.Context, req model.GetAccountDetailsRequest) (model.AccountDetailsResponse, error) {
	return b.svc.GetAccountDetails(ctx, req)
}

func (b *Business) GetTradeByID(ctx context.Context, tradeID string) (model.Trade, error) {
	return b.svc.GetTradeByID(ctx, tradeID)
}

func (b *Business) KillBot(ctx context.Context, botName string, req model.KillBotRequest) (model.BotKillSwitchResponse, error) {
	return b.svc.KillBot(ctx, botName, req)
}

func (b *Business) ResumeBot(ctx context.Context, botName string, req model.ResumeBotRequest) (model.BotKillSwitchResponse, error) {
	return b.svc.ResumeBot(ctx, botName, req)
}

func (b *Business) GetBotKillSwitch(ctx context.Context, botName string) (model.BotKillSwitchResponse, error) {
	return b.svc.GetBotKillSwitch(ctx, botName)
}

func (b *Business) ModifyTrade(ctx context.Context, tradeID string, req model.ModifyTradeRequest) (model.ModifyTradeResponse, error) {
	return b.svc.ModifyTrade(ctx, tradeID, req)
}

func (b *Business) SquareOffTrade(ctx context.Context, tradeID string, req model.SquareOffTradeRequest) (model.SquareOffTradeResponse, error) {
	return b.svc.SquareOffTrade(ctx, tradeID, req)
}
