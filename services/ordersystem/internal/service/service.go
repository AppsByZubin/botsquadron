package service

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/AppsByZubin/botsquadron/services/ordersystem/internal/config"
	"github.com/AppsByZubin/botsquadron/services/ordersystem/internal/model"
	"github.com/AppsByZubin/botsquadron/services/ordersystem/internal/store"
	"github.com/AppsByZubin/botsquadron/services/ordersystem/internal/upstox"
)

type Service struct {
	cfg    config.Config
	store  *store.Store
	upstox *upstox.Client
}

func New(cfg config.Config, st *store.Store, upClient *upstox.Client) *Service {
	return &Service{
		cfg:    cfg,
		store:  st,
		upstox: upClient,
	}
}

func (s *Service) CreateTrade(ctx context.Context, req model.CreateTradeRequest) (model.CreateTradeResponse, error) {
	if strings.TrimSpace(req.Symbol) == "" {
		return model.CreateTradeResponse{}, fmt.Errorf("symbol is required")
	}
	if strings.TrimSpace(req.InstrumentToken) == "" {
		return model.CreateTradeResponse{}, fmt.Errorf("instrument_token is required")
	}
	if req.Qty <= 0 {
		return model.CreateTradeResponse{}, fmt.Errorf("qty must be > 0")
	}

	mode := strings.TrimSpace(req.Mode)
	if mode == "" {
		mode = s.cfg.AppMode
	}

	side := strings.ToUpper(strings.TrimSpace(req.Side))
	if side == "" {
		side = "BUY"
	}
	if side != "BUY" && side != "SELL" {
		return model.CreateTradeResponse{}, fmt.Errorf("side must be BUY or SELL")
	}

	product := strings.ToUpper(strings.TrimSpace(req.Product))
	if product == "" {
		product = "D"
	}
	validity := strings.ToUpper(strings.TrimSpace(req.Validity))
	if validity == "" {
		validity = "DAY"
	}

	slice := true
	if req.Slice != nil {
		slice = *req.Slice
	}

	status := "OPEN"
	entryOrderIDs := make([]string, 0, 1)
	slOrderIDs := make([]string, 0, 1)
	slOrderQtyMap := make(map[string]int)

	isProduction := strings.EqualFold(mode, "production")
	if isProduction {
		if s.upstox == nil || !s.upstox.Enabled() {
			return model.CreateTradeResponse{}, fmt.Errorf("production mode is enabled but upstox client is not configured")
		}

		entryResp, err := s.upstox.PlaceOrder(ctx, upstox.PlaceOrderRequest{
			Quantity:        req.Qty,
			Product:         product,
			Validity:        validity,
			Price:           0,
			Tag:             req.TagEntry,
			InstrumentToken: req.InstrumentToken,
			OrderType:       "MARKET",
			TransactionType: side,
			DisclosedQty:    0,
			TriggerPrice:    0,
			IsAMO:           req.IsAMO,
			Slice:           slice,
		})
		if err != nil {
			return model.CreateTradeResponse{}, fmt.Errorf("place entry order: %w", err)
		}
		entryOrderIDs = append(entryOrderIDs, entryResp.OrderID)

		if req.Stoploss != nil && *req.Stoploss > 0 {
			slLimit := *req.Stoploss
			if req.SLLimit != nil && *req.SLLimit > 0 {
				slLimit = *req.SLLimit
			}

			slTxnType := oppositeSide(side)
			slResp, err := s.upstox.PlaceOrder(ctx, upstox.PlaceOrderRequest{
				Quantity:        req.Qty,
				Product:         product,
				Validity:        validity,
				Price:           slLimit,
				Tag:             req.TagSL,
				InstrumentToken: req.InstrumentToken,
				OrderType:       "SL",
				TransactionType: slTxnType,
				DisclosedQty:    0,
				TriggerPrice:    *req.Stoploss,
				IsAMO:           req.IsAMO,
				Slice:           slice,
			})
			if err != nil {
				return model.CreateTradeResponse{}, fmt.Errorf("place stoploss order: %w", err)
			}
			slOrderIDs = append(slOrderIDs, slResp.OrderID)
			slOrderQtyMap[slResp.OrderID] = req.Qty
		}
	}

	tradeID, err := s.store.CreateTrade(ctx, store.CreateTradeParams{
		BotName:         req.BotName,
		Symbol:          req.Symbol,
		InstrumentToken: req.InstrumentToken,
		Side:            side,
		Qty:             req.Qty,
		Product:         product,
		Validity:        validity,
		EntryOrderIDs:   entryOrderIDs,
		SLOrderIDs:      slOrderIDs,
		TargetOrderID:   nil,
		EntryPrice:      req.EntryPrice,
		Target:          req.Target,
		Stoploss:        req.Stoploss,
		SLLimit:         req.SLLimit,
		TSLActive:       req.TSLActive,
		StartTrailAfter: req.StartTrailAfter,
		EntrySpot:       req.EntrySpot,
		SpotLTP:         req.SpotLTP,
		TrailPoints:     req.TrailPoints,
		Status:          status,
		Taxes:           req.Taxes,
		TagEntry:        req.TagEntry,
		TagSL:           req.TagSL,
		Description:     req.Description,
		SLOrderQtyMap:   slOrderQtyMap,
	})
	if err != nil {
		return model.CreateTradeResponse{}, err
	}

	message := "trade created"
	if isProduction {
		message = "trade created and orders placed on upstox"
	}

	return model.CreateTradeResponse{
		TradeID:       tradeID,
		Status:        status,
		EntryOrderIDs: entryOrderIDs,
		SLOrderIDs:    slOrderIDs,
		Message:       message,
	}, nil
}

func (s *Service) GetTradeByID(ctx context.Context, tradeID string) (model.Trade, error) {
	return s.store.GetTradeByID(ctx, tradeID)
}

func (s *Service) PollStopLossOrders(ctx context.Context) error {
	if !s.cfg.IsProduction() {
		return nil
	}
	if s.upstox == nil || !s.upstox.Enabled() {
		return nil
	}

	trades, err := s.store.ListOpenTradesForSLPolling(ctx)
	if err != nil {
		return err
	}
	if len(trades) == 0 {
		return nil
	}

	for _, trade := range trades {
		for _, slOrderID := range trade.SLOrderIDs {
			statusResp, err := s.upstox.GetOrderStatus(ctx, slOrderID)
			if err != nil {
				log.Printf("poll sl order status failed for trade_id=%s order_id=%s: %v", trade.ID, slOrderID, err)
				continue
			}
			if !upstox.IsTerminalOrderStatus(statusResp.Status) {
				continue
			}

			if upstox.IsFilledOrderStatus(statusResp.Status) {
				exitPrice := trade.Stoploss
				if statusResp.AveragePrice != nil && *statusResp.AveragePrice > 0 {
					exitPrice = *statusResp.AveragePrice
				}
				if err := s.store.MarkTradeClosedFromSL(ctx, trade.ID, exitPrice, 0, "STOPLOSS HIT"); err != nil {
					log.Printf("close trade on SL failed for trade_id=%s: %v", trade.ID, err)
				}
			} else {
				dbStatus := "SL_" + strings.ToUpper(strings.ReplaceAll(strings.TrimSpace(statusResp.Status), " ", "_"))
				if err := s.store.UpdateTradeStatus(ctx, trade.ID, dbStatus); err != nil {
					log.Printf("update non-filled SL status failed for trade_id=%s status=%s: %v", trade.ID, dbStatus, err)
				}
			}
			break
		}
	}

	return nil
}

func oppositeSide(side string) string {
	if strings.EqualFold(strings.TrimSpace(side), "SELL") {
		return "BUY"
	}
	return "SELL"
}
