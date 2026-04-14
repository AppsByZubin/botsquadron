package service

import (
	"context"
	"fmt"
	"log"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/AppsByZubin/botsquadron/services/ordersystem/internal/config"
	"github.com/AppsByZubin/botsquadron/services/ordersystem/internal/model"
	"github.com/AppsByZubin/botsquadron/services/ordersystem/internal/store"
	"github.com/AppsByZubin/botsquadron/services/ordersystem/internal/upstox"
)

type Service struct {
	cfg           config.Config
	store         *store.Store
	upstox        *upstox.Client
	slRefreshMu   sync.Mutex
	slRefreshLast map[string]time.Time
}

func New(cfg config.Config, st *store.Store, upClient *upstox.Client) *Service {
	return &Service{
		cfg:           cfg,
		store:         st,
		upstox:        upClient,
		slRefreshLast: make(map[string]time.Time),
	}
}

func (s *Service) CreateAccount(ctx context.Context, req model.CreateAccountRequest) (model.AccountResponse, error) {
	if strings.TrimSpace(req.BotName) == "" {
		return model.AccountResponse{}, fmt.Errorf("bot_name is required")
	}
	if s.store == nil {
		return model.AccountResponse{}, fmt.Errorf("store is not configured")
	}

	account, err := s.store.CreateAccount(ctx, store.CreateAccountParams{
		BotName:   req.BotName,
		CurrDate:  req.CurrDate,
		MonthYear: req.MonthYear,
		InitCash:  req.InitCash,
	})
	if err != nil {
		return model.AccountResponse{}, err
	}

	return model.AccountResponse{
		AccountID: account.ID,
		BotName:   account.BotName,
		CurrDate:  account.CurrDate,
		MonthYear: account.MonthYear,
		InitCash:  account.InitCash,
		NetProfit: account.NetProfit,
		Message:   "account row ready",
	}, nil
}

func (s *Service) GetAccountDetails(ctx context.Context, req model.GetAccountDetailsRequest) (model.AccountDetailsResponse, error) {
	if strings.TrimSpace(req.BotName) == "" {
		return model.AccountDetailsResponse{}, fmt.Errorf("bot_name is required")
	}
	if strings.TrimSpace(req.CurrDate) == "" {
		return model.AccountDetailsResponse{}, fmt.Errorf("curr_date is required")
	}
	if s.store == nil {
		return model.AccountDetailsResponse{}, fmt.Errorf("store is not configured")
	}

	account, err := s.store.GetAccountByBotDate(ctx, req.BotName, req.CurrDate)
	if err != nil {
		return model.AccountDetailsResponse{}, err
	}

	trades, err := s.store.ListTradesByAccountID(ctx, account.ID)
	if err != nil {
		return model.AccountDetailsResponse{}, fmt.Errorf("load account trades: %w", err)
	}

	return model.AccountDetailsResponse{
		AccountID: account.ID,
		BotName:   account.BotName,
		CurrDate:  account.CurrDate,
		MonthYear: account.MonthYear,
		InitCash:  account.InitCash,
		NetProfit: account.NetProfit,
		Trades:    trades,
		Message:   "account details loaded",
	}, nil
}

func (s *Service) CreateTrade(ctx context.Context, req model.CreateTradeRequest) (model.CreateTradeResponse, error) {
	if strings.TrimSpace(req.BotName) == "" {
		return model.CreateTradeResponse{}, fmt.Errorf("bot_name is required")
	}
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
			IsAMO:           false,
			Slice:           true,
		})
		if err != nil {
			return model.CreateTradeResponse{}, fmt.Errorf("place entry order: %w", err)
		}
		entryOrderIDs = append(entryOrderIDs, entryResp.OrderIDs...)

		if req.SLTrigger != nil && *req.SLTrigger > 0 {
			slLimit := *req.SLTrigger
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
				TriggerPrice:    *req.SLTrigger,
				IsAMO:           req.IsAMO,
				Slice:           slice,
			})
			if err != nil {
				return model.CreateTradeResponse{}, fmt.Errorf("place stoploss order: %w", err)
			}
			slOrderIDs = append(slOrderIDs, slResp.OrderIDs...)
		}
	}

	accountID, err := s.store.GetAccountIDForBotDate(ctx, req.BotName, req.CurrDate)
	if err != nil {
		return model.CreateTradeResponse{}, fmt.Errorf("load account for trade: %w", err)
	}

	orders := make([]store.CreateOrderParams, 0, len(entryOrderIDs)+len(slOrderIDs)+2)
	orders = append(orders, buildOrderParams(entryOrderIDs, store.CreateOrderParams{
		InstrumentToken: req.InstrumentToken,
		OrderType:       "entry",
		EntryPrice:      req.EntryPrice,
		Target:          req.Target,
	})...)
	if req.SLTrigger != nil || req.SLLimit != nil {
		orders = append(orders, buildOrderParams(slOrderIDs, store.CreateOrderParams{
			InstrumentToken: req.InstrumentToken,
			OrderType:       "sl",
			Stoploss:        req.SLTrigger,
			SLLimit:         req.SLLimit,
		})...)
	}

	tradeID, err := s.store.CreateTrade(ctx, store.CreateTradeParams{
		AccountID:       accountID,
		Symbol:          req.Symbol,
		InstrumentToken: req.InstrumentToken,
		Side:            side,
		Qty:             req.Qty,
		Product:         product,
		Validity:        validity,
		TSLActive:       req.TSLActive,
		StartTrailAfter: req.StartTrailAfter,
		EntrySpot:       req.EntrySpot,
		SpotLTP:         req.SpotLTP,
		SpotTrailAnchor: req.SpotTrailAnchor,
		TrailPoints:     req.TrailPoints,
		Status:          status,
		TotalBrokerage:  req.TotalBrokerage,
		TagEntry:        req.TagEntry,
		TagSL:           req.TagSL,
		Description:     req.Description,
		Orders:          orders,
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

func (s *Service) ModifyTrade(ctx context.Context, tradeID string, req model.ModifyTradeRequest) (model.ModifyTradeResponse, error) {
	tradeID = strings.TrimSpace(tradeID)
	if tradeID == "" {
		return model.ModifyTradeResponse{}, fmt.Errorf("trade id is required")
	}
	validity := strings.ToUpper(strings.TrimSpace(req.Validity))
	if validity == "" {
		validity = "DAY"
	}

	orderType := strings.ToUpper(strings.TrimSpace(req.OrderType))
	if orderType == "" {
		orderType = "SL"
	}

	mode := strings.TrimSpace(req.Mode)
	if mode == "" {
		mode = s.cfg.AppMode
	}
	isProduction := strings.EqualFold(mode, "production")

	stoploss, slLimit, spotTrailAnchor := req.Stoploss, req.SLLimit, req.SpotTrailAnchor
	if err := validateModifyTradeRequest(req, stoploss, slLimit, spotTrailAnchor, validity, orderType); err != nil {
		return model.ModifyTradeResponse{}, err
	}
	if isProduction {
		if err := validateProductionModifyTradeRequest(orderType, stoploss, slLimit); err != nil {
			return model.ModifyTradeResponse{}, err
		}
	}

	if s.store == nil {
		return model.ModifyTradeResponse{}, fmt.Errorf("store is not configured")
	}

	trade, err := s.store.GetTradeByID(ctx, tradeID)
	if err != nil {
		return model.ModifyTradeResponse{}, fmt.Errorf("load trade: %w", err)
	}
	if err := validateModifiedTradeAgainstTrade(trade, stoploss, slLimit); err != nil {
		return model.ModifyTradeResponse{}, err
	}

	if !isProduction {
		if err := s.persistModifiedTradeState(ctx, tradeID, stoploss, slLimit, spotTrailAnchor); err != nil {
			return model.ModifyTradeResponse{}, err
		}
		return model.ModifyTradeResponse{
			TradeID:          tradeID,
			ModifiedOrderIDs: append([]string(nil), trade.SLOrderIDs...),
			Message:          "trade modification saved in db because app is not in production mode",
		}, nil
	}

	if s.upstox == nil || !s.upstox.Enabled() {
		return model.ModifyTradeResponse{}, fmt.Errorf("production mode is enabled but upstox client is not configured")
	}
	if len(trade.SLOrderIDs) == 0 {
		return model.ModifyTradeResponse{}, fmt.Errorf("trade has no stoploss orders to modify")
	}

	modifiedOrderIDs := make([]string, 0, len(trade.SLOrderIDs))
	failedOrderMessages := make([]string, 0)

	for _, slOrderID := range trade.SLOrderIDs {
		orderID := strings.TrimSpace(slOrderID)
		if orderID == "" {
			continue
		}

		qty := slOrderQuantity(trade, orderID)
		if qty <= 0 {
			failedOrderMessages = append(failedOrderMessages, fmt.Sprintf("%s: quantity missing", orderID))
			continue
		}

		if _, err := s.upstox.ModifyOrder(ctx, upstox.ModifyOrderRequest{
			Quantity:     qty,
			Validity:     validity,
			Price:        float64Value(slLimit),
			OrderID:      orderID,
			OrderType:    orderType,
			DisclosedQty: req.DisclosedQty,
			TriggerPrice: float64Value(stoploss),
		}); err != nil {
			failedOrderMessages = append(failedOrderMessages, fmt.Sprintf("%s: %v", orderID, err))
			continue
		}

		modifiedOrderIDs = append(modifiedOrderIDs, orderID)
	}

	if len(failedOrderMessages) > 0 {
		return model.ModifyTradeResponse{}, fmt.Errorf("modify trade partially failed for trade_id=%s: %s", tradeID, strings.Join(failedOrderMessages, "; "))
	}

	if err := s.persistModifiedTradeState(ctx, tradeID, stoploss, slLimit, spotTrailAnchor); err != nil {
		return model.ModifyTradeResponse{}, err
	}

	return model.ModifyTradeResponse{
		TradeID:          tradeID,
		ModifiedOrderIDs: modifiedOrderIDs,
		Message:          "trade stoploss orders modified on upstox",
	}, nil
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
		if !s.shouldRefreshStopLossTrade(trade.ID) {
			continue
		}
		s.refreshStopLossTrade(ctx, trade)
	}

	return nil
}

func (s *Service) shouldRefreshStopLossTrade(tradeID string) bool {
	interval := s.cfg.SLRefreshMinInterval
	if interval <= 0 {
		return true
	}

	now := time.Now()
	s.slRefreshMu.Lock()
	defer s.slRefreshMu.Unlock()

	last := s.slRefreshLast[tradeID]
	if !last.IsZero() && now.Sub(last) < interval {
		return false
	}
	s.slRefreshLast[tradeID] = now
	return true
}

func (s *Service) refreshStopLossTrade(ctx context.Context, trade model.TradeForSLPolling) {
	for _, slOrder := range trade.SLOrders {
		orderID := strings.TrimSpace(slOrder.OrderID)
		if orderID == "" {
			continue
		}

		tradesResp, err := s.upstox.GetOrderTrades(ctx, orderID)
		if err != nil {
			if upstox.IsRateLimited(err) {
				log.Printf("poll sl order trades rate-limited for trade_id=%s order_id=%s: %v", trade.ID, orderID, err)
				continue
			} else {
				log.Printf("poll sl order trades failed for trade_id=%s order_id=%s: %v", trade.ID, orderID, err)
			}
		} else if tradesResp.Filled {
			s.closeTradeFromStopLoss(ctx, trade, orderID, slOrder.Stoploss, tradesResp.AveragePrice)
			return
		} else if upstox.IsTerminalOrderStatus(tradesResp.Status) {
			s.handleTerminalUnfilledStopLoss(ctx, trade.ID, orderID, tradesResp.Status)
			return
		}

		statusResp, err := s.upstox.GetOrderStatus(ctx, orderID)
		if err != nil {
			if upstox.IsRateLimited(err) {
				log.Printf("poll sl order status rate-limited for trade_id=%s order_id=%s: %v", trade.ID, orderID, err)
			} else {
				log.Printf("poll sl order status failed for trade_id=%s order_id=%s: %v", trade.ID, orderID, err)
			}
			continue
		}
		if !upstox.IsTerminalOrderStatus(statusResp.Status) {
			continue
		}

		if upstox.IsFilledOrderStatus(statusResp.Status) {
			s.closeTradeFromStopLoss(ctx, trade, orderID, slOrder.Stoploss, statusResp.AveragePrice)
			return
		}

		s.handleTerminalUnfilledStopLoss(ctx, trade.ID, orderID, statusResp.Status)
		return
	}
}

func (s *Service) closeTradeFromStopLoss(ctx context.Context, trade model.TradeForSLPolling, orderID string, fallbackExitPrice float64, averagePrice *float64) {
	exitPrice := fallbackExitPrice
	if averagePrice != nil && *averagePrice > 0 {
		exitPrice = *averagePrice
	}
	if err := s.store.MarkTradeClosedFromSL(ctx, trade.ID, orderID, exitPrice, 0, "STOPLOSS HIT"); err != nil {
		log.Printf("close trade on SL failed for trade_id=%s: %v", trade.ID, err)
	}
}

func (s *Service) handleTerminalUnfilledStopLoss(ctx context.Context, tradeID string, orderID string, status string) {
	if err := s.store.DisableTrailingByTradeID(ctx, tradeID); err != nil {
		log.Printf("disable trailing after terminal SL failed for trade_id=%s order_id=%s status=%s: %v", tradeID, orderID, status, err)
		return
	}
	log.Printf("sl order terminal but not filled; disabled trailing and kept trade open trade_id=%s order_id=%s status=%s", tradeID, orderID, status)
}

func validateModifyTradeRequest(req model.ModifyTradeRequest, stoploss *float64, slLimit *float64, spotTrailAnchor *float64, validity string, orderType string) error {
	if req.DisclosedQty < 0 {
		return fmt.Errorf("disclosed_quantity must be >= 0")
	}

	if validity != "DAY" && validity != "IOC" {
		return fmt.Errorf("validity must be DAY or IOC")
	}

	if orderType != "SL" && orderType != "SL-M" {
		return fmt.Errorf("order_type must be SL or SL-M")
	}

	if stoploss == nil && slLimit == nil && spotTrailAnchor == nil {
		return fmt.Errorf("at least one of stoploss, sl_limit, or spot_trail_anchor is required")
	}

	if err := validatePositiveFloatPtr("stoploss", stoploss); err != nil {
		return err
	}
	if err := validatePositiveFloatPtr("sl_limit", slLimit); err != nil {
		return err
	}
	if err := validatePositiveFloatPtr("spot_trail_anchor", spotTrailAnchor); err != nil {
		return err
	}

	return nil
}

func validateProductionModifyTradeRequest(orderType string, stoploss *float64, slLimit *float64) error {
	if stoploss == nil {
		return fmt.Errorf("stoploss is required in production mode")
	}
	if orderType == "SL" && slLimit == nil {
		return fmt.Errorf("sl_limit is required for SL order modification in production mode")
	}
	return nil
}

func validateModifiedTradeAgainstTrade(trade model.Trade, stoploss *float64, slLimit *float64) error {
	if stoploss == nil || slLimit == nil {
		return nil
	}

	side := strings.ToUpper(strings.TrimSpace(trade.Side))
	if side == "SELL" {
		if *slLimit <= *stoploss {
			return fmt.Errorf("sl_limit must be greater than stoploss for SELL trades")
		}
		return nil
	}

	if *slLimit >= *stoploss {
		return fmt.Errorf("sl_limit must be less than stoploss for BUY trades")
	}
	return nil
}

func validatePositiveFloatPtr(name string, value *float64) error {
	if value == nil {
		return nil
	}
	if math.IsNaN(*value) || math.IsInf(*value, 0) || *value <= 0 {
		return fmt.Errorf("%s must be > 0", name)
	}
	return nil
}

func (s *Service) persistModifiedTradeState(ctx context.Context, tradeID string, stoploss *float64, slLimit *float64, spotTrailAnchor *float64) error {
	if stoploss == nil && slLimit == nil && spotTrailAnchor == nil {
		return nil
	}
	if s.store == nil {
		return fmt.Errorf("store is not configured")
	}
	if err := s.store.UpdateTrailingStateByTradeID(ctx, tradeID, stoploss, slLimit, spotTrailAnchor); err != nil {
		return err
	}
	return nil
}

func buildOrderParams(orderIDs []string, base store.CreateOrderParams) []store.CreateOrderParams {
	if len(orderIDs) == 0 {
		return []store.CreateOrderParams{base}
	}

	orders := make([]store.CreateOrderParams, 0, len(orderIDs))
	for _, orderID := range orderIDs {
		orderID = strings.TrimSpace(orderID)
		if orderID == "" {
			continue
		}
		order := base
		order.OrderID = orderID
		orders = append(orders, order)
	}
	if len(orders) == 0 {
		return []store.CreateOrderParams{base}
	}
	return orders
}

func float64Value(value *float64) float64 {
	if value == nil {
		return 0
	}
	return *value
}

func slOrderQuantity(trade model.Trade, orderID string) int {
	if strings.TrimSpace(orderID) != "" && trade.Qty > 0 {
		return trade.Qty
	}
	return 0
}

func oppositeSide(side string) string {
	if strings.EqualFold(strings.TrimSpace(side), "SELL") {
		return "BUY"
	}
	return "SELL"
}
