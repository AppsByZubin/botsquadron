package service

import (
	"context"
	"errors"
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
	dayLossMu     sync.Mutex
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

	existingAccount, err := s.store.GetAccountByBotMonthYear(ctx, req.BotName, req.MonthYear)
	if err == nil {
		return accountResponse(existingAccount, "Account already created"), nil
	}
	if !errors.Is(err, store.ErrAccountNotFound) {
		return model.AccountResponse{}, err
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

	return accountResponse(account, "account row ready"), nil
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
	if s.store == nil {
		return model.CreateTradeResponse{}, fmt.Errorf("store is not configured")
	}
	killState, err := s.effectiveBotKillSwitch(ctx, req.BotName)
	if err != nil {
		return model.CreateTradeResponse{}, err
	}
	if killState.KillEnabled {
		closedTrades, err := s.store.ListClosedTradesByBotDate(ctx, req.BotName, req.CurrDate)
		if err != nil {
			return model.CreateTradeResponse{}, err
		}
		return model.CreateTradeResponse{
			Status:       model.KillModeStatus,
			Message:      model.KillModeMessage,
			Reason:       killState.Reason,
			ClosedTrades: closedTrades,
			ClosedOrders: closedTrades,
		}, nil
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
	mode, err = normalizeRuntimeMode(mode)
	if err != nil {
		return model.CreateTradeResponse{}, err
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
	entryOrders := make([]model.OrderRef, 0, 1)
	slOrders := make([]model.OrderRef, 0, 1)

	if s.upstox == nil || !s.upstox.Enabled() {
		return model.CreateTradeResponse{}, fmt.Errorf("%s mode is enabled but upstox client is not configured", mode)
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
	entryOrders = orderRefsFromUpstox(entryResp.Orders)
	if len(entryOrders) == 0 {
		entryOrders = orderRefsFromIDs(entryResp.OrderIDs)
	}
	entryOrders = s.hydrateOrderExchangeIDs(ctx, entryOrders)
	entryOrderIDs = orderIDsFromModelRefs(entryOrders)

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
		slOrders = orderRefsFromUpstox(slResp.Orders)
		if len(slOrders) == 0 {
			slOrders = orderRefsFromIDs(slResp.OrderIDs)
		}
		slOrders = s.hydrateOrderExchangeIDs(ctx, slOrders)
		slOrderIDs = orderIDsFromModelRefs(slOrders)
	}

	accountID, err := s.store.GetAccountIDForBotDate(ctx, req.BotName, req.CurrDate)
	if err != nil {
		if strings.TrimSpace(req.MonthYear) == "" || !errors.Is(err, store.ErrAccountNotFound) {
			return model.CreateTradeResponse{}, fmt.Errorf("load account for trade: %w", err)
		}

		account, monthErr := s.store.GetAccountByBotMonthYear(ctx, req.BotName, req.MonthYear)
		if monthErr != nil {
			return model.CreateTradeResponse{}, fmt.Errorf("load account for trade by month_year: %w", monthErr)
		}
		accountID = account.ID
	}

	orders := make([]store.CreateOrderParams, 0, len(entryOrderIDs)+len(slOrderIDs)+2)
	orders = append(orders, buildOrderParams(entryOrders, store.CreateOrderParams{
		InstrumentToken: req.InstrumentToken,
		OrderType:       "entry",
		EntryPrice:      req.EntryPrice,
		Target:          req.Target,
	})...)
	if req.SLTrigger != nil || req.SLLimit != nil {
		orders = append(orders, buildOrderParams(slOrders, store.CreateOrderParams{
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

	message := fmt.Sprintf("trade created and orders placed on upstox (%s)", mode)

	return model.CreateTradeResponse{
		TradeID:       tradeID,
		Status:        status,
		EntryOrderIDs: entryOrderIDs,
		SLOrderIDs:    slOrderIDs,
		EntryOrders:   entryOrders,
		SLOrders:      slOrders,
		Message:       message,
	}, nil
}

func accountResponse(account model.Account, message string) model.AccountResponse {
	return model.AccountResponse{
		AccountID: account.ID,
		BotName:   account.BotName,
		CurrDate:  account.CurrDate,
		MonthYear: account.MonthYear,
		InitCash:  account.InitCash,
		NetProfit: account.NetProfit,
		Message:   message,
	}
}

func (s *Service) GetTradeByID(ctx context.Context, tradeID string) (model.Trade, error) {
	return s.store.GetTradeByID(ctx, tradeID)
}

func (s *Service) RefreshTradeBrokerStatus(ctx context.Context, tradeID string) (model.Trade, error) {
	tradeID = strings.TrimSpace(tradeID)
	if tradeID == "" {
		return model.Trade{}, fmt.Errorf("trade id is required")
	}
	if s.store == nil {
		return model.Trade{}, fmt.Errorf("store is not configured")
	}

	trade, err := s.store.GetTradeByID(ctx, tradeID)
	if err != nil {
		return model.Trade{}, err
	}
	if isClosedTradeStatus(trade.Status) {
		return trade, nil
	}
	if !s.cfg.IsProduction() || s.upstox == nil || !s.upstox.Enabled() {
		return trade, nil
	}

	pollingTrade := tradeForStopLossSync(trade)
	s.syncEntryOrders(ctx, pollingTrade)
	for _, slOrder := range pollingTrade.SLOrders {
		orderID := strings.TrimSpace(slOrder.OrderID)
		if orderID == "" {
			continue
		}
		s.syncStopLossTerminalStateDetailed(ctx, trade, orderID, "manual refresh")
	}

	return s.store.GetTradeByID(ctx, tradeID)
}

func (s *Service) KillBot(ctx context.Context, botName string, req model.KillBotRequest) (model.BotKillSwitchResponse, error) {
	botName = strings.TrimSpace(botName)
	if botName == "" {
		return model.BotKillSwitchResponse{}, fmt.Errorf("bot_name is required")
	}
	if s.store == nil {
		return model.BotKillSwitchResponse{}, fmt.Errorf("store is not configured")
	}
	if strings.TrimSpace(req.Mode) != "" {
		if _, err := normalizeRuntimeMode(req.Mode); err != nil {
			return model.BotKillSwitchResponse{}, err
		}
	}

	reason := strings.TrimSpace(req.Reason)
	if reason == "" {
		reason = model.KillSwitchExitStatus
	}
	state, err := s.store.SetBotKillSwitch(ctx, botName, true, reason)
	if err != nil {
		return model.BotKillSwitchResponse{}, err
	}

	openTrades, err := s.store.ListOpenTradesByBotDate(ctx, botName, req.CurrDate)
	if err != nil {
		return model.BotKillSwitchResponse{}, err
	}

	response := model.BotKillSwitchResponse{
		BotName:     state.BotName,
		CurrDate:    strings.TrimSpace(req.CurrDate),
		KillEnabled: state.KillEnabled,
		Status:      model.KillModeStatus,
		Message:     "kill switch enabled",
		Reason:      state.Reason,
		Segment:     strings.TrimSpace(req.Segment),
		UpdatedAt:   state.UpdatedAt,
	}

	if len(openTrades) == 0 {
		response.Message = "kill switch enabled; no open trades found"
		return response, nil
	}

	tags := killPositionTags(botName, req.Tag, openTrades)
	response.Tags = tags

	if s.upstox == nil || !s.upstox.Enabled() {
		response.Errors = append(response.Errors, "upstox client is not configured; kill mode is enabled but broker orders were not changed")
	} else {
		cancelledSLOrderIDs, cancelErrors := s.cancelBotStopLossOrders(ctx, openTrades)
		response.CancelledSLOrderIDs = cancelledSLOrderIDs
		response.Errors = append(response.Errors, cancelErrors...)

		exitOrderIDs, exitErrors := s.exitBotPositionsByTag(ctx, req.Segment, tags)
		response.ExitOrderIDs = exitOrderIDs
		response.Errors = append(response.Errors, exitErrors...)
	}

	closedTrades, err := s.store.MarkOpenTradesKilled(ctx, botName, req.CurrDate, response.ExitOrderIDs, time.Now(), model.KillSwitchExitStatus)
	if err != nil {
		return model.BotKillSwitchResponse{}, err
	}
	response.ClosedTrades = closedTrades
	response.ClosedOrders = closedTrades
	response.Message = "kill switch enabled; stoploss orders cancelled and positions exit requested"
	if len(response.Errors) > 0 {
		response.Message = "kill switch enabled with broker errors"
	}
	return response, nil
}

func (s *Service) BlockBotOrders(ctx context.Context, botName string, req model.BlockBotOrdersRequest) (model.BotKillSwitchResponse, error) {
	botName = strings.TrimSpace(botName)
	if botName == "" {
		return model.BotKillSwitchResponse{}, fmt.Errorf("bot_name is required")
	}
	if s.store == nil {
		return model.BotKillSwitchResponse{}, fmt.Errorf("store is not configured")
	}

	reason := strings.TrimSpace(req.Reason)
	if reason == "" {
		reason = model.OrderBlockStatus
	}
	state, err := s.store.SetBotKillSwitch(ctx, botName, true, reason)
	if err != nil {
		return model.BotKillSwitchResponse{}, err
	}

	return model.BotKillSwitchResponse{
		BotName:     state.BotName,
		KillEnabled: state.KillEnabled,
		Status:      model.OrderBlockStatus,
		Message:     model.OrderBlockMessage,
		Reason:      state.Reason,
		UpdatedAt:   state.UpdatedAt,
	}, nil
}

func (s *Service) ResumeBot(ctx context.Context, botName string, req model.ResumeBotRequest) (model.BotKillSwitchResponse, error) {
	botName = strings.TrimSpace(botName)
	if botName == "" {
		return model.BotKillSwitchResponse{}, fmt.Errorf("bot_name is required")
	}
	if s.store == nil {
		return model.BotKillSwitchResponse{}, fmt.Errorf("store is not configured")
	}

	state, err := s.store.SetBotKillSwitch(ctx, botName, false, req.Reason)
	if err != nil {
		return model.BotKillSwitchResponse{}, err
	}
	return model.BotKillSwitchResponse{
		BotName:     state.BotName,
		KillEnabled: state.KillEnabled,
		Status:      "RESUMED",
		Message:     "kill switch disabled; orders can be accepted",
		Reason:      state.Reason,
		UpdatedAt:   state.UpdatedAt,
	}, nil
}

func (s *Service) GetBotKillSwitch(ctx context.Context, botName string) (model.BotKillSwitchResponse, error) {
	botName = strings.TrimSpace(botName)
	if botName == "" {
		return model.BotKillSwitchResponse{}, fmt.Errorf("bot_name is required")
	}
	if s.store == nil {
		return model.BotKillSwitchResponse{}, fmt.Errorf("store is not configured")
	}

	state, err := s.effectiveBotKillSwitch(ctx, botName)
	if err != nil {
		return model.BotKillSwitchResponse{}, err
	}
	status := "RESUMED"
	message := "kill switch disabled; orders can be accepted"
	if state.KillEnabled {
		status = model.KillModeStatus
		message = model.KillModeMessage
	}
	return model.BotKillSwitchResponse{
		BotName:     state.BotName,
		KillEnabled: state.KillEnabled,
		Status:      status,
		Message:     message,
		Reason:      state.Reason,
		UpdatedAt:   state.UpdatedAt,
	}, nil
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
	mode, err := normalizeRuntimeMode(mode)
	if err != nil {
		return model.ModifyTradeResponse{}, err
	}

	stoploss, slLimit, spotTrailAnchor := req.Stoploss, req.SLLimit, req.SpotTrailAnchor
	if err := validateModifyTradeRequest(req, stoploss, slLimit, spotTrailAnchor, validity, orderType); err != nil {
		return model.ModifyTradeResponse{}, err
	}
	if err := validateProductionModifyTradeRequest(orderType, stoploss, slLimit); err != nil {
		return model.ModifyTradeResponse{}, err
	}

	if s.store == nil {
		return model.ModifyTradeResponse{}, fmt.Errorf("store is not configured")
	}

	trade, err := s.store.GetTradeByID(ctx, tradeID)
	if err != nil {
		return model.ModifyTradeResponse{}, fmt.Errorf("load trade: %w", err)
	}
	if shouldSkipBrokerStoplossModifyForForceTrail(trade, stoploss, req.ForceTrail) {
		return model.ModifyTradeResponse{
			TradeID: tradeID,
			Message: fmt.Sprintf(
				"force trail skipped because existing stoploss %.2f is greater than requested stoploss %.2f",
				trade.Stoploss,
				*stoploss,
			),
		}, nil
	}
	if err := validateModifiedTradeAgainstTrade(trade, stoploss, slLimit); err != nil {
		return model.ModifyTradeResponse{}, err
	}

	if s.upstox == nil || !s.upstox.Enabled() {
		return model.ModifyTradeResponse{}, fmt.Errorf("%s mode is enabled but upstox client is not configured", mode)
	}
	if len(trade.SLOrderIDs) == 0 {
		return model.ModifyTradeResponse{}, fmt.Errorf("trade has no stoploss orders to modify")
	}

	modifiedOrderIDs := make([]string, 0, len(trade.SLOrderIDs))
	failedOrderMessages := make([]string, 0)
	terminalOrderMessages := make([]string, 0)

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

		if message, outcome := s.syncStopLossBeforeModify(ctx, trade, orderID); outcome != stopLossNotTerminal {
			terminalOrderMessages = append(terminalOrderMessages, message)
			if outcome == stopLossFilled {
				if resp, ok := s.closedModifyTradeResponse(ctx, tradeID, strings.Join(terminalOrderMessages, "; ")); ok {
					return resp, nil
				}
			}
			continue
		}

		modifyResp, err := s.upstox.ModifyOrder(ctx, upstox.ModifyOrderRequest{
			Quantity:     qty,
			Validity:     validity,
			Price:        float64Value(slLimit),
			OrderID:      orderID,
			OrderType:    orderType,
			DisclosedQty: req.DisclosedQty,
			TriggerPrice: float64Value(stoploss),
		})
		if err != nil {
			if upstox.IsRateLimited(err) {
				return model.ModifyTradeResponse{}, fmt.Errorf("modify trade rate limited for trade_id=%s order_id=%s: %w", tradeID, orderID, err)
			}
			if message, outcome := s.syncStopLossAfterModifyError(ctx, trade, orderID, err); outcome != stopLossNotTerminal {
				terminalOrderMessages = append(terminalOrderMessages, message)
				if outcome == stopLossFilled {
					if resp, ok := s.closedModifyTradeResponse(ctx, tradeID, strings.Join(terminalOrderMessages, "; ")); ok {
						return resp, nil
					}
				}
				continue
			}
			failedOrderMessages = append(failedOrderMessages, fmt.Sprintf("%s: %v", orderID, err))
			continue
		}

		activeOrderID := strings.TrimSpace(modifyResp.OrderID)
		if activeOrderID == "" {
			activeOrderID = orderID
		}
		if activeOrderID != orderID {
			if err := s.store.ReplaceStopLossOrderID(ctx, tradeID, orderID, activeOrderID); err != nil {
				return model.ModifyTradeResponse{}, err
			}
			log.Printf("stoploss modify replaced broker order id trade_id=%s old_order_id=%s new_order_id=%s", tradeID, orderID, activeOrderID)
		}
		modifiedOrderIDs = append(modifiedOrderIDs, activeOrderID)
	}

	if len(failedOrderMessages) > 0 {
		return model.ModifyTradeResponse{}, fmt.Errorf("modify trade partially failed for trade_id=%s: %s", tradeID, strings.Join(failedOrderMessages, "; "))
	}

	if len(modifiedOrderIDs) == 0 && len(terminalOrderMessages) > 0 {
		return model.ModifyTradeResponse{
			TradeID: tradeID,
			Message: strings.Join(terminalOrderMessages, "; "),
		}, nil
	}

	if err := s.persistModifiedTradeState(ctx, tradeID, stoploss, slLimit, spotTrailAnchor); err != nil {
		return model.ModifyTradeResponse{}, err
	}

	message := fmt.Sprintf("trade stoploss orders modified on upstox (%s)", mode)
	if len(terminalOrderMessages) > 0 {
		message = message + "; " + strings.Join(terminalOrderMessages, "; ")
	}

	return model.ModifyTradeResponse{
		TradeID:          tradeID,
		ModifiedOrderIDs: modifiedOrderIDs,
		Message:          message,
	}, nil
}

func (s *Service) closedModifyTradeResponse(ctx context.Context, tradeID string, message string) (model.ModifyTradeResponse, bool) {
	closedTrade, err := s.store.GetTradeByID(ctx, tradeID)
	if err != nil {
		log.Printf("reload trade after stoploss modify sync failed for trade_id=%s: %v", tradeID, err)
		return model.ModifyTradeResponse{}, false
	}
	if !isClosedTradeStatus(closedTrade.Status) {
		return model.ModifyTradeResponse{}, false
	}
	if strings.TrimSpace(message) == "" {
		message = "stoploss order already closed; synced trade status"
	}
	return model.ModifyTradeResponse{
		TradeID:      tradeID,
		Status:       closedTrade.Status,
		ExitPrice:    closedTrade.ExitPrice,
		ExitTime:     closedTrade.ExitTime,
		ClosedTrade:  &closedTrade,
		ClosedTrades: []model.Trade{closedTrade},
		ClosedOrders: []model.Trade{closedTrade},
		Message:      message,
	}, true
}

func (s *Service) SquareOffTrade(ctx context.Context, tradeID string, req model.SquareOffTradeRequest) (model.SquareOffTradeResponse, error) {
	tradeID = strings.TrimSpace(tradeID)
	if tradeID == "" {
		return model.SquareOffTradeResponse{}, fmt.Errorf("trade id is required")
	}
	if req.ExitPrice <= 0 {
		return model.SquareOffTradeResponse{}, fmt.Errorf("exit_price must be > 0")
	}
	if req.DisclosedQty < 0 {
		return model.SquareOffTradeResponse{}, fmt.Errorf("disclosed_quantity must be >= 0")
	}
	validity := strings.ToUpper(strings.TrimSpace(req.Validity))
	if validity == "" {
		validity = "DAY"
	}
	if validity != "DAY" && validity != "IOC" {
		return model.SquareOffTradeResponse{}, fmt.Errorf("validity must be DAY or IOC")
	}

	mode := strings.TrimSpace(req.Mode)
	if mode == "" {
		mode = s.cfg.AppMode
	}
	mode, err := normalizeRuntimeMode(mode)
	if err != nil {
		return model.SquareOffTradeResponse{}, err
	}
	exitStatus := strings.TrimSpace(req.Reason)
	if exitStatus == "" {
		exitStatus = "EOD_SQUARE_OFF"
	}

	if s.store == nil {
		return model.SquareOffTradeResponse{}, fmt.Errorf("store is not configured")
	}
	trade, err := s.store.GetTradeByID(ctx, tradeID)
	if err != nil {
		return model.SquareOffTradeResponse{}, fmt.Errorf("load trade: %w", err)
	}
	if isClosedTradeStatus(trade.Status) {
		return model.SquareOffTradeResponse{
			TradeID:   tradeID,
			Status:    trade.Status,
			ExitPrice: trade.ExitPrice,
			ExitTime:  trade.ExitTime,
			Message:   "trade is already closed",
		}, nil
	}

	exitOrderIDs := append([]string(nil), trade.SLOrderIDs...)
	if s.upstox == nil || !s.upstox.Enabled() {
		return model.SquareOffTradeResponse{}, fmt.Errorf("%s mode is enabled but upstox client is not configured", mode)
	}
	exitOrderIDs, err = s.squareOffBrokerOrders(ctx, trade, validity, req.DisclosedQty)
	if err != nil {
		return model.SquareOffTradeResponse{}, err
	}
	if len(exitOrderIDs) == 0 {
		closedTrade, err := s.store.GetTradeByID(ctx, tradeID)
		if err != nil {
			return model.SquareOffTradeResponse{}, fmt.Errorf("reload trade after square-off broker sync: %w", err)
		}
		if isClosedTradeStatus(closedTrade.Status) {
			return model.SquareOffTradeResponse{
				TradeID:   tradeID,
				Status:    closedTrade.Status,
				ExitPrice: closedTrade.ExitPrice,
				ExitTime:  closedTrade.ExitTime,
				Message:   "trade already closed after stoploss broker sync",
			}, nil
		}
		return model.SquareOffTradeResponse{}, fmt.Errorf("square-off did not produce exit order ids for trade_id=%s; trade status is %s", tradeID, closedTrade.Status)
	}

	if err := s.store.SquareOffTrade(ctx, tradeID, exitOrderIDs, req.ExitPrice, trade.Qty, req.ExitTime, exitStatus); err != nil {
		return model.SquareOffTradeResponse{}, err
	}
	s.enforceThresholdDayLossAfterTradeClose(ctx, tradeID)

	closedTrade, err := s.store.GetTradeByID(ctx, tradeID)
	if err != nil {
		return model.SquareOffTradeResponse{}, fmt.Errorf("reload squared-off trade: %w", err)
	}

	return model.SquareOffTradeResponse{
		TradeID:      tradeID,
		Status:       closedTrade.Status,
		ExitOrderIDs: exitOrderIDs,
		ExitPrice:    closedTrade.ExitPrice,
		ExitTime:     closedTrade.ExitTime,
		Message:      fmt.Sprintf("trade squared off (%s)", mode),
	}, nil
}

func (s *Service) squareOffBrokerOrders(ctx context.Context, trade model.Trade, validity string, disclosedQty int) ([]string, error) {
	_ = validity
	_ = disclosedQty

	cancelledSLOrderIDs, stopLossFilled, err := s.cancelTradeStopLossOrders(ctx, trade)
	if err != nil {
		return nil, err
	}
	if stopLossFilled {
		return nil, nil
	}
	if len(cancelledSLOrderIDs) > 0 {
		log.Printf("square-off cancelled stoploss orders trade_id=%s order_ids=%s", trade.ID, strings.Join(cancelledSLOrderIDs, ","))
	}

	tag := tradePositionExitTag(trade)
	if tag == "" {
		return nil, fmt.Errorf("trade tag_entry is required for Upstox exit-position square-off trade_id=%s", trade.ID)
	}

	resp, err := s.upstox.ExitPositions(ctx, upstox.ExitPositionsRequest{Tag: tag})
	if err != nil {
		return nil, fmt.Errorf("exit positions by tag=%s: %w", tag, err)
	}
	if len(resp.OrderIDs) == 0 {
		return nil, fmt.Errorf("exit positions by tag=%s returned no order ids", tag)
	}
	return append([]string(nil), resp.OrderIDs...), nil
}

func (s *Service) cancelTradeStopLossOrders(ctx context.Context, trade model.Trade) ([]string, bool, error) {
	slOrderIDs := cleanStringSet(collectTradeSLOrderIDs([]model.Trade{trade}))
	cancelled := make([]string, 0, len(slOrderIDs))
	failedOrderMessages := make([]string, 0)

	for _, orderID := range slOrderIDs {
		if message, outcome, _ := s.syncStopLossTerminalStateDetailed(ctx, trade, orderID, "pre-square-off cancel"); outcome != stopLossNotTerminal {
			if outcome == stopLossFilled {
				log.Printf("square-off skipped broker exit because stoploss filled trade_id=%s order_id=%s: %s", trade.ID, orderID, message)
				return cancelled, true, nil
			}
			log.Printf("square-off stoploss order already terminal trade_id=%s order_id=%s: %s", trade.ID, orderID, message)
			continue
		}

		resp, err := s.upstox.CancelOrder(ctx, orderID)
		if err != nil {
			if upstox.IsRateLimited(err) {
				return cancelled, false, fmt.Errorf("square-off cancel rate limited for trade_id=%s order_id=%s: %w", trade.ID, orderID, err)
			}
			if isTerminalOrderCancelError(err) {
				message, outcome, _ := s.syncStopLossTerminalStateDetailed(ctx, trade, orderID, "square-off cancel rejected")
				if outcome == stopLossFilled {
					log.Printf("square-off synced already filled stoploss order trade_id=%s order_id=%s: %s", trade.ID, orderID, message)
					return cancelled, true, nil
				}
				if outcome == stopLossTerminalUnfilled {
					log.Printf("square-off stoploss cancel rejected for terminal order trade_id=%s order_id=%s: %s", trade.ID, orderID, message)
					continue
				}
			}
			failedOrderMessages = append(failedOrderMessages, fmt.Sprintf("%s: %v", orderID, err))
			continue
		}
		cancelled = append(cancelled, firstNonEmpty(resp.OrderID, orderID))
	}

	if len(failedOrderMessages) > 0 {
		return cancelled, false, fmt.Errorf("square-off cancel sl failed for trade_id=%s: %s", trade.ID, strings.Join(failedOrderMessages, "; "))
	}
	return cleanStringSet(cancelled), false, nil
}

func (s *Service) cancelBotStopLossOrders(ctx context.Context, trades []model.Trade) ([]string, []string) {
	slOrderIDs := cleanStringSet(collectTradeSLOrderIDs(trades))
	cancelled := make([]string, 0, len(slOrderIDs))
	errs := make([]string, 0)

	for _, orderID := range slOrderIDs {
		resp, err := s.upstox.CancelOrder(ctx, orderID)
		if err != nil {
			errs = append(errs, fmt.Sprintf("cancel sl order %s: %v", orderID, err))
			continue
		}
		cancelled = append(cancelled, firstNonEmpty(resp.OrderID, orderID))
	}

	return cleanStringSet(cancelled), errs
}

func (s *Service) exitBotPositionsByTag(ctx context.Context, segment string, tags []string) ([]string, []string) {
	exitOrderIDs := make([]string, 0)
	errs := make([]string, 0)
	for _, tag := range cleanStringSet(tags) {
		resp, err := s.upstox.ExitPositions(ctx, upstox.ExitPositionsRequest{
			Segment: strings.TrimSpace(segment),
			Tag:     strings.TrimSpace(tag),
		})
		if err != nil {
			errs = append(errs, fmt.Sprintf("exit positions tag=%s: %v", tag, err))
			continue
		}
		exitOrderIDs = append(exitOrderIDs, resp.OrderIDs...)
	}
	return cleanStringSet(exitOrderIDs), errs
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
	s.syncEntryOrders(ctx, trade)

	for _, slOrder := range trade.SLOrders {
		orderID := strings.TrimSpace(slOrder.OrderID)
		if orderID == "" {
			continue
		}

		statusResp, haveStatus := s.getStopLossOrderStatus(ctx, "poll", trade.ID, orderID)
		if haveStatus && isStopLossFill(statusResp, upstox.OrderTrades{}, false) {
			s.recordStopLossFillFromResponses(ctx, trade, slOrder, statusResp, upstox.OrderTrades{}, false)
			continue
		}

		var tradesResp upstox.OrderTrades
		haveTrades := false
		tradesResp, haveTrades = s.getStopLossOrderTrades(ctx, "poll", trade.ID, orderID)

		if !haveStatus && !haveTrades {
			continue
		}

		if isStopLossFill(statusResp, tradesResp, haveTrades) {
			s.recordStopLossFillFromResponses(ctx, trade, slOrder, statusResp, tradesResp, haveTrades)
			continue
		}

		terminalStatus := stopLossTerminalStatus(statusResp, tradesResp, haveTrades)
		if !upstox.IsTerminalOrderStatus(terminalStatus) {
			continue
		}

		s.handleTerminalUnfilledStopLoss(ctx, trade.ID, orderID, terminalStatus)
	}
}

func (s *Service) getStopLossOrderStatus(ctx context.Context, reason string, tradeID string, orderID string) (upstox.OrderStatus, bool) {
	statusResp, err := s.upstox.GetOrderStatus(ctx, orderID)
	if err != nil {
		if upstox.IsRateLimited(err) {
			log.Printf("%s sl order status rate-limited for trade_id=%s order_id=%s: %v", reason, tradeID, orderID, err)
		} else {
			log.Printf("%s sl order status failed for trade_id=%s order_id=%s: %v", reason, tradeID, orderID, err)
		}
		return upstox.OrderStatus{}, false
	}
	return statusResp, true
}

func (s *Service) getStopLossOrderTrades(ctx context.Context, reason string, tradeID string, orderID string) (upstox.OrderTrades, bool) {
	tradesResp, err := s.upstox.GetOrderTrades(ctx, orderID)
	if err != nil {
		if upstox.IsRateLimited(err) {
			log.Printf("%s sl order trades rate-limited for trade_id=%s order_id=%s: %v", reason, tradeID, orderID, err)
		} else {
			log.Printf("%s sl order trades failed for trade_id=%s order_id=%s: %v", reason, tradeID, orderID, err)
		}
		return upstox.OrderTrades{}, false
	}
	return tradesResp, true
}

func isStopLossFill(statusResp upstox.OrderStatus, tradesResp upstox.OrderTrades, haveTrades bool) bool {
	if upstox.IsFilledOrderStatus(statusResp.Status) {
		return true
	}
	if statusResp.FilledQuantity > 0 && statusResp.Quantity > 0 && statusResp.FilledQuantity >= statusResp.Quantity {
		return true
	}
	return haveTrades && tradesResp.Filled
}

func stopLossTerminalStatus(statusResp upstox.OrderStatus, tradesResp upstox.OrderTrades, haveTrades bool) string {
	terminalStatus := strings.TrimSpace(statusResp.Status)
	if terminalStatus == "" && haveTrades {
		terminalStatus = strings.TrimSpace(tradesResp.Status)
	}
	return terminalStatus
}

func (s *Service) recordStopLossFillFromResponses(ctx context.Context, trade model.TradeForSLPolling, slOrder model.StopLossOrderForPolling, statusResp upstox.OrderStatus, tradesResp upstox.OrderTrades, haveTrades bool) {
	avgPrice := statusResp.AveragePrice
	if haveTrades && tradesResp.AveragePrice != nil && *tradesResp.AveragePrice > 0 {
		avgPrice = tradesResp.AveragePrice
	}
	filledQty := statusResp.FilledQuantity
	if haveTrades && tradesResp.FilledQuantity > 0 {
		filledQty = tradesResp.FilledQuantity
	}
	log.Printf(
		"broker SL fill response trade_id=%s order_id=%s status=%s filled_qty=%d avg_price=%s status_raw=%s trades_raw=%s",
		trade.ID,
		strings.TrimSpace(slOrder.OrderID),
		strings.TrimSpace(statusResp.Status),
		filledQty,
		floatPtrForLog(avgPrice),
		rawJSONForLog(statusResp.RawData),
		rawJSONForLog(tradesResp.RawData),
	)
	s.recordStopLossFill(ctx, trade, slOrder, statusResp, avgPrice, filledQty)
}

func (s *Service) syncEntryOrders(ctx context.Context, trade model.TradeForSLPolling) {
	for _, entryOrder := range trade.EntryOrders {
		orderID := strings.TrimSpace(entryOrder.OrderID)
		if orderID == "" || entryOrderAlreadySynced(entryOrder) {
			continue
		}

		statusResp, err := s.upstox.GetOrderStatus(ctx, orderID)
		if err != nil {
			if upstox.IsRateLimited(err) {
				log.Printf("poll entry order status rate-limited for trade_id=%s order_id=%s: %v", trade.ID, orderID, err)
			} else {
				log.Printf("poll entry order status failed for trade_id=%s order_id=%s: %v", trade.ID, orderID, err)
			}
			continue
		}
		if !upstox.IsFilledOrderStatus(statusResp.Status) {
			continue
		}

		entryPrice := executionPrice(statusResp.AveragePrice)
		if entryPrice <= 0 {
			entryPrice = entryOrder.EntryPrice
		}
		entryQty := executionQuantity(statusResp.FilledQuantity, statusResp.Quantity, entryOrder.Qty, singleOrderFallbackQty(trade.Qty, len(trade.EntryOrders)))
		brokerage := s.calculateBrokerage(ctx, trade, statusResp, entryPrice, entryQty, trade.Side)
		if err := s.store.SyncEntryOrderExecution(ctx, trade.ID, orderID, entryPrice, entryQty, brokerage); err != nil {
			log.Printf("sync entry order execution failed for trade_id=%s order_id=%s: %v", trade.ID, orderID, err)
		}
	}
}

func entryOrderAlreadySynced(order model.EntryOrderForPolling) bool {
	return order.EntryPrice > 0 && order.Qty > 0 && order.Brokerage > 0
}

func (s *Service) syncStopLossBeforeModify(ctx context.Context, trade model.Trade, orderID string) (string, stopLossTerminalOutcome) {
	message, outcome, _ := s.syncStopLossTerminalStateDetailed(ctx, trade, orderID, "pre-modify")
	return message, outcome
}

func (s *Service) syncStopLossAfterModifyError(ctx context.Context, trade model.Trade, orderID string, modifyErr error) (string, stopLossTerminalOutcome) {
	message, outcome, _ := s.syncStopLossTerminalStateDetailed(ctx, trade, orderID, "modify rejected")
	if outcome != stopLossNotTerminal {
		return message, outcome
	}
	if !isTerminalModifyOrderError(modifyErr) {
		return "", stopLossNotTerminal
	}

	// Upstox sometimes rejects modify with UDAPI100041 before order details/trades
	// reflect the terminal state. Treat the modify rejection as authoritative for
	// trailing management so bots do not repeatedly retry the same dead SL order.
	disabled, err := s.store.DisableTrailingByActiveStopLossOrderID(ctx, trade.ID, orderID)
	if err != nil {
		log.Printf("disable trailing after terminal modify rejection failed for trade_id=%s order_id=%s: %v", trade.ID, orderID, err)
		return "", stopLossNotTerminal
	}
	if !disabled {
		log.Printf("modify rejected for stale terminal SL; current active SL already moved on trade_id=%s order_id=%s: %v", trade.ID, orderID, modifyErr)
		return fmt.Sprintf("%s: modify rejected for stale terminal stoploss order", orderID), stopLossTerminalUnfilled
	}
	log.Printf("modify rejected for terminal SL; disabled trailing and kept trade open trade_id=%s order_id=%s: %v", trade.ID, orderID, modifyErr)
	return fmt.Sprintf("%s: modify rejected because stoploss order is terminal; disabled trailing", orderID), stopLossTerminalUnfilled
}

func (s *Service) syncStopLossTerminalState(ctx context.Context, trade model.Trade, orderID string, reason string) (string, bool, bool) {
	message, outcome, synced := s.syncStopLossTerminalStateDetailed(ctx, trade, orderID, reason)
	return message, outcome != stopLossNotTerminal, synced
}

type stopLossTerminalOutcome int

const (
	stopLossNotTerminal stopLossTerminalOutcome = iota
	stopLossFilled
	stopLossTerminalUnfilled
)

func (s *Service) syncStopLossTerminalStateDetailed(ctx context.Context, trade model.Trade, orderID string, reason string) (string, stopLossTerminalOutcome, bool) {
	pollingTrade := tradeForStopLossSync(trade)
	slOrder := stopLossOrderForSync(pollingTrade, trade, orderID)

	statusResp, haveStatus := s.getStopLossOrderStatus(ctx, reason+" sync", trade.ID, orderID)
	if haveStatus && isStopLossFill(statusResp, upstox.OrderTrades{}, false) {
		s.recordStopLossFillFromResponses(ctx, pollingTrade, slOrder, statusResp, upstox.OrderTrades{}, false)
		return fmt.Sprintf("%s: stoploss order already filled; synced trade status", orderID), stopLossFilled, true
	}

	tradesResp, haveTrades := s.getStopLossOrderTrades(ctx, reason+" sync", trade.ID, orderID)

	if !haveStatus && !haveTrades {
		return "", stopLossNotTerminal, false
	}

	if isStopLossFill(statusResp, tradesResp, haveTrades) {
		s.recordStopLossFillFromResponses(ctx, pollingTrade, slOrder, statusResp, tradesResp, haveTrades)
		return fmt.Sprintf("%s: stoploss order already filled; synced trade status", orderID), stopLossFilled, true
	}

	terminalStatus := stopLossTerminalStatus(statusResp, tradesResp, haveTrades)
	if upstox.IsTerminalOrderStatus(terminalStatus) {
		s.handleTerminalUnfilledStopLoss(ctx, trade.ID, orderID, terminalStatus)
		return fmt.Sprintf("%s: stoploss order already terminal (%s); disabled trailing", orderID, terminalStatus), stopLossTerminalUnfilled, true
	}

	return "", stopLossNotTerminal, true
}

func tradeForStopLossSync(trade model.Trade) model.TradeForSLPolling {
	out := model.TradeForSLPolling{
		ID:              trade.ID,
		BotName:         trade.BotName,
		InstrumentToken: trade.InstrumentToken,
		Side:            trade.Side,
		Qty:             trade.Qty,
		Product:         trade.Product,
		EntryPrice:      trade.EntryPrice,
		TotalBrokerage:  trade.TotalBrokerage,
	}

	for _, order := range trade.Orders {
		orderID := strings.TrimSpace(order.OrderID)
		if orderID == "" {
			continue
		}
		switch strings.ToLower(strings.TrimSpace(order.OrderType)) {
		case "entry":
			out.EntryOrders = append(out.EntryOrders, model.EntryOrderForPolling{
				OrderID:    orderID,
				EntryPrice: order.EntryPrice,
				Qty:        order.Qty,
				Brokerage:  order.Brokerage,
			})
		case "sl":
			out.SLOrders = append(out.SLOrders, model.StopLossOrderForPolling{
				OrderID:   orderID,
				Stoploss:  order.Stoploss,
				Qty:       order.Qty,
				Brokerage: order.Brokerage,
			})
		}
	}

	if len(out.EntryOrders) == 0 {
		for _, orderID := range trade.EntryOrderIDs {
			orderID = strings.TrimSpace(orderID)
			if orderID == "" {
				continue
			}
			out.EntryOrders = append(out.EntryOrders, model.EntryOrderForPolling{
				OrderID:    orderID,
				EntryPrice: trade.EntryPrice,
				Qty:        trade.Qty,
			})
		}
	}

	if len(out.SLOrders) == 0 {
		for _, orderID := range trade.SLOrderIDs {
			orderID = strings.TrimSpace(orderID)
			if orderID == "" {
				continue
			}
			out.SLOrders = append(out.SLOrders, model.StopLossOrderForPolling{
				OrderID:  orderID,
				Stoploss: trade.Stoploss,
				Qty:      slOrderQuantity(trade, orderID),
			})
		}
	}

	return out
}

func stopLossOrderForSync(pollingTrade model.TradeForSLPolling, trade model.Trade, orderID string) model.StopLossOrderForPolling {
	orderID = strings.TrimSpace(orderID)
	for _, slOrder := range pollingTrade.SLOrders {
		if strings.TrimSpace(slOrder.OrderID) == orderID {
			return slOrder
		}
	}
	return model.StopLossOrderForPolling{
		OrderID:  orderID,
		Stoploss: trade.Stoploss,
		Qty:      slOrderQuantity(trade, orderID),
	}
}

func (s *Service) recordStopLossFill(ctx context.Context, trade model.TradeForSLPolling, slOrder model.StopLossOrderForPolling, statusResp upstox.OrderStatus, averagePrice *float64, filledQty int) {
	orderID := strings.TrimSpace(slOrder.OrderID)
	exitPrice := executionPrice(averagePrice, statusResp.AveragePrice)
	if exitPrice <= 0 {
		exitPrice = slOrder.Stoploss
	}
	exitQty := executionQuantity(filledQty, statusResp.FilledQuantity, statusResp.Quantity, slOrder.Qty, singleOrderFallbackQty(trade.Qty, len(trade.SLOrders)))
	brokerage := s.calculateBrokerage(ctx, trade, statusResp, exitPrice, exitQty, oppositeSide(trade.Side))
	if err := s.store.RecordStopLossFill(ctx, trade.ID, orderID, exitPrice, exitQty, brokerage, "STOPLOSS HIT"); err != nil {
		log.Printf("record SL fill failed for trade_id=%s order_id=%s: %v", trade.ID, orderID, err)
		return
	}
	s.enforceThresholdDayLossAfterTradeClose(ctx, trade.ID)
	log.Printf("recorded SL fill in DB trade_id=%s order_id=%s exit_price=%.2f qty=%d brokerage=%s", trade.ID, orderID, exitPrice, exitQty, floatPtrForLog(brokerage))
}

func (s *Service) effectiveBotKillSwitch(ctx context.Context, botName string) (model.BotKillSwitch, error) {
	globalState, err := s.store.GetBotKillSwitch(ctx, model.AllStrategiesKillSwitchBotName)
	if err != nil {
		return model.BotKillSwitch{}, err
	}
	if globalState.KillEnabled {
		globalState.BotName = strings.TrimSpace(botName)
		return globalState, nil
	}
	return s.store.GetBotKillSwitch(ctx, botName)
}

func (s *Service) enforceThresholdDayLossAfterTradeClose(ctx context.Context, tradeID string) {
	if s.store == nil || s.cfg.ThresholdDayLoss <= 0 {
		return
	}

	s.dayLossMu.Lock()
	defer s.dayLossMu.Unlock()

	summary, err := s.store.DailyLossSummaryForTrade(ctx, tradeID)
	if err != nil {
		log.Printf("daily loss threshold check failed for trade_id=%s: %v", tradeID, err)
		return
	}
	if !dayLossThresholdReached(summary.Loss, s.cfg.ThresholdDayLoss) {
		return
	}

	globalState, err := s.store.GetBotKillSwitch(ctx, model.AllStrategiesKillSwitchBotName)
	if err != nil {
		log.Printf("daily loss threshold global kill state check failed: %v", err)
		return
	}
	if globalState.KillEnabled {
		return
	}

	reason := fmt.Sprintf(
		"threshold_day_loss reached for %s: day_loss=%.2f threshold=%.2f realized_pnl=%.2f",
		summary.CurrDate,
		summary.Loss,
		s.cfg.ThresholdDayLoss,
		summary.RealizedPNL,
	)
	if err := s.enableKillSwitchForAllStrategies(ctx, summary.CurrDate, reason); err != nil {
		log.Printf("daily loss threshold kill-all failed: %v", err)
		return
	}
	log.Printf("daily loss threshold kill-all enabled: %s", reason)
}

func dayLossThresholdReached(dayLoss float64, threshold float64) bool {
	return threshold > 0 && dayLoss >= threshold
}

func (s *Service) enableKillSwitchForAllStrategies(ctx context.Context, currDate string, reason string) error {
	if s.store == nil {
		return fmt.Errorf("store is not configured")
	}

	if _, err := s.store.SetBotKillSwitch(ctx, model.AllStrategiesKillSwitchBotName, true, reason); err != nil {
		return err
	}

	botNames := append([]string(nil), s.cfg.StrategyBotNames...)
	knownBotNames, err := s.store.ListKnownBotNames(ctx)
	if err != nil {
		log.Printf("daily loss threshold list known bots failed: %v", err)
	} else {
		botNames = append(botNames, knownBotNames...)
	}
	botNames = cleanStringSet(botNames)

	errorsOut := make([]string, 0)
	for _, botName := range botNames {
		if strings.TrimSpace(botName) == "" || botName == model.AllStrategiesKillSwitchBotName {
			continue
		}
		resp, err := s.KillBot(ctx, botName, model.KillBotRequest{
			CurrDate: currDate,
			Reason:   reason,
		})
		if err != nil {
			errorsOut = append(errorsOut, fmt.Sprintf("%s: %v", botName, err))
			continue
		}
		for _, brokerErr := range resp.Errors {
			log.Printf("daily loss threshold kill-all broker warning bot=%s: %s", botName, brokerErr)
		}
	}
	if len(errorsOut) > 0 {
		return errors.New(strings.Join(errorsOut, "; "))
	}
	return nil
}

func (s *Service) calculateBrokerage(ctx context.Context, trade model.TradeForSLPolling, statusResp upstox.OrderStatus, price float64, qty int, fallbackTxnType string) *float64 {
	instrumentToken := strings.TrimSpace(statusResp.InstrumentToken)
	if instrumentToken == "" {
		instrumentToken = strings.TrimSpace(trade.InstrumentToken)
	}
	product := strings.ToUpper(strings.TrimSpace(statusResp.Product))
	if product == "" {
		product = strings.ToUpper(strings.TrimSpace(trade.Product))
	}
	transactionType := strings.ToUpper(strings.TrimSpace(statusResp.TransactionType))
	if transactionType == "" {
		transactionType = strings.ToUpper(strings.TrimSpace(fallbackTxnType))
	}
	if qty <= 0 {
		qty = executionQuantity(statusResp.FilledQuantity, statusResp.Quantity)
	}
	if instrumentToken == "" || product == "" || transactionType == "" || qty <= 0 || price <= 0 {
		return nil
	}

	resp, err := s.upstox.GetBrokerage(ctx, upstox.BrokerageRequest{
		InstrumentToken: instrumentToken,
		Quantity:        qty,
		Product:         product,
		TransactionType: transactionType,
		Price:           price,
	})
	if err != nil {
		log.Printf("calculate brokerage failed for trade_id=%s order_id=%s: %v", trade.ID, statusResp.OrderID, err)
		return nil
	}
	return resp.Total
}

func executionPrice(prices ...*float64) float64 {
	for _, price := range prices {
		if price != nil && *price > 0 {
			return *price
		}
	}
	return 0
}

func executionQuantity(values ...int) int {
	for _, value := range values {
		if value > 0 {
			return value
		}
	}
	return 0
}

func floatPtrForLog(value *float64) string {
	if value == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%.2f", *value)
}

func rawJSONForLog(data []byte) string {
	text := strings.TrimSpace(string(data))
	if text == "" {
		return "<empty>"
	}
	const maxLogBytes = 4096
	if len(text) > maxLogBytes {
		return text[:maxLogBytes] + "...<truncated>"
	}
	return text
}

func singleOrderFallbackQty(tradeQty int, orderCount int) int {
	if orderCount == 1 && tradeQty > 0 {
		return tradeQty
	}
	return 0
}

func (s *Service) handleTerminalUnfilledStopLoss(ctx context.Context, tradeID string, orderID string, status string) {
	disabled, err := s.store.DisableTrailingByActiveStopLossOrderID(ctx, tradeID, orderID)
	if err != nil {
		log.Printf("disable trailing after terminal SL failed for trade_id=%s order_id=%s status=%s: %v", tradeID, orderID, status, err)
		return
	}
	if !disabled {
		log.Printf("sl order terminal but no longer active; kept trailing unchanged trade_id=%s order_id=%s status=%s", tradeID, orderID, status)
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
		return fmt.Errorf("stoploss is required in sandbox/production mode")
	}
	if orderType == "SL" && slLimit == nil {
		return fmt.Errorf("sl_limit is required for SL order modification in sandbox/production mode")
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

func shouldSkipBrokerStoplossModifyForForceTrail(trade model.Trade, stoploss *float64, forceTrail bool) bool {
	return forceTrail && stoploss != nil && trade.Stoploss > 0 && trade.Stoploss > *stoploss
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

func (s *Service) hydrateOrderExchangeIDs(ctx context.Context, orders []model.OrderRef) []model.OrderRef {
	if len(orders) == 0 || !s.cfg.IsProduction() || s.upstox == nil || !s.upstox.Enabled() {
		return orders
	}

	hydrateCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	out := make([]model.OrderRef, 0, len(orders))
	for _, order := range orders {
		order.OrderID = strings.TrimSpace(order.OrderID)
		order.ExchangeOrderID = strings.TrimSpace(order.ExchangeOrderID)
		if order.OrderID == "" {
			continue
		}
		if order.ExchangeOrderID == "" && hydrateCtx.Err() == nil {
			statusResp, err := s.upstox.GetOrderStatusFresh(hydrateCtx, order.OrderID)
			if err != nil {
				log.Printf("hydrate exchange order id failed order_id=%s: %v", order.OrderID, err)
			} else {
				order.OrderID = firstNonEmpty(statusResp.OrderID, order.OrderID)
				order.ExchangeOrderID = strings.TrimSpace(statusResp.ExchangeOrderID)
			}
		}
		out = append(out, order)
	}
	return out
}

func buildOrderParams(orders []model.OrderRef, base store.CreateOrderParams) []store.CreateOrderParams {
	if len(orders) == 0 {
		return []store.CreateOrderParams{base}
	}

	params := make([]store.CreateOrderParams, 0, len(orders))
	for _, orderRef := range orders {
		orderID := strings.TrimSpace(orderRef.OrderID)
		if orderID == "" {
			continue
		}
		order := base
		order.OrderID = orderID
		order.ExchangeOrderID = strings.TrimSpace(orderRef.ExchangeOrderID)
		params = append(params, order)
	}
	if len(params) == 0 {
		return []store.CreateOrderParams{base}
	}
	return params
}

func orderRefsFromUpstox(orders []upstox.OrderRef) []model.OrderRef {
	out := make([]model.OrderRef, 0, len(orders))
	for _, order := range orders {
		orderID := strings.TrimSpace(order.OrderID)
		if orderID == "" {
			continue
		}
		out = append(out, model.OrderRef{
			OrderID:         orderID,
			ExchangeOrderID: strings.TrimSpace(order.ExchangeOrderID),
		})
	}
	return out
}

func orderRefsFromIDs(orderIDs []string) []model.OrderRef {
	out := make([]model.OrderRef, 0, len(orderIDs))
	for _, orderID := range orderIDs {
		orderID = strings.TrimSpace(orderID)
		if orderID == "" {
			continue
		}
		out = append(out, model.OrderRef{OrderID: orderID})
	}
	return out
}

func orderIDsFromModelRefs(orders []model.OrderRef) []string {
	out := make([]string, 0, len(orders))
	for _, order := range orders {
		orderID := strings.TrimSpace(order.OrderID)
		if orderID == "" {
			continue
		}
		seen := false
		for _, existing := range out {
			if existing == orderID {
				seen = true
				break
			}
		}
		if !seen {
			out = append(out, orderID)
		}
	}
	return out
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

func squareOffBrokerOrderQuantity(trade model.Trade, orderID string) int {
	orderID = strings.TrimSpace(orderID)
	if orderID == "" {
		return 0
	}
	for _, order := range trade.Orders {
		if strings.TrimSpace(order.OrderID) == orderID && order.Qty > 0 {
			return order.Qty
		}
	}
	if trade.Qty <= 0 {
		return 0
	}
	if len(trade.SLOrderIDs) <= 1 {
		return trade.Qty
	}
	if trade.Qty%len(trade.SLOrderIDs) == 0 {
		return trade.Qty / len(trade.SLOrderIDs)
	}
	return trade.Qty
}

func collectTradeSLOrderIDs(trades []model.Trade) []string {
	out := make([]string, 0)
	for _, trade := range trades {
		out = append(out, trade.SLOrderIDs...)
		for _, order := range trade.Orders {
			if strings.EqualFold(strings.TrimSpace(order.OrderType), "sl") {
				out = append(out, order.OrderID)
			}
		}
	}
	return out
}

func killPositionTags(botName string, requestedTag string, trades []model.Trade) []string {
	if tag := strings.TrimSpace(requestedTag); tag != "" {
		return []string{tag}
	}
	tags := make([]string, 0, len(trades))
	for _, trade := range trades {
		if tag := strings.TrimSpace(trade.TagEntry); tag != "" {
			tags = append(tags, tag)
		}
	}
	tags = cleanStringSet(tags)
	if len(tags) > 0 {
		return tags
	}
	botName = strings.TrimSpace(botName)
	if botName == "" {
		return nil
	}
	return []string{botName + "-entry"}
}

func tradePositionExitTag(trade model.Trade) string {
	if tag := strings.TrimSpace(trade.TagEntry); tag != "" {
		return tag
	}
	botName := strings.TrimSpace(trade.BotName)
	if botName == "" {
		return ""
	}
	return botName + "-entry"
}

func cleanStringSet(values []string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		seen := false
		for _, existing := range out {
			if existing == value {
				seen = true
				break
			}
		}
		if !seen {
			out = append(out, value)
		}
	}
	return out
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func oppositeSide(side string) string {
	if strings.EqualFold(strings.TrimSpace(side), "SELL") {
		return "BUY"
	}
	return "SELL"
}

func normalizeRuntimeMode(mode string) (string, error) {
	normalized := strings.ToLower(strings.TrimSpace(mode))
	switch normalized {
	case "", config.ModeSandbox:
		return config.ModeSandbox, nil
	case "prod", config.ModeProduction:
		return config.ModeProduction, nil
	default:
		return "", fmt.Errorf("mode must be sandbox or production")
	}
}

func isClosedTradeStatus(status string) bool {
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case "TARGET HIT", "STOPLOSS HIT", "MANUAL EXIT", "EOD_SQUARE_OFF", model.KillSwitchExitStatus:
		return true
	default:
		return false
	}
}

func isTerminalModifyOrderError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "udapi100041") ||
		strings.Contains(msg, "already cancelled") ||
		strings.Contains(msg, "already canceled") ||
		strings.Contains(msg, "already rejected") ||
		strings.Contains(msg, "already completed") ||
		(strings.Contains(msg, "modifications of already") && strings.Contains(msg, "orders is not allowed"))
}

func isTerminalOrderCancelError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "udapi100040") ||
		strings.Contains(msg, "already cancelled") ||
		strings.Contains(msg, "already canceled") ||
		strings.Contains(msg, "already rejected") ||
		strings.Contains(msg, "already completed")
}
