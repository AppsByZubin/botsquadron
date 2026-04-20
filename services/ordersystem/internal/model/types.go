package model

import "time"

type CreateTradeRequest struct {
	BotName         string   `json:"bot_name"`
	InitCash        *float64 `json:"init_cash,omitempty"`
	CurrDate        string   `json:"curr_date,omitempty"`
	MonthYear       string   `json:"month_year,omitempty"`
	Mode            string   `json:"mode,omitempty"`
	Symbol          string   `json:"symbol"`
	InstrumentToken string   `json:"instrument_token"`
	Side            string   `json:"side"`
	Qty             int      `json:"qty"`
	Product         string   `json:"product,omitempty"`
	Validity        string   `json:"validity,omitempty"`
	EntryPrice      *float64 `json:"entry_price,omitempty"`
	Target          *float64 `json:"target,omitempty"`
	SLTrigger       *float64 `json:"sl_trigger,omitempty"`
	SLLimit         *float64 `json:"sl_limit,omitempty"`
	TSLActive       bool     `json:"tsl_active,omitempty"`
	StartTrailAfter *float64 `json:"start_trail_after,omitempty"`
	EntrySpot       *float64 `json:"entry_spot,omitempty"`
	SpotLTP         *float64 `json:"spot_ltp,omitempty"`
	SpotTrailAnchor *float64 `json:"spot_trail_anchor,omitempty"`
	TrailPoints     *float64 `json:"trail_points,omitempty"`
	TotalBrokerage  *float64 `json:"total_brokerage,omitempty"`
	TagEntry        string   `json:"tag_entry,omitempty"`
	TagSL           string   `json:"tag_sl,omitempty"`
	Description     string   `json:"description,omitempty"`
	IsAMO           bool     `json:"is_amo,omitempty"`
	Slice           *bool    `json:"slice,omitempty"`
}

type CreateTradeResponse struct {
	TradeID       string   `json:"trade_id"`
	Status        string   `json:"status"`
	EntryOrderIDs []string `json:"entry_order_ids,omitempty"`
	SLOrderIDs    []string `json:"sl_order_ids,omitempty"`
	Message       string   `json:"message,omitempty"`
}

type CreateAccountRequest struct {
	BotName   string   `json:"bot_name"`
	CurrDate  string   `json:"curr_date,omitempty"`
	MonthYear string   `json:"month_year,omitempty"`
	InitCash  *float64 `json:"init_cash,omitempty"`
}

type GetAccountDetailsRequest struct {
	BotName  string `json:"bot_name"`
	CurrDate string `json:"curr_date"`
}

type AccountResponse struct {
	AccountID string  `json:"account_id"`
	BotName   string  `json:"bot_name"`
	CurrDate  string  `json:"curr_date"`
	MonthYear string  `json:"month_year"`
	InitCash  float64 `json:"init_cash"`
	NetProfit float64 `json:"net_profit"`
	Message   string  `json:"message,omitempty"`
}

type AccountDetailsResponse struct {
	AccountID string  `json:"account_id"`
	BotName   string  `json:"bot_name"`
	CurrDate  string  `json:"curr_date"`
	MonthYear string  `json:"month_year"`
	InitCash  float64 `json:"init_cash"`
	NetProfit float64 `json:"net_profit"`
	Trades    []Trade `json:"trades"`
	Message   string  `json:"message,omitempty"`
}

type Account struct {
	ID        string
	BotName   string
	CurrDate  string
	MonthYear string
	InitCash  float64
	NetProfit float64
}

type ModifyTradeRequest struct {
	Mode            string   `json:"mode,omitempty"`
	Validity        string   `json:"validity,omitempty"`
	OrderType       string   `json:"order_type,omitempty"`
	DisclosedQty    int      `json:"disclosed_quantity,omitempty"`
	Stoploss        *float64 `json:"stoploss,omitempty"`
	SLLimit         *float64 `json:"sl_limit,omitempty"`
	SpotTrailAnchor *float64 `json:"spot_trail_anchor,omitempty"`
}

type ModifyTradeResponse struct {
	TradeID          string   `json:"trade_id"`
	ModifiedOrderIDs []string `json:"modified_order_ids,omitempty"`
	Message          string   `json:"message,omitempty"`
}

type SquareOffTradeRequest struct {
	Mode         string     `json:"mode,omitempty"`
	ExitPrice    float64    `json:"exit_price"`
	ExitTime     *time.Time `json:"exit_time,omitempty"`
	Reason       string     `json:"reason,omitempty"`
	Validity     string     `json:"validity,omitempty"`
	DisclosedQty int        `json:"disclosed_quantity,omitempty"`
}

type SquareOffTradeResponse struct {
	TradeID      string     `json:"trade_id"`
	Status       string     `json:"status"`
	ExitOrderIDs []string   `json:"exit_order_ids,omitempty"`
	ExitPrice    float64    `json:"exit_price,omitempty"`
	ExitTime     *time.Time `json:"exit_time,omitempty"`
	Message      string     `json:"message,omitempty"`
}

type Order struct {
	ID              string     `json:"id"`
	OrderID         string     `json:"order_id,omitempty"`
	TradeID         string     `json:"trade_id,omitempty"`
	InstrumentToken string     `json:"instrument_token,omitempty"`
	OrderType       string     `json:"order_type"`
	Qty             int        `json:"qty,omitempty"`
	EntryPrice      float64    `json:"entry_price,omitempty"`
	Target          float64    `json:"target,omitempty"`
	Stoploss        float64    `json:"stoploss,omitempty"`
	SLLimit         float64    `json:"sl_limit,omitempty"`
	ExitPrice       float64    `json:"exit_price,omitempty"`
	PNL             float64    `json:"pnl,omitempty"`
	ExitTime        *time.Time `json:"exit_time,omitempty"`
	Brokerage       float64    `json:"brokerage,omitempty"`
}

type Trade struct {
	ID              string     `json:"id"`
	AccountID       string     `json:"acct_id,omitempty"`
	BotName         string     `json:"bot_name,omitempty"`
	Symbol          string     `json:"symbol"`
	InstrumentToken string     `json:"instrument_token"`
	Side            string     `json:"side"`
	Qty             int        `json:"qty"`
	Product         string     `json:"product,omitempty"`
	Validity        string     `json:"validity,omitempty"`
	EntryOrderIDs   []string   `json:"entry_order_ids,omitempty"`
	SLOrderIDs      []string   `json:"sl_order_ids,omitempty"`
	Orders          []Order    `json:"orders,omitempty"`
	EntryPrice      float64    `json:"entry_price,omitempty"`
	Target          float64    `json:"target,omitempty"`
	Stoploss        float64    `json:"stoploss,omitempty"`
	SLLimit         float64    `json:"sl_limit,omitempty"`
	TSLActive       bool       `json:"tsl_active,omitempty"`
	StartTrailAfter float64    `json:"start_trail_after,omitempty"`
	EntrySpot       float64    `json:"entry_spot,omitempty"`
	SpotLTP         float64    `json:"spot_ltp,omitempty"`
	SpotTrailAnchor float64    `json:"spot_trail_anchor,omitempty"`
	TrailPoints     float64    `json:"trail_points,omitempty"`
	Status          string     `json:"status"`
	Timestamp       time.Time  `json:"timestamp"`
	ExitPrice       float64    `json:"exit_price,omitempty"`
	PNL             float64    `json:"pnl,omitempty"`
	TotalBrokerage  float64    `json:"total_brokerage,omitempty"`
	ExitTime        *time.Time `json:"exit_time,omitempty"`
	TagEntry        string     `json:"tag_entry,omitempty"`
	TagSL           string     `json:"tag_sl,omitempty"`
	Description     string     `json:"description,omitempty"`
}

type StopLossOrderForPolling struct {
	OrderID   string
	Stoploss  float64
	Qty       int
	Brokerage float64
}

type EntryOrderForPolling struct {
	OrderID    string
	EntryPrice float64
	Qty        int
	Brokerage  float64
}

type TradeForSLPolling struct {
	ID              string
	BotName         string
	InstrumentToken string
	Side            string
	Qty             int
	Product         string
	EntryPrice      float64
	TotalBrokerage  float64
	EntryOrders     []EntryOrderForPolling
	SLOrders        []StopLossOrderForPolling
}
