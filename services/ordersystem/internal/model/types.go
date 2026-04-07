package model

import "time"

type CreateTradeRequest struct {
	BotName         string   `json:"bot_name"`
	Mode            string   `json:"mode,omitempty"`
	Symbol          string   `json:"symbol"`
	InstrumentToken string   `json:"instrument_token"`
	Side            string   `json:"side"`
	Qty             int      `json:"qty"`
	Product         string   `json:"product,omitempty"`
	Validity        string   `json:"validity,omitempty"`
	EntryPrice      *float64 `json:"entry_price,omitempty"`
	Target          *float64 `json:"target,omitempty"`
	Stoploss        *float64 `json:"stoploss,omitempty"`
	SLLimit         *float64 `json:"sl_limit,omitempty"`
	TSLActive       bool     `json:"tsl_active,omitempty"`
	StartTrailAfter *float64 `json:"start_trail_after,omitempty"`
	EntrySpot       *float64 `json:"entry_spot,omitempty"`
	SpotLTP         *float64 `json:"spot_ltp,omitempty"`
	TrailPoints     *float64 `json:"trail_points,omitempty"`
	Taxes           *float64 `json:"taxes,omitempty"`
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

type Trade struct {
	ID              string     `json:"id"`
	BotName         string     `json:"bot_name,omitempty"`
	Symbol          string     `json:"symbol"`
	InstrumentToken string     `json:"instrument_token"`
	Side            string     `json:"side"`
	Qty             int        `json:"qty"`
	Product         string     `json:"product,omitempty"`
	Validity        string     `json:"validity,omitempty"`
	EntryOrderIDs   []string   `json:"entry_order_ids,omitempty"`
	SLOrderIDs      []string   `json:"sl_order_ids,omitempty"`
	TargetOrderID   string     `json:"target_order_id,omitempty"`
	EntryPrice      float64    `json:"entry_price,omitempty"`
	Target          float64    `json:"target,omitempty"`
	Stoploss        float64    `json:"stoploss,omitempty"`
	SLLimit         float64    `json:"sl_limit,omitempty"`
	TSLActive       bool       `json:"tsl_active,omitempty"`
	StartTrailAfter float64    `json:"start_trail_after,omitempty"`
	EntrySpot       float64    `json:"entry_spot,omitempty"`
	SpotLTP         float64    `json:"spot_ltp,omitempty"`
	TrailPoints     float64    `json:"trail_points,omitempty"`
	Status          string     `json:"status"`
	Timestamp       time.Time  `json:"timestamp"`
	ExitPrice       float64    `json:"exit_price,omitempty"`
	PNL             float64    `json:"pnl,omitempty"`
	Taxes           float64    `json:"taxes,omitempty"`
	ExitTime        *time.Time `json:"exit_time,omitempty"`
	TagEntry        string     `json:"tag_entry,omitempty"`
	TagSL           string     `json:"tag_sl,omitempty"`
	Description     string     `json:"description,omitempty"`
}

type TradeForSLPolling struct {
	ID         string
	BotName    string
	Side       string
	Qty        int
	EntryPrice float64
	Stoploss   float64
	Taxes      float64
	SLOrderIDs []string
}
