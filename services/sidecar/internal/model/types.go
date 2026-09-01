package model

import "time"

const (
	SchemaVersion   = 1
	Nifty50IndexKey = "NSE_INDEX|Nifty 50"
	HeavyBullish    = "heavy_bullish"
	Bullish         = "bullish"
	Neutral         = "neutral"
	Bearish         = "bearish"
	HeavyBearish    = "heavy_bearish"
)

// Constituent is one row from the official NIFTY 50 weights file. WeightPercent
// is normalized by the loader so the full basket always totals exactly 100.
type Constituent struct {
	Symbol        string  `json:"symbol"`
	WeightPercent float64 `json:"weight_percent"`
	Name          string  `json:"name"`
	InstrumentKey string  `json:"instrument_key"`
}

// Tick is the normalized subset of a marketfeeder message used by the sidecar.
type Tick struct {
	InstrumentKey string
	Price         float64
	PreviousClose float64
	Timestamp     time.Time
	ObservedAt    time.Time
}

// MinuteCandle contains one instrument's exchange-time, one-minute OHLC.
type MinuteCandle struct {
	Start     string  `json:"start"`
	End       string  `json:"end"`
	Open      float64 `json:"open"`
	High      float64 `json:"high"`
	Low       float64 `json:"low"`
	Close     float64 `json:"close"`
	TickCount int     `json:"tick_count"`
}

// Contribution is a constituent's calculated NIFTY point contribution. Candle
// is nil before the first trade of the day; in that case PreviousClose is used.
type Contribution struct {
	Symbol         string        `json:"symbol"`
	Name           string        `json:"name"`
	InstrumentKey  string        `json:"instrument_key"`
	WeightPercent  float64       `json:"weight_percent"`
	LTP            float64       `json:"ltp"`
	PreviousClose  float64       `json:"previous_close"`
	ChangePercent  float64       `json:"change_percent"`
	Points         float64       `json:"points"`
	Candle         *MinuteCandle `json:"candle,omitempty"`
	CarriedForward bool          `json:"carried_forward"`
}

// Snapshot is emitted when a minute candle closes. Datetime/Timestamp are the
// candle's availability time (09:16 for the candle starting at 09:15), matching
// the garageforbots reference calculator.
type Snapshot struct {
	ExecutionDate                 string         `json:"execution_date"`
	Datetime                      string         `json:"datetime"`
	Timestamp                     string         `json:"timestamp"`
	CandleStart                   string         `json:"candle_start"`
	IndexLTP                      float64        `json:"index_ltp"`
	IndexPreviousClose            float64        `json:"index_previous_close"`
	ActualIndexMove               float64        `json:"actual_index_move"`
	PullerValue                   float64        `json:"puller_value"`
	DraggerValue                  float64        `json:"dragger_value"`
	NetValue                      float64        `json:"net_value"`
	MarketClassification          string         `json:"market_classification"`
	ResidualActualMinusCalculated float64        `json:"residual_actual_minus_calculated"`
	CoverageCount                 int            `json:"coverage_count"`
	ExpectedCount                 int            `json:"expected_count"`
	CoverageWeightPercent         float64        `json:"coverage_weight_percent"`
	FreshCount                    int            `json:"fresh_count"`
	IndexCandle                   *MinuteCandle  `json:"index_candle,omitempty"`
	Contributions                 []Contribution `json:"contributions"`
}

// SnapshotFile is the durable, versioned JSON document written by the service.
type SnapshotFile struct {
	SchemaVersion int        `json:"schema_version"`
	ExecutionDate string     `json:"execution_date,omitempty"`
	UpdatedAt     string     `json:"updated_at,omitempty"`
	Latest        *Snapshot  `json:"latest,omitempty"`
	Snapshots     []Snapshot `json:"snapshots"`
}

// Readiness describes whether exact reference-compatible snapshots can be made.
type Readiness struct {
	Ready                 bool     `json:"ready"`
	NATSConnected         bool     `json:"nats_connected"`
	CalculationReady      bool     `json:"calculation_ready"`
	ExpectedConstituents  int      `json:"expected_constituents"`
	PreviousClosesLoaded  int      `json:"previous_closes_loaded"`
	MissingPreviousCloses []string `json:"missing_previous_closes,omitempty"`
	IndexPreviousClose    bool     `json:"index_previous_close_loaded"`
	LatestDatetime        string   `json:"latest_datetime,omitempty"`
}
