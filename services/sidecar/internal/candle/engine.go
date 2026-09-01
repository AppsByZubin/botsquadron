package candle

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/AppsByZubin/botsquadron/services/sidecar/internal/calculator"
	"github.com/AppsByZubin/botsquadron/services/sidecar/internal/model"
)

type rawCandle struct {
	start               time.Time
	firstTick, lastTick time.Time
	open, high, low     float64
	close               float64
	tickCount           int
}

func newRawCandle(start, tickTime time.Time, price float64) *rawCandle {
	return &rawCandle{
		start: start, firstTick: tickTime, lastTick: tickTime,
		open: price, high: price, low: price, close: price, tickCount: 1,
	}
}

func (candle *rawCandle) update(tickTime time.Time, price float64) {
	if price > candle.high {
		candle.high = price
	}
	if price < candle.low {
		candle.low = price
	}
	if tickTime.Before(candle.firstTick) {
		candle.firstTick = tickTime
		candle.open = price
	}
	if !tickTime.Before(candle.lastTick) {
		candle.lastTick = tickTime
		candle.close = price
	}
	candle.tickCount++
}

func (candle *rawCandle) export() *model.MinuteCandle {
	return &model.MinuteCandle{
		Start: candle.start.Format(time.RFC3339),
		End:   candle.start.Add(time.Minute).Format(time.RFC3339),
		Open:  candle.open, High: candle.high, Low: candle.low, Close: candle.close,
		TickCount: candle.tickCount,
	}
}

// Engine builds exchange-time one-minute candles and emits completed snapshots.
// All exported methods are safe for concurrent NATS and HTTP goroutines.
type Engine struct {
	mu sync.RWMutex

	constituents []model.Constituent
	byKey        map[string]model.Constituent
	indexKey     string
	location     *time.Location
	sessionOpen  int
	sessionClose int

	activeDate     string
	previousCloses map[string]float64
	candles        map[time.Time]map[string]*rawCandle
	pendingMinutes map[time.Time]struct{}
	lastCandles    map[string]*model.MinuteCandle
	lastCloses     map[string]float64
	lastFinalized  time.Time
}

func NewEngine(
	constituents []model.Constituent,
	indexKey string,
	location *time.Location,
	sessionOpen, sessionClose string,
) (*Engine, error) {
	if location == nil {
		return nil, fmt.Errorf("timezone is required")
	}
	openMinute, err := parseClock(sessionOpen)
	if err != nil {
		return nil, fmt.Errorf("parse market open: %w", err)
	}
	closeMinute, err := parseClock(sessionClose)
	if err != nil {
		return nil, fmt.Errorf("parse market close: %w", err)
	}
	if openMinute >= closeMinute {
		return nil, fmt.Errorf("market open must be before market close")
	}
	if strings.TrimSpace(indexKey) == "" {
		return nil, fmt.Errorf("index instrument key is required")
	}
	if len(constituents) == 0 {
		return nil, fmt.Errorf("at least one constituent is required")
	}
	byKey := make(map[string]model.Constituent, len(constituents))
	for _, constituent := range constituents {
		if _, exists := byKey[constituent.InstrumentKey]; exists {
			return nil, fmt.Errorf("duplicate instrument key %q", constituent.InstrumentKey)
		}
		byKey[constituent.InstrumentKey] = constituent
	}
	if _, exists := byKey[indexKey]; exists {
		return nil, fmt.Errorf("index instrument key %q duplicates a constituent", indexKey)
	}
	return &Engine{
		constituents: append([]model.Constituent(nil), constituents...),
		byKey:        byKey, indexKey: indexKey, location: location,
		sessionOpen: openMinute, sessionClose: closeMinute,
		previousCloses: make(map[string]float64),
		candles:        make(map[time.Time]map[string]*rawCandle),
		pendingMinutes: make(map[time.Time]struct{}),
		lastCandles:    make(map[string]*model.MinuteCandle),
		lastCloses:     make(map[string]float64),
	}, nil
}

// InstrumentKeys returns the exact 50-stock basket plus the required NIFTY key.
func (engine *Engine) InstrumentKeys() []string {
	engine.mu.RLock()
	defer engine.mu.RUnlock()
	keys := make([]string, 0, len(engine.constituents)+1)
	for _, constituent := range engine.constituents {
		keys = append(keys, constituent.InstrumentKey)
	}
	keys = append(keys, engine.indexKey)
	return keys
}

// AddTick records a valid current-session tick. Previous close metadata is kept
// even for an initial feed whose last-trade timestamp is from an older session.
func (engine *Engine) AddTick(tick model.Tick) bool {
	if !positiveFinite(tick.Price) && !positiveFinite(tick.PreviousClose) {
		return false
	}
	engine.mu.Lock()
	defer engine.mu.Unlock()

	if tick.InstrumentKey != engine.indexKey {
		if _, exists := engine.byKey[tick.InstrumentKey]; !exists {
			return false
		}
	}
	dayTime := tick.ObservedAt
	if dayTime.IsZero() {
		dayTime = tick.Timestamp
	}
	if dayTime.IsZero() {
		return false
	}
	dayTime = dayTime.In(engine.location)
	day := dayTime.Format("2006-01-02")
	if engine.activeDate == "" || day > engine.activeDate {
		engine.resetDayLocked(day)
	} else if day < engine.activeDate {
		return false
	}
	if positiveFinite(tick.PreviousClose) {
		engine.previousCloses[tick.InstrumentKey] = tick.PreviousClose
	}
	if !positiveFinite(tick.Price) || tick.Timestamp.IsZero() {
		return false
	}

	eventTime := tick.Timestamp.In(engine.location)
	if eventTime.Format("2006-01-02") != engine.activeDate {
		return false
	}
	minute := eventTime.Truncate(time.Minute)
	clockMinute := minute.Hour()*60 + minute.Minute()
	if clockMinute < engine.sessionOpen || clockMinute >= engine.sessionClose {
		return false
	}
	if !engine.lastFinalized.IsZero() && !minute.After(engine.lastFinalized) {
		return false
	}
	byInstrument := engine.candles[minute]
	if byInstrument == nil {
		byInstrument = make(map[string]*rawCandle)
		engine.candles[minute] = byInstrument
	}
	if current := byInstrument[tick.InstrumentKey]; current != nil {
		current.update(eventTime, tick.Price)
	} else {
		byInstrument[tick.InstrumentKey] = newRawCandle(minute, eventTime, tick.Price)
	}
	engine.pendingMinutes[minute] = struct{}{}
	return true
}

// FinalizeBefore emits every pending candle whose end is not later than cutoff.
// If previous closes are incomplete, pending candles remain available for retry.
func (engine *Engine) FinalizeBefore(cutoff time.Time) ([]model.Snapshot, error) {
	engine.mu.Lock()
	defer engine.mu.Unlock()
	if !engine.calculationReadyLocked() {
		return nil, nil
	}
	cutoff = cutoff.In(engine.location)
	minutes := make([]time.Time, 0, len(engine.pendingMinutes))
	for minute := range engine.pendingMinutes {
		if !minute.Add(time.Minute).After(cutoff) {
			minutes = append(minutes, minute)
		}
	}
	sort.Slice(minutes, func(i, j int) bool { return minutes[i].Before(minutes[j]) })

	snapshots := make([]model.Snapshot, 0, len(minutes))
	for _, minute := range minutes {
		snapshot, err := engine.finalizeMinuteLocked(minute)
		if err != nil {
			return snapshots, err
		}
		snapshots = append(snapshots, snapshot)
		delete(engine.pendingMinutes, minute)
		delete(engine.candles, minute)
		engine.lastFinalized = minute
	}
	return snapshots, nil
}

func (engine *Engine) finalizeMinuteLocked(minute time.Time) (model.Snapshot, error) {
	current := engine.candles[minute]
	closes := make(map[string]float64, len(engine.constituents))
	candles := make(map[string]*model.MinuteCandle, len(engine.constituents))
	fresh := make(map[string]bool, len(engine.constituents))
	for _, constituent := range engine.constituents {
		key := constituent.InstrumentKey
		if raw := current[key]; raw != nil {
			exported := raw.export()
			engine.lastCandles[key] = exported
			engine.lastCloses[key] = raw.close
			fresh[key] = true
		}
		closePrice := engine.lastCloses[key]
		if !positiveFinite(closePrice) {
			closePrice = engine.previousCloses[key]
		}
		closes[key] = closePrice
		candles[key] = engine.lastCandles[key]
	}

	indexCandle := engine.lastCandles[engine.indexKey]
	if raw := current[engine.indexKey]; raw != nil {
		indexCandle = raw.export()
		engine.lastCandles[engine.indexKey] = indexCandle
		engine.lastCloses[engine.indexKey] = raw.close
	}
	indexClose := engine.lastCloses[engine.indexKey]
	if !positiveFinite(indexClose) {
		indexClose = engine.previousCloses[engine.indexKey]
	}
	return calculator.Calculate(calculator.SnapshotInput{
		Constituents: engine.constituents, CandleStart: minute,
		IndexPreviousClose: engine.previousCloses[engine.indexKey], IndexClose: indexClose,
		IndexCandle: indexCandle, Closes: closes, PreviousCloses: engine.previousCloses,
		Candles: candles, Fresh: fresh,
	})
}

// Restore resumes carry-forward state from today's latest durable snapshot.
func (engine *Engine) Restore(snapshot model.Snapshot) error {
	start, err := time.Parse(time.RFC3339, snapshot.CandleStart)
	if err != nil {
		return fmt.Errorf("parse restored candle start: %w", err)
	}
	start = start.In(engine.location).Truncate(time.Minute)
	if snapshot.ExecutionDate != start.Format("2006-01-02") {
		return fmt.Errorf("restored execution date %q does not match candle start %s", snapshot.ExecutionDate, start.Format(time.RFC3339))
	}
	engine.mu.Lock()
	defer engine.mu.Unlock()
	engine.resetDayLocked(snapshot.ExecutionDate)
	engine.lastFinalized = start
	if positiveFinite(snapshot.IndexPreviousClose) {
		engine.previousCloses[engine.indexKey] = snapshot.IndexPreviousClose
	}
	if positiveFinite(snapshot.IndexLTP) {
		engine.lastCloses[engine.indexKey] = snapshot.IndexLTP
	}
	engine.lastCandles[engine.indexKey] = snapshot.IndexCandle
	for _, row := range snapshot.Contributions {
		if _, exists := engine.byKey[row.InstrumentKey]; !exists {
			continue
		}
		if positiveFinite(row.PreviousClose) {
			engine.previousCloses[row.InstrumentKey] = row.PreviousClose
		}
		if positiveFinite(row.LTP) {
			engine.lastCloses[row.InstrumentKey] = row.LTP
		}
		engine.lastCandles[row.InstrumentKey] = row.Candle
	}
	return nil
}

func (engine *Engine) Readiness(natsConnected bool) model.Readiness {
	engine.mu.RLock()
	defer engine.mu.RUnlock()
	missing := make([]string, 0)
	loaded := 0
	for _, constituent := range engine.constituents {
		if positiveFinite(engine.previousCloses[constituent.InstrumentKey]) {
			loaded++
		} else {
			missing = append(missing, constituent.InstrumentKey)
		}
	}
	indexReady := positiveFinite(engine.previousCloses[engine.indexKey])
	calculationReady := loaded == len(engine.constituents) && indexReady
	return model.Readiness{
		Ready:         natsConnected && calculationReady,
		NATSConnected: natsConnected, CalculationReady: calculationReady,
		ExpectedConstituents: len(engine.constituents), PreviousClosesLoaded: loaded,
		MissingPreviousCloses: missing, IndexPreviousClose: indexReady,
	}
}

func (engine *Engine) calculationReadyLocked() bool {
	if !positiveFinite(engine.previousCloses[engine.indexKey]) {
		return false
	}
	for _, constituent := range engine.constituents {
		if !positiveFinite(engine.previousCloses[constituent.InstrumentKey]) {
			return false
		}
	}
	return true
}

func (engine *Engine) resetDayLocked(day string) {
	engine.activeDate = day
	engine.previousCloses = make(map[string]float64)
	engine.candles = make(map[time.Time]map[string]*rawCandle)
	engine.pendingMinutes = make(map[time.Time]struct{})
	engine.lastCandles = make(map[string]*model.MinuteCandle)
	engine.lastCloses = make(map[string]float64)
	engine.lastFinalized = time.Time{}
}

func parseClock(value string) (int, error) {
	parsed, err := time.Parse("15:04", strings.TrimSpace(value))
	if err != nil {
		return 0, fmt.Errorf("%q must use HH:MM: %w", value, err)
	}
	return parsed.Hour()*60 + parsed.Minute(), nil
}

func positiveFinite(value float64) bool {
	return value > 0 && !math.IsNaN(value) && !math.IsInf(value, 0)
}
