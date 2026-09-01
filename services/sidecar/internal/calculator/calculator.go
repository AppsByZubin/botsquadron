package calculator

import (
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/AppsByZubin/botsquadron/services/sidecar/internal/model"
)

// ClassifyMarket exactly ports garageforbots' classify_market thresholds.
func ClassifyMarket(puller, dragger, niftyPreviousClose float64) string {
	puller = math.Max(0, puller)
	draggerAbs := math.Abs(math.Min(0, dragger))
	netPercent := (puller - draggerAbs) / niftyPreviousClose * 100
	pullerDominance := puller / math.Max(draggerAbs, 1)
	draggerDominance := draggerAbs / math.Max(puller, 1)

	switch {
	case netPercent >= 0.50 && pullerDominance >= 2.0:
		return model.HeavyBullish
	case netPercent >= 0.15 && pullerDominance >= 1.25:
		return model.Bullish
	case netPercent <= -0.50 && draggerDominance >= 2.0:
		return model.HeavyBearish
	case netPercent <= -0.15 && draggerDominance >= 1.25:
		return model.Bearish
	default:
		return model.Neutral
	}
}

// SnapshotInput contains completed or carried-forward candles for one minute.
type SnapshotInput struct {
	Constituents       []model.Constituent
	CandleStart        time.Time
	IndexPreviousClose float64
	IndexClose         float64
	IndexCandle        *model.MinuteCandle
	Closes             map[string]float64
	PreviousCloses     map[string]float64
	Candles            map[string]*model.MinuteCandle
	Fresh              map[string]bool
}

// Calculate builds a reference-compatible puller/dragger snapshot.
func Calculate(input SnapshotInput) (model.Snapshot, error) {
	if !isPositiveFinite(input.IndexPreviousClose) || !isPositiveFinite(input.IndexClose) {
		return model.Snapshot{}, fmt.Errorf("NIFTY previous/current close must be positive and finite")
	}
	if len(input.Constituents) == 0 {
		return model.Snapshot{}, fmt.Errorf("no constituents configured")
	}

	pullers := make([]model.Contribution, 0, len(input.Constituents))
	draggers := make([]model.Contribution, 0, len(input.Constituents))
	unchanged := make([]model.Contribution, 0, len(input.Constituents))
	freshCount := 0
	coverageWeight := 0.0
	for _, constituent := range input.Constituents {
		closePrice := input.Closes[constituent.InstrumentKey]
		previousClose := input.PreviousCloses[constituent.InstrumentKey]
		if !isPositiveFinite(closePrice) || !isPositiveFinite(previousClose) {
			return model.Snapshot{}, fmt.Errorf("missing valid close for %s", constituent.InstrumentKey)
		}
		move := closePrice/previousClose - 1
		points := input.IndexPreviousClose * (constituent.WeightPercent / 100) * move
		row := model.Contribution{
			Symbol: constituent.Symbol, Name: constituent.Name,
			InstrumentKey: constituent.InstrumentKey, WeightPercent: constituent.WeightPercent,
			LTP: closePrice, PreviousClose: previousClose, ChangePercent: move * 100,
			Points: points, Candle: input.Candles[constituent.InstrumentKey],
			CarriedForward: !input.Fresh[constituent.InstrumentKey],
		}
		coverageWeight += constituent.WeightPercent
		if input.Fresh[constituent.InstrumentKey] {
			freshCount++
		}
		switch {
		case points > 0:
			pullers = append(pullers, row)
		case points < 0:
			draggers = append(draggers, row)
		default:
			unchanged = append(unchanged, row)
		}
	}

	sort.SliceStable(pullers, func(i, j int) bool {
		if pullers[i].Points == pullers[j].Points {
			return pullers[i].Symbol < pullers[j].Symbol
		}
		return pullers[i].Points > pullers[j].Points
	})
	sort.SliceStable(draggers, func(i, j int) bool {
		if draggers[i].Points == draggers[j].Points {
			return draggers[i].Symbol < draggers[j].Symbol
		}
		return draggers[i].Points < draggers[j].Points
	})
	sort.SliceStable(unchanged, func(i, j int) bool { return unchanged[i].Symbol < unchanged[j].Symbol })

	pullerValue := sumPoints(pullers)
	draggerValue := sumPoints(draggers)
	netValue := pullerValue + draggerValue
	actualMove := input.IndexClose - input.IndexPreviousClose
	availableAt := input.CandleStart.Add(time.Minute)
	contributions := make([]model.Contribution, 0, len(input.Constituents))
	contributions = append(contributions, pullers...)
	contributions = append(contributions, unchanged...)
	for position := len(draggers) - 1; position >= 0; position-- {
		contributions = append(contributions, draggers[position])
	}

	return model.Snapshot{
		ExecutionDate: input.CandleStart.Format("2006-01-02"),
		Datetime:      availableAt.Format(time.RFC3339), Timestamp: availableAt.Format(time.RFC3339),
		CandleStart: input.CandleStart.Format(time.RFC3339),
		IndexLTP:    input.IndexClose, IndexPreviousClose: input.IndexPreviousClose,
		ActualIndexMove: actualMove, PullerValue: pullerValue, DraggerValue: draggerValue,
		NetValue: netValue, MarketClassification: ClassifyMarket(pullerValue, draggerValue, input.IndexPreviousClose),
		ResidualActualMinusCalculated: actualMove - netValue,
		CoverageCount:                 len(input.Constituents), ExpectedCount: len(input.Constituents),
		CoverageWeightPercent: coverageWeight, FreshCount: freshCount,
		IndexCandle: input.IndexCandle, Contributions: contributions,
	}, nil
}

func sumPoints(rows []model.Contribution) float64 {
	total := 0.0
	for _, row := range rows {
		total += row.Points
	}
	return total
}
