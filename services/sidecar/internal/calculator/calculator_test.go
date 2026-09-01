package calculator

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/AppsByZubin/botsquadron/services/sidecar/internal/model"
)

func TestClassifyMarketReferenceVectors(t *testing.T) {
	t.Parallel()

	const niftyPreviousClose = 20_000.0
	tests := []struct {
		name    string
		puller  float64
		dragger float64
		want    string
	}{
		{name: "heavy bullish at inclusive thresholds", puller: 200, dragger: -100, want: model.HeavyBullish},
		{name: "bullish at inclusive thresholds", puller: 150, dragger: -120, want: model.Bullish},
		{name: "heavy net without heavy dominance is bullish", puller: 300, dragger: -200, want: model.Bullish},
		{name: "positive net without bullish dominance is neutral", puller: 180, dragger: -150, want: model.Neutral},
		{name: "heavy bearish at inclusive thresholds", puller: 100, dragger: -200, want: model.HeavyBearish},
		{name: "bearish at inclusive thresholds", puller: 120, dragger: -150, want: model.Bearish},
		{name: "heavy negative net without heavy dominance is bearish", puller: 200, dragger: -300, want: model.Bearish},
		{name: "negative net without bearish dominance is neutral", puller: 150, dragger: -180, want: model.Neutral},
		{name: "balanced contributions", puller: 60, dragger: -60, want: model.Neutral},
		{name: "positive dragger input is clamped away", puller: 200, dragger: 100, want: model.HeavyBullish},
		{name: "negative puller input is clamped away", puller: -100, dragger: -200, want: model.HeavyBearish},
		{name: "one point dominance floor", puller: 0.5, dragger: 0, want: model.Neutral},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := ClassifyMarket(test.puller, test.dragger, niftyPreviousClose); got != test.want {
				t.Fatalf("ClassifyMarket(%v, %v, %v) = %q, want %q", test.puller, test.dragger, niftyPreviousClose, got, test.want)
			}
		})
	}
}

func TestCalculateReferenceFiftyConstituentExample(t *testing.T) {
	t.Parallel()

	location := time.FixedZone("IST", 5*60*60+30*60)
	candleStart := time.Date(2026, time.August, 12, 9, 15, 0, 0, location)
	constituents := make([]model.Constituent, 0, 50)
	closes := make(map[string]float64, 50)
	previousCloses := make(map[string]float64, 50)
	candles := make(map[string]*model.MinuteCandle, 50)
	fresh := make(map[string]bool, 50)

	for position := 0; position < 50; position++ {
		symbol := fmt.Sprintf("STOCK%02d", position)
		key := fmt.Sprintf("NSE_EQ|TEST%02d", position)
		constituents = append(constituents, model.Constituent{
			Symbol: symbol, Name: "Stock " + symbol,
			InstrumentKey: key, WeightPercent: 2,
		})
		closePrice := 100.0
		switch position {
		case 0:
			closePrice = 110
		case 1:
			closePrice = 105
		case 2:
			closePrice = 90
		case 3:
			closePrice = 95
		}
		closes[key] = closePrice
		previousCloses[key] = 100
		candles[key] = &model.MinuteCandle{
			Start: candleStart.Format(time.RFC3339),
			End:   candleStart.Add(time.Minute).Format(time.RFC3339),
			Open:  closePrice, High: closePrice, Low: closePrice, Close: closePrice,
			TickCount: 1,
		}
		fresh[key] = true
	}

	snapshot, err := Calculate(SnapshotInput{
		Constituents:       constituents,
		CandleStart:        candleStart,
		IndexPreviousClose: 20_000,
		IndexClose:         20_000,
		Closes:             closes,
		PreviousCloses:     previousCloses,
		Candles:            candles,
		Fresh:              fresh,
	})
	if err != nil {
		t.Fatalf("Calculate() error = %v", err)
	}

	assertFloatClose(t, "puller value", snapshot.PullerValue, 60)
	assertFloatClose(t, "signed dragger value", snapshot.DraggerValue, -60)
	assertFloatClose(t, "net value", snapshot.NetValue, 0)
	assertFloatClose(t, "coverage weight", snapshot.CoverageWeightPercent, 100)
	assertFloatClose(t, "residual", snapshot.ResidualActualMinusCalculated, 0)
	if snapshot.MarketClassification != model.Neutral {
		t.Fatalf("MarketClassification = %q, want %q", snapshot.MarketClassification, model.Neutral)
	}
	if snapshot.CoverageCount != 50 || snapshot.ExpectedCount != 50 || snapshot.FreshCount != 50 {
		t.Fatalf("coverage/fresh = %d/%d fresh=%d, want 50/50 fresh=50", snapshot.CoverageCount, snapshot.ExpectedCount, snapshot.FreshCount)
	}
	if snapshot.ExecutionDate != "2026-08-12" {
		t.Fatalf("ExecutionDate = %q, want 2026-08-12", snapshot.ExecutionDate)
	}
	if snapshot.CandleStart != "2026-08-12T09:15:00+05:30" {
		t.Fatalf("CandleStart = %q, want start-labelled minute", snapshot.CandleStart)
	}
	if snapshot.Datetime != "2026-08-12T09:16:00+05:30" || snapshot.Timestamp != snapshot.Datetime {
		t.Fatalf("Datetime/Timestamp = %q/%q, want availability time 09:16", snapshot.Datetime, snapshot.Timestamp)
	}
	if len(snapshot.Contributions) != 50 {
		t.Fatalf("len(Contributions) = %d, want 50", len(snapshot.Contributions))
	}

	wantOrder := []struct {
		position int
		symbol   string
		points   float64
	}{
		{position: 0, symbol: "STOCK00", points: 40},
		{position: 1, symbol: "STOCK01", points: 20},
		// Reference order reverses the already sorted draggers when exposing
		// the aggregate contributions list.
		{position: 48, symbol: "STOCK03", points: -20},
		{position: 49, symbol: "STOCK02", points: -40},
	}
	for _, want := range wantOrder {
		row := snapshot.Contributions[want.position]
		if row.Symbol != want.symbol {
			t.Errorf("Contributions[%d].Symbol = %q, want %q", want.position, row.Symbol, want.symbol)
		}
		assertFloatClose(t, fmt.Sprintf("Contributions[%d].Points", want.position), row.Points, want.points)
		if row.CarriedForward {
			t.Errorf("Contributions[%d].CarriedForward = true, want false", want.position)
		}
	}
}

func assertFloatClose(t *testing.T, label string, got, want float64) {
	t.Helper()
	const tolerance = 1e-9
	if math.Abs(got-want) > tolerance {
		t.Fatalf("%s = %.12f, want %.12f (tolerance %g)", label, got, want, tolerance)
	}
}
