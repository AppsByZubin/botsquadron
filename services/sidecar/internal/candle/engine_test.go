package candle

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/AppsByZubin/botsquadron/services/sidecar/internal/model"
)

const testIndexKey = model.Nifty50IndexKey

var testLocation = time.FixedZone("IST", 5*60*60+30*60)

func TestReadinessRequiresEveryConstituentAndIndexPreviousClose(t *testing.T) {
	t.Parallel()

	constituents := testConstituents(50)
	engine := newTestEngine(t, constituents)
	observedAt := testTimestamp(2026, time.August, 12, 9, 14, 55)

	for _, constituent := range constituents[:49] {
		addPreviousClose(t, engine, constituent.InstrumentKey, 100, observedAt)
	}
	readiness := engine.Readiness(true)
	if readiness.CalculationReady || readiness.Ready {
		t.Fatalf("engine became ready with one stock CP and the index CP missing: %+v", readiness)
	}
	if readiness.PreviousClosesLoaded != 49 || readiness.ExpectedConstituents != 50 {
		t.Fatalf("previous-close coverage = %d/%d, want 49/50", readiness.PreviousClosesLoaded, readiness.ExpectedConstituents)
	}
	if len(readiness.MissingPreviousCloses) != 1 || readiness.MissingPreviousCloses[0] != constituents[49].InstrumentKey {
		t.Fatalf("MissingPreviousCloses = %v, want [%s]", readiness.MissingPreviousCloses, constituents[49].InstrumentKey)
	}
	if readiness.IndexPreviousClose {
		t.Fatal("IndexPreviousClose = true before the NIFTY CP was received")
	}

	addPreviousClose(t, engine, testIndexKey, 20_000, observedAt)
	readiness = engine.Readiness(true)
	if readiness.CalculationReady || readiness.Ready {
		t.Fatalf("engine became ready while one constituent CP was missing: %+v", readiness)
	}
	if !readiness.IndexPreviousClose {
		t.Fatal("IndexPreviousClose = false after the NIFTY CP was received")
	}

	addPreviousClose(t, engine, constituents[49].InstrumentKey, 100, observedAt)
	readiness = engine.Readiness(false)
	if !readiness.CalculationReady {
		t.Fatalf("CalculationReady = false after all CPs were received: %+v", readiness)
	}
	if readiness.Ready {
		t.Fatal("Ready = true while NATS is disconnected")
	}
	readiness = engine.Readiness(true)
	if !readiness.Ready || !readiness.CalculationReady || readiness.PreviousClosesLoaded != 50 {
		t.Fatalf("fully initialized readiness = %+v, want ready with 50 CPs", readiness)
	}
}

func TestInitialFeedWithStaleExchangeDateLoadsCPButDoesNotCreateCandle(t *testing.T) {
	t.Parallel()

	constituents := testConstituents(2)
	engine := newTestEngine(t, constituents)
	observedAt := testTimestamp(2026, time.August, 12, 9, 14, 55)
	staleExchangeTime := testTimestamp(2026, time.August, 11, 15, 29, 30)

	initialFeeds := []model.Tick{
		{InstrumentKey: constituents[0].InstrumentKey, Price: 150, PreviousClose: 100, Timestamp: staleExchangeTime, ObservedAt: observedAt},
		{InstrumentKey: constituents[1].InstrumentKey, Price: 50, PreviousClose: 200, Timestamp: staleExchangeTime, ObservedAt: observedAt},
		{InstrumentKey: testIndexKey, Price: 21_000, PreviousClose: 20_000, Timestamp: staleExchangeTime, ObservedAt: observedAt},
	}
	for _, tick := range initialFeeds {
		if engine.AddTick(tick) {
			t.Errorf("AddTick(%s stale initial feed) = true, want metadata-only false", tick.InstrumentKey)
		}
	}
	if readiness := engine.Readiness(true); !readiness.Ready {
		t.Fatalf("stale initial feeds did not initialize CP readiness: %+v", readiness)
	}
	if snapshots, err := engine.FinalizeBefore(testTimestamp(2026, time.August, 12, 9, 16, 0)); err != nil {
		t.Fatalf("FinalizeBefore() error = %v", err)
	} else if len(snapshots) != 0 {
		t.Fatalf("stale initial feeds created %d snapshots, want none", len(snapshots))
	}

	minuteStart := testTimestamp(2026, time.August, 12, 9, 15, 0)
	if !engine.AddTick(model.Tick{
		InstrumentKey: testIndexKey,
		Price:         20_010,
		Timestamp:     minuteStart.Add(10 * time.Second),
		ObservedAt:    minuteStart.Add(10 * time.Second),
	}) {
		t.Fatal("current-session index tick was rejected")
	}
	snapshots, err := engine.FinalizeBefore(minuteStart.Add(time.Minute))
	if err != nil {
		t.Fatalf("FinalizeBefore() error = %v", err)
	}
	if len(snapshots) != 1 {
		t.Fatalf("len(snapshots) = %d, want 1", len(snapshots))
	}
	snapshot := snapshots[0]
	if snapshot.FreshCount != 0 {
		t.Fatalf("FreshCount = %d, want 0 carried constituents", snapshot.FreshCount)
	}
	assertCandleFloat(t, "index LTP", snapshot.IndexLTP, 20_010)
	for _, row := range snapshot.Contributions {
		if !row.CarriedForward || row.Candle != nil {
			t.Errorf("%s carried/candle = %v/%+v, want true/nil", row.InstrumentKey, row.CarriedForward, row.Candle)
		}
		assertCandleFloat(t, row.InstrumentKey+" LTP", row.LTP, row.PreviousClose)
	}
}

func TestFinalizeAtMinuteAvailabilityBuildsOHLC(t *testing.T) {
	t.Parallel()

	constituents := testConstituents(2)
	engine := newTestEngine(t, constituents)
	minuteStart := testTimestamp(2026, time.August, 12, 9, 15, 0)
	seedAllPreviousCloses(t, engine, constituents, minuteStart.Add(-time.Second), []float64{100, 200}, 20_000)

	for position, price := range []float64{100, 105, 99, 102} {
		at := minuteStart.Add(time.Duration(position*10+1) * time.Second)
		if !engine.AddTick(model.Tick{InstrumentKey: constituents[0].InstrumentKey, Price: price, Timestamp: at, ObservedAt: at}) {
			t.Fatalf("stock AddTick(position=%d, price=%v) = false", position, price)
		}
	}
	for position, price := range []float64{20_000, 20_010, 19_990, 20_005} {
		at := minuteStart.Add(time.Duration(position*10+2) * time.Second)
		if !engine.AddTick(model.Tick{InstrumentKey: testIndexKey, Price: price, Timestamp: at, ObservedAt: at}) {
			t.Fatalf("index AddTick(position=%d, price=%v) = false", position, price)
		}
	}

	beforeClose := minuteStart.Add(time.Minute).Add(-time.Nanosecond)
	if snapshots, err := engine.FinalizeBefore(beforeClose); err != nil {
		t.Fatalf("FinalizeBefore(before close) error = %v", err)
	} else if len(snapshots) != 0 {
		t.Fatalf("FinalizeBefore(before close) emitted %d snapshots, want 0", len(snapshots))
	}

	snapshots, err := engine.FinalizeBefore(minuteStart.Add(time.Minute))
	if err != nil {
		t.Fatalf("FinalizeBefore(at close) error = %v", err)
	}
	if len(snapshots) != 1 {
		t.Fatalf("len(snapshots) = %d, want 1", len(snapshots))
	}
	snapshot := snapshots[0]
	if snapshot.CandleStart != "2026-08-12T09:15:00+05:30" {
		t.Fatalf("CandleStart = %q, want 09:15 start label", snapshot.CandleStart)
	}
	if snapshot.Datetime != "2026-08-12T09:16:00+05:30" || snapshot.Timestamp != snapshot.Datetime {
		t.Fatalf("Datetime/Timestamp = %q/%q, want 09:16 availability", snapshot.Datetime, snapshot.Timestamp)
	}
	if snapshot.FreshCount != 1 {
		t.Fatalf("FreshCount = %d, want 1", snapshot.FreshCount)
	}

	stock := contributionByKey(t, snapshot, constituents[0].InstrumentKey)
	assertMinuteCandle(t, "stock candle", stock.Candle, minuteStart, 100, 105, 99, 102, 4)
	if stock.CarriedForward {
		t.Fatal("fresh stock was marked carried forward")
	}
	carried := contributionByKey(t, snapshot, constituents[1].InstrumentKey)
	if !carried.CarriedForward || carried.Candle != nil {
		t.Fatalf("unticked stock carried/candle = %v/%+v, want true/nil", carried.CarriedForward, carried.Candle)
	}
	assertCandleFloat(t, "carried stock LTP", carried.LTP, 200)
	assertMinuteCandle(t, "index candle", snapshot.IndexCandle, minuteStart, 20_000, 20_010, 19_990, 20_005, 4)

	if duplicate, err := engine.FinalizeBefore(minuteStart.Add(2 * time.Minute)); err != nil {
		t.Fatalf("second FinalizeBefore() error = %v", err)
	} else if len(duplicate) != 0 {
		t.Fatalf("second FinalizeBefore() emitted %d duplicate snapshots", len(duplicate))
	}
}

func TestMissingMinuteCarriesPastCloseAndNeverUsesFutureCandle(t *testing.T) {
	t.Parallel()

	constituents := testConstituents(2)
	engine := newTestEngine(t, constituents)
	start := testTimestamp(2026, time.August, 12, 9, 15, 0)
	seedAllPreviousCloses(t, engine, constituents, start.Add(-time.Second), []float64{100, 100}, 20_000)

	addAcceptedTick(t, engine, constituents[0].InstrumentKey, 101, start.Add(20*time.Second))
	addAcceptedTick(t, engine, testIndexKey, 20_000, start.Add(30*time.Second))
	addAcceptedTick(t, engine, testIndexKey, 20_010, start.Add(time.Minute+30*time.Second))
	// Insert the 09:17 future candle before 09:15 and 09:16 are finalized. A
	// correct engine must not let it leak backwards into either snapshot.
	addAcceptedTick(t, engine, constituents[0].InstrumentKey, 150, start.Add(2*time.Minute+20*time.Second))
	addAcceptedTick(t, engine, testIndexKey, 20_020, start.Add(2*time.Minute+30*time.Second))

	snapshots, err := engine.FinalizeBefore(start.Add(2 * time.Minute))
	if err != nil {
		t.Fatalf("FinalizeBefore(09:17) error = %v", err)
	}
	if len(snapshots) != 2 {
		t.Fatalf("len(snapshots through 09:17) = %d, want 2", len(snapshots))
	}
	first := contributionByKey(t, snapshots[0], constituents[0].InstrumentKey)
	assertCandleFloat(t, "09:15 stock LTP", first.LTP, 101)
	if first.CarriedForward {
		t.Fatal("09:15 exact stock candle was marked carried")
	}
	assertMinuteCandle(t, "09:15 source candle", first.Candle, start, 101, 101, 101, 101, 1)

	missingMinute := contributionByKey(t, snapshots[1], constituents[0].InstrumentKey)
	assertCandleFloat(t, "09:16 carried LTP", missingMinute.LTP, 101)
	if !missingMinute.CarriedForward {
		t.Fatal("09:16 missing stock candle was not marked carried")
	}
	assertMinuteCandle(t, "09:16 carried source candle", missingMinute.Candle, start, 101, 101, 101, 101, 1)

	later, err := engine.FinalizeBefore(start.Add(3 * time.Minute))
	if err != nil {
		t.Fatalf("FinalizeBefore(09:18) error = %v", err)
	}
	if len(later) != 1 {
		t.Fatalf("len(09:17 snapshots) = %d, want 1", len(later))
	}
	future := contributionByKey(t, later[0], constituents[0].InstrumentKey)
	assertCandleFloat(t, "09:17 stock LTP", future.LTP, 150)
	if future.CarriedForward {
		t.Fatal("09:17 exact stock candle was marked carried")
	}
}

func TestTickForFinalizedMinuteIsRejected(t *testing.T) {
	t.Parallel()

	constituents := testConstituents(1)
	engine := newTestEngine(t, constituents)
	start := testTimestamp(2026, time.August, 12, 9, 15, 0)
	seedAllPreviousCloses(t, engine, constituents, start.Add(-time.Second), []float64{100}, 20_000)
	addAcceptedTick(t, engine, constituents[0].InstrumentKey, 101, start.Add(10*time.Second))
	addAcceptedTick(t, engine, testIndexKey, 20_000, start.Add(20*time.Second))
	if snapshots, err := engine.FinalizeBefore(start.Add(time.Minute)); err != nil {
		t.Fatalf("FinalizeBefore() error = %v", err)
	} else if len(snapshots) != 1 {
		t.Fatalf("len(initial snapshots) = %d, want 1", len(snapshots))
	}

	lateAt := start.Add(59 * time.Second)
	if engine.AddTick(model.Tick{
		InstrumentKey: constituents[0].InstrumentKey,
		Price:         150,
		Timestamp:     lateAt,
		ObservedAt:    start.Add(time.Minute + 5*time.Second),
	}) {
		t.Fatal("late tick for finalized 09:15 minute was accepted")
	}
	addAcceptedTick(t, engine, testIndexKey, 20_010, start.Add(time.Minute+20*time.Second))
	snapshots, err := engine.FinalizeBefore(start.Add(2 * time.Minute))
	if err != nil {
		t.Fatalf("FinalizeBefore(next minute) error = %v", err)
	}
	if len(snapshots) != 1 {
		t.Fatalf("len(next-minute snapshots) = %d, want 1", len(snapshots))
	}
	carried := contributionByKey(t, snapshots[0], constituents[0].InstrumentKey)
	assertCandleFloat(t, "post-late carried LTP", carried.LTP, 101)
	if !carried.CarriedForward {
		t.Fatal("post-late missing minute was not marked carried")
	}
}

func TestRestoreResumesPreviousClosesAndCarryForward(t *testing.T) {
	t.Parallel()

	constituents := testConstituents(2)
	start := testTimestamp(2026, time.August, 12, 9, 15, 0)
	source := newTestEngine(t, constituents)
	seedAllPreviousCloses(t, source, constituents, start.Add(-time.Second), []float64{100, 200}, 20_000)
	addAcceptedTick(t, source, constituents[0].InstrumentKey, 101, start.Add(10*time.Second))
	addAcceptedTick(t, source, constituents[1].InstrumentKey, 198, start.Add(20*time.Second))
	addAcceptedTick(t, source, testIndexKey, 20_010, start.Add(30*time.Second))
	initial, err := source.FinalizeBefore(start.Add(time.Minute))
	if err != nil {
		t.Fatalf("source FinalizeBefore() error = %v", err)
	}
	if len(initial) != 1 {
		t.Fatalf("len(source snapshots) = %d, want 1", len(initial))
	}

	restored := newTestEngine(t, constituents)
	if err := restored.Restore(initial[0]); err != nil {
		t.Fatalf("Restore() error = %v", err)
	}
	if readiness := restored.Readiness(true); !readiness.Ready || readiness.PreviousClosesLoaded != 2 || !readiness.IndexPreviousClose {
		t.Fatalf("restored readiness = %+v, want fully ready", readiness)
	}
	if restored.AddTick(model.Tick{
		InstrumentKey: constituents[0].InstrumentKey,
		Price:         999,
		Timestamp:     start.Add(50 * time.Second),
		ObservedAt:    start.Add(time.Minute + 5*time.Second),
	}) {
		t.Fatal("restored engine accepted a tick from its last finalized minute")
	}

	addAcceptedTick(t, restored, testIndexKey, 20_020, start.Add(time.Minute+20*time.Second))
	resumed, err := restored.FinalizeBefore(start.Add(2 * time.Minute))
	if err != nil {
		t.Fatalf("restored FinalizeBefore() error = %v", err)
	}
	if len(resumed) != 1 {
		t.Fatalf("len(resumed snapshots) = %d, want 1", len(resumed))
	}
	if resumed[0].Timestamp != "2026-08-12T09:17:00+05:30" {
		t.Fatalf("resumed Timestamp = %q, want 09:17 availability", resumed[0].Timestamp)
	}
	if resumed[0].FreshCount != 0 {
		t.Fatalf("resumed FreshCount = %d, want 0 carried stocks", resumed[0].FreshCount)
	}
	assertCandleFloat(t, "resumed index LTP", resumed[0].IndexLTP, 20_020)
	wantLTP := map[string]float64{
		constituents[0].InstrumentKey: 101,
		constituents[1].InstrumentKey: 198,
	}
	for key, want := range wantLTP {
		row := contributionByKey(t, resumed[0], key)
		assertCandleFloat(t, key+" restored LTP", row.LTP, want)
		if !row.CarriedForward {
			t.Errorf("%s was not carried after restore", key)
		}
		if row.Candle == nil || row.Candle.Start != start.Format(time.RFC3339) {
			t.Errorf("%s restored source candle = %+v, want 09:15 candle", key, row.Candle)
		}
	}
}

func testConstituents(count int) []model.Constituent {
	constituents := make([]model.Constituent, 0, count)
	for position := 0; position < count; position++ {
		constituents = append(constituents, model.Constituent{
			Symbol:        fmt.Sprintf("STOCK%02d", position),
			Name:          fmt.Sprintf("Stock %02d", position),
			InstrumentKey: fmt.Sprintf("NSE_EQ|TEST%02d", position),
			WeightPercent: 100 / float64(count),
		})
	}
	return constituents
}

func newTestEngine(t *testing.T, constituents []model.Constituent) *Engine {
	t.Helper()
	engine, err := NewEngine(constituents, testIndexKey, testLocation, "09:15", "15:30")
	if err != nil {
		t.Fatalf("NewEngine() error = %v", err)
	}
	return engine
}

func testTimestamp(year int, month time.Month, day, hour, minute, second int) time.Time {
	return time.Date(year, month, day, hour, minute, second, 0, testLocation)
}

func addPreviousClose(t *testing.T, engine *Engine, key string, previousClose float64, observedAt time.Time) {
	t.Helper()
	if engine.AddTick(model.Tick{InstrumentKey: key, PreviousClose: previousClose, ObservedAt: observedAt}) {
		t.Fatalf("metadata-only AddTick(%s) = true, want false", key)
	}
}

func seedAllPreviousCloses(
	t *testing.T,
	engine *Engine,
	constituents []model.Constituent,
	observedAt time.Time,
	previousCloses []float64,
	indexPreviousClose float64,
) {
	t.Helper()
	if len(previousCloses) != len(constituents) {
		t.Fatalf("test setup has %d previous closes for %d constituents", len(previousCloses), len(constituents))
	}
	for position, constituent := range constituents {
		addPreviousClose(t, engine, constituent.InstrumentKey, previousCloses[position], observedAt)
	}
	addPreviousClose(t, engine, testIndexKey, indexPreviousClose, observedAt)
}

func addAcceptedTick(t *testing.T, engine *Engine, key string, price float64, timestamp time.Time) {
	t.Helper()
	if !engine.AddTick(model.Tick{
		InstrumentKey: key,
		Price:         price,
		Timestamp:     timestamp,
		ObservedAt:    timestamp,
	}) {
		t.Fatalf("AddTick(%s, %.4f, %s) = false", key, price, timestamp.Format(time.RFC3339Nano))
	}
}

func contributionByKey(t *testing.T, snapshot model.Snapshot, key string) model.Contribution {
	t.Helper()
	for _, row := range snapshot.Contributions {
		if row.InstrumentKey == key {
			return row
		}
	}
	t.Fatalf("snapshot has no contribution for %s", key)
	return model.Contribution{}
}

func assertMinuteCandle(
	t *testing.T,
	label string,
	candle *model.MinuteCandle,
	start time.Time,
	open, high, low, close float64,
	tickCount int,
) {
	t.Helper()
	if candle == nil {
		t.Fatalf("%s = nil", label)
	}
	if candle.Start != start.Format(time.RFC3339) || candle.End != start.Add(time.Minute).Format(time.RFC3339) {
		t.Fatalf("%s start/end = %q/%q, want %q/%q", label, candle.Start, candle.End, start.Format(time.RFC3339), start.Add(time.Minute).Format(time.RFC3339))
	}
	assertCandleFloat(t, label+" open", candle.Open, open)
	assertCandleFloat(t, label+" high", candle.High, high)
	assertCandleFloat(t, label+" low", candle.Low, low)
	assertCandleFloat(t, label+" close", candle.Close, close)
	if candle.TickCount != tickCount {
		t.Fatalf("%s TickCount = %d, want %d", label, candle.TickCount, tickCount)
	}
}

func assertCandleFloat(t *testing.T, label string, got, want float64) {
	t.Helper()
	const tolerance = 1e-9
	if math.Abs(got-want) > tolerance {
		t.Fatalf("%s = %.12f, want %.12f (tolerance %g)", label, got, want, tolerance)
	}
}
