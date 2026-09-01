package natsfeed

import (
	"fmt"
	"testing"
	"time"
)

func TestDecodeTicksDecodesFullMarketAndIndexFeeds(t *testing.T) {
	t.Parallel()

	observedAt := time.Date(2026, time.August, 31, 9, 17, 3, 0, time.FixedZone("IST", 5*60*60+30*60))
	marketAt := observedAt.Add(-3 * time.Second)
	indexAt := observedAt.Add(-2 * time.Second)
	payload := []byte(fmt.Sprintf(`{
		"type":"live_feed",
		"feeds":{
			"NSE_EQ|INE040A01034":{"fullFeed":{"marketFF":{"ltpc":{"ltp":987.65,"ltt":%d,"cp":975.25}}}},
			"NSE_INDEX|Nifty 50":{"fullFeed":{"indexFF":{"ltpc":{"ltp":25001.5,"ltt":%d,"cp":24950.0}}}}
		},
		"currentTs":"%d"
	}`, marketAt.UnixMilli(), indexAt.UnixMilli(), observedAt.UnixMilli()))

	ticks, err := DecodeTicks(payload)
	if err != nil {
		t.Fatalf("DecodeTicks returned error: %v", err)
	}
	if len(ticks) != 2 {
		t.Fatalf("len(ticks) = %d, want 2", len(ticks))
	}

	byInstrument := make(map[string]struct {
		price         float64
		previousClose float64
		timestamp     time.Time
		observedAt    time.Time
	}, len(ticks))
	for _, tick := range ticks {
		byInstrument[tick.InstrumentKey] = struct {
			price         float64
			previousClose float64
			timestamp     time.Time
			observedAt    time.Time
		}{tick.Price, tick.PreviousClose, tick.Timestamp, tick.ObservedAt}
	}

	market, ok := byInstrument["NSE_EQ|INE040A01034"]
	if !ok {
		t.Fatal("marketFF tick is missing")
	}
	if market.price != 987.65 || market.previousClose != 975.25 {
		t.Fatalf("marketFF prices = (%v, %v), want (987.65, 975.25)", market.price, market.previousClose)
	}
	if !market.timestamp.Equal(marketAt) {
		t.Fatalf("marketFF Timestamp = %s, want %s", market.timestamp, marketAt)
	}
	if !market.observedAt.Equal(observedAt) {
		t.Fatalf("marketFF ObservedAt = %s, want %s", market.observedAt, observedAt)
	}

	index, ok := byInstrument["NSE_INDEX|Nifty 50"]
	if !ok {
		t.Fatal("indexFF tick is missing")
	}
	if index.price != 25001.5 || index.previousClose != 24950.0 {
		t.Fatalf("indexFF prices = (%v, %v), want (25001.5, 24950)", index.price, index.previousClose)
	}
	if !index.timestamp.Equal(indexAt) {
		t.Fatalf("indexFF Timestamp = %s, want %s", index.timestamp, indexAt)
	}
	if !index.observedAt.Equal(observedAt) {
		t.Fatalf("indexFF ObservedAt = %s, want %s", index.observedAt, observedAt)
	}
}

func TestDecodeTicksPreservesStaleEventTimeAndObservedTimeMetadata(t *testing.T) {
	t.Parallel()

	eventTime := time.Date(2026, time.August, 31, 9, 15, 12, 0, time.UTC)
	observedAt := eventTime.Add(4*time.Minute + 48*time.Second)
	payload := []byte(fmt.Sprintf(`{
		"feeds":{"NSE_EQ|STALE":{"fullFeed":{"marketFF":{"ltpc":{"ltp":101.25,"ltt":%d,"cp":100}}}}},
		"currentTs":%d
	}`, eventTime.UnixMilli(), observedAt.UnixMilli()))

	ticks, err := DecodeTicks(payload)
	if err != nil {
		t.Fatalf("DecodeTicks returned error: %v", err)
	}
	if len(ticks) != 1 {
		t.Fatalf("len(ticks) = %d, want 1", len(ticks))
	}
	if !ticks[0].Timestamp.Equal(eventTime) {
		t.Fatalf("Timestamp = %s, want stale exchange time %s", ticks[0].Timestamp, eventTime)
	}
	if !ticks[0].ObservedAt.Equal(observedAt) {
		t.Fatalf("ObservedAt = %s, want receipt metadata %s", ticks[0].ObservedAt, observedAt)
	}
	if got := ticks[0].ObservedAt.Sub(ticks[0].Timestamp); got != 4*time.Minute+48*time.Second {
		t.Fatalf("observed/event delta = %s, want 4m48s", got)
	}
}

func TestDecodeTicksDecodesFlatPayloadTimestamps(t *testing.T) {
	t.Parallel()

	wantTime := time.Date(2026, time.August, 31, 9, 16, 5, 123_000_000, time.FixedZone("IST", 5*60*60+30*60))
	tests := []struct {
		name      string
		timestamp string
	}{
		{name: "RFC3339", timestamp: fmt.Sprintf("%q", wantTime.Format(time.RFC3339Nano))},
		{name: "epoch milliseconds", timestamp: fmt.Sprintf("%d", wantTime.UnixMilli())},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			payload := []byte(fmt.Sprintf(`{
				"instrument_key":"NSE_EQ|INE090A01021",
				"price":1444.75,
				"volume":100,
				"timestamp":%s
			}`, test.timestamp))

			ticks, err := DecodeTicks(payload)
			if err != nil {
				t.Fatalf("DecodeTicks returned error: %v", err)
			}
			if len(ticks) != 1 {
				t.Fatalf("len(ticks) = %d, want 1", len(ticks))
			}
			tick := ticks[0]
			if tick.InstrumentKey != "NSE_EQ|INE090A01021" || tick.Price != 1444.75 {
				t.Fatalf("tick identity/price = (%q, %v), want expected flat values", tick.InstrumentKey, tick.Price)
			}
			if tick.PreviousClose != 0 {
				t.Fatalf("PreviousClose = %v, want zero for flat payload", tick.PreviousClose)
			}
			if !tick.Timestamp.Equal(wantTime) || !tick.ObservedAt.Equal(wantTime) {
				t.Fatalf("flat times = (%s, %s), want %s", tick.Timestamp, tick.ObservedAt, wantTime)
			}
		})
	}
}

func TestDecodeTicksKeepsPreviousCloseMetadataWithoutPositiveLTP(t *testing.T) {
	t.Parallel()

	observedAt := time.Date(2026, time.September, 1, 9, 0, 0, 0, time.UTC)
	payload := []byte(fmt.Sprintf(`{
		"feeds":{"NSE_EQ|PREOPEN":{"fullFeed":{"marketFF":{"ltpc":{"ltp":0,"ltt":0,"cp":123.45}}}}},
		"currentTs":"%d"
	}`, observedAt.UnixMilli()))

	ticks, err := DecodeTicks(payload)
	if err != nil {
		t.Fatalf("DecodeTicks returned error: %v", err)
	}
	if len(ticks) != 1 {
		t.Fatalf("len(ticks) = %d, want metadata tick", len(ticks))
	}
	if ticks[0].Price != 0 || ticks[0].PreviousClose != 123.45 {
		t.Fatalf("metadata tick price/CP = %v/%v, want 0/123.45", ticks[0].Price, ticks[0].PreviousClose)
	}
	if !ticks[0].Timestamp.Equal(observedAt) || !ticks[0].ObservedAt.Equal(observedAt) {
		t.Fatalf("metadata tick times = %s/%s, want %s", ticks[0].Timestamp, ticks[0].ObservedAt, observedAt)
	}
}
