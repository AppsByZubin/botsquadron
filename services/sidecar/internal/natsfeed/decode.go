package natsfeed

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/AppsByZubin/botsquadron/services/sidecar/internal/model"
)

type ltpcPayload struct {
	LTP float64 `json:"ltp"`
	LTT int64   `json:"ltt"`
	CP  float64 `json:"cp"`
}

type feedEntry struct {
	LTPC     ltpcPayload `json:"ltpc"`
	FullFeed struct {
		MarketFF struct {
			LTPC ltpcPayload `json:"ltpc"`
		} `json:"marketFF"`
		IndexFF struct {
			LTPC ltpcPayload `json:"ltpc"`
		} `json:"indexFF"`
	} `json:"fullFeed"`
	FirstLevelWithGreeks struct {
		LTPC ltpcPayload `json:"ltpc"`
	} `json:"firstLevelWithGreeks"`
}

type feedEnvelope struct {
	Feeds     map[string]feedEntry `json:"feeds"`
	CurrentTS json.RawMessage      `json:"currentTs"`
}

type flatTick struct {
	InstrumentKey string          `json:"instrument_key"`
	Price         float64         `json:"price"`
	Timestamp     json.RawMessage `json:"timestamp"`
}

// DecodeTicks accepts both marketfeeder's full-feed envelope and its flat LTPC
// fallback. The full feed is required to obtain previous-session close values.
func DecodeTicks(payload []byte) ([]model.Tick, error) {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	var envelope feedEnvelope
	if err := decoder.Decode(&envelope); err != nil {
		return nil, fmt.Errorf("decode marketfeeder payload: %w", err)
	}
	if len(envelope.Feeds) > 0 {
		observedAt, err := parseEpochJSON(envelope.CurrentTS)
		if err != nil {
			return nil, fmt.Errorf("decode marketfeeder currentTs: %w", err)
		}
		ticks := make([]model.Tick, 0, len(envelope.Feeds))
		for instrumentKey, feed := range envelope.Feeds {
			ltpc := selectLTPC(feed)
			if strings.TrimSpace(instrumentKey) == "" || (!positiveFinite(ltpc.LTP) && !positiveFinite(ltpc.CP)) {
				continue
			}
			eventTime := epochTime(ltpc.LTT)
			if eventTime.IsZero() {
				eventTime = observedAt
			}
			if eventTime.IsZero() {
				continue
			}
			ticks = append(ticks, model.Tick{
				InstrumentKey: instrumentKey, Price: ltpc.LTP, PreviousClose: ltpc.CP,
				Timestamp: eventTime, ObservedAt: observedAt,
			})
		}
		return ticks, nil
	}

	var flat flatTick
	if err := json.Unmarshal(payload, &flat); err != nil {
		return nil, fmt.Errorf("decode flat marketfeeder tick: %w", err)
	}
	if strings.TrimSpace(flat.InstrumentKey) == "" || !positiveFinite(flat.Price) {
		return nil, fmt.Errorf("marketfeeder payload has no usable feed")
	}
	timestamp, err := parseTimestampJSON(flat.Timestamp)
	if err != nil {
		return nil, fmt.Errorf("decode flat tick timestamp: %w", err)
	}
	return []model.Tick{{
		InstrumentKey: flat.InstrumentKey, Price: flat.Price,
		Timestamp: timestamp, ObservedAt: timestamp,
	}}, nil
}

func selectLTPC(feed feedEntry) ltpcPayload {
	for _, candidate := range []ltpcPayload{
		feed.LTPC,
		feed.FullFeed.MarketFF.LTPC,
		feed.FullFeed.IndexFF.LTPC,
		feed.FirstLevelWithGreeks.LTPC,
	} {
		if positiveFinite(candidate.LTP) || positiveFinite(candidate.CP) {
			return candidate
		}
	}
	return ltpcPayload{}
}

func parseEpochJSON(raw json.RawMessage) (time.Time, error) {
	text := strings.Trim(strings.TrimSpace(string(raw)), "\"")
	if text == "" || text == "null" {
		return time.Time{}, nil
	}
	value, err := strconv.ParseInt(text, 10, 64)
	if err != nil {
		return time.Time{}, err
	}
	parsed := epochTime(value)
	if parsed.IsZero() {
		return time.Time{}, fmt.Errorf("invalid epoch %q", text)
	}
	return parsed, nil
}

func parseTimestampJSON(raw json.RawMessage) (time.Time, error) {
	text := strings.Trim(strings.TrimSpace(string(raw)), "\"")
	if text == "" || text == "null" {
		return time.Time{}, fmt.Errorf("timestamp is required")
	}
	if value, err := strconv.ParseInt(text, 10, 64); err == nil {
		if parsed := epochTime(value); !parsed.IsZero() {
			return parsed, nil
		}
	}
	parsed, err := time.Parse(time.RFC3339Nano, text)
	if err != nil {
		return time.Time{}, err
	}
	return parsed, nil
}

func epochTime(value int64) time.Time {
	if value <= 0 {
		return time.Time{}
	}
	// Upstox uses milliseconds. Accept seconds as a defensive compatibility aid.
	if value < 100_000_000_000 {
		return time.Unix(value, 0)
	}
	return time.UnixMilli(value)
}

func positiveFinite(value float64) bool {
	return value > 0 && !math.IsNaN(value) && !math.IsInf(value, 0)
}
