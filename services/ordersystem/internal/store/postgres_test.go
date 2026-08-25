package store

import (
	"testing"
	"time"
)

func TestLossPeriodBoundsUseConfiguredTimezone(t *testing.T) {
	t.Parallel()

	loc, err := time.LoadLocation("Asia/Kolkata")
	if err != nil {
		t.Fatalf("load location: %v", err)
	}
	at := time.Date(2026, time.August, 31, 18, 45, 0, 0, time.UTC)
	dayStart, dayEnd, monthStart, monthEnd, dayKey, monthKey := lossPeriodBounds(at, loc)

	if dayKey != "01-09-2026" || monthKey != "09-2026" {
		t.Fatalf("period keys = %s/%s, want 01-09-2026/09-2026", dayKey, monthKey)
	}
	if got := dayEnd.Sub(dayStart); got != 24*time.Hour {
		t.Fatalf("day window = %v, want 24h", got)
	}
	if monthStart.Day() != 1 || monthStart.Month() != time.September {
		t.Fatalf("monthStart = %v, want September 1", monthStart)
	}
	if monthEnd.Month() != time.October || monthEnd.Day() != 1 {
		t.Fatalf("monthEnd = %v, want October 1", monthEnd)
	}
}
