package calculator

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestOfficialWeightsFile(t *testing.T) {
	t.Parallel()
	constituents, err := LoadConstituents(filepath.Join("..", "..", "files", "official_nifty50_weights.csv"))
	if err != nil {
		t.Fatalf("LoadConstituents(official file) error = %v", err)
	}
	if len(constituents) != 50 {
		t.Fatalf("official constituent count = %d, want 50", len(constituents))
	}
	total := 0.0
	seen := make(map[string]bool, len(constituents))
	for _, constituent := range constituents {
		total += constituent.WeightPercent
		if seen[constituent.InstrumentKey] {
			t.Fatalf("duplicate official instrument key %q", constituent.InstrumentKey)
		}
		seen[constituent.InstrumentKey] = true
	}
	if math.Abs(total-100) > 1e-9 {
		t.Fatalf("normalized official weight total = %.12f, want 100", total)
	}
}

func TestLoadConstituentsRejectsInvalidBasket(t *testing.T) {
	tests := []struct {
		name   string
		mutate func([]string)
	}{
		{
			name:   "wrong count",
			mutate: func(rows []string) { rows[50] = "" },
		},
		{
			name:   "duplicate key",
			mutate: func(rows []string) { rows[50] = "STOCK49,2,Stock 49,NSE_EQ|TEST00" },
		},
		{
			name:   "non equity key",
			mutate: func(rows []string) { rows[50] = "STOCK49,2,Stock 49,NSE_INDEX|TEST49" },
		},
		{
			name:   "weight total outside tolerance",
			mutate: func(rows []string) { rows[50] = "STOCK49,10,Stock 49,NSE_EQ|TEST49" },
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rows := []string{"symbol,weight_percent,name,instrument_key"}
			for position := 0; position < 50; position++ {
				rows = append(rows, fmt.Sprintf("STOCK%02d,2,Stock %02d,NSE_EQ|TEST%02d", position, position, position))
			}
			test.mutate(rows)
			path := filepath.Join(t.TempDir(), "weights.csv")
			if err := os.WriteFile(path, []byte(strings.Join(rows, "\n")+"\n"), 0o600); err != nil {
				t.Fatalf("write test weights: %v", err)
			}
			if _, err := LoadConstituents(path); err == nil {
				t.Fatal("LoadConstituents() accepted an invalid basket")
			}
		})
	}
}
