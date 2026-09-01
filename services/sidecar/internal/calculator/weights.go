package calculator

import (
	"encoding/csv"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"strings"

	"github.com/AppsByZubin/botsquadron/services/sidecar/internal/model"
)

const expectedConstituentCount = 50

// LoadConstituents validates and normalizes an official NIFTY 50 weights CSV.
func LoadConstituents(path string) ([]model.Constituent, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open weights file %q: %w", path, err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	header, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("read weights header: %w", err)
	}
	wanted := map[string]int{}
	for position, value := range header {
		wanted[strings.TrimSpace(value)] = position
	}
	for _, column := range []string{"symbol", "weight_percent", "name", "instrument_key"} {
		if _, ok := wanted[column]; !ok {
			return nil, fmt.Errorf("weights file is missing %q column", column)
		}
	}

	var constituents []model.Constituent
	seenSymbols := make(map[string]struct{})
	seenKeys := make(map[string]struct{})
	totalWeight := 0.0
	for rowNumber := 2; ; rowNumber++ {
		row, readErr := reader.Read()
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return nil, fmt.Errorf("read weights row %d: %w", rowNumber, readErr)
		}
		value := func(column string) string {
			position := wanted[column]
			if position >= len(row) {
				return ""
			}
			return strings.TrimSpace(row[position])
		}

		symbol := value("symbol")
		name := value("name")
		instrumentKey := value("instrument_key")
		weight, parseErr := strconv.ParseFloat(value("weight_percent"), 64)
		if symbol == "" || name == "" || instrumentKey == "" {
			return nil, fmt.Errorf("weights row %d contains an empty required value", rowNumber)
		}
		if parseErr != nil || !isPositiveFinite(weight) {
			return nil, fmt.Errorf("weights row %d has invalid weight %q", rowNumber, value("weight_percent"))
		}
		if !strings.HasPrefix(instrumentKey, "NSE_EQ|") {
			return nil, fmt.Errorf("weights row %d instrument key %q is not NSE_EQ", rowNumber, instrumentKey)
		}
		if _, exists := seenSymbols[symbol]; exists {
			return nil, fmt.Errorf("duplicate constituent symbol %q", symbol)
		}
		if _, exists := seenKeys[instrumentKey]; exists {
			return nil, fmt.Errorf("duplicate constituent instrument key %q", instrumentKey)
		}
		seenSymbols[symbol] = struct{}{}
		seenKeys[instrumentKey] = struct{}{}
		totalWeight += weight
		constituents = append(constituents, model.Constituent{
			Symbol: symbol, Name: name, InstrumentKey: instrumentKey, WeightPercent: weight,
		})
	}

	if len(constituents) != expectedConstituentCount {
		return nil, fmt.Errorf("weights file must contain exactly %d constituents, got %d", expectedConstituentCount, len(constituents))
	}
	if !isPositiveFinite(totalWeight) || totalWeight < 99 || totalWeight > 101 {
		return nil, fmt.Errorf("constituent weights must total between 99 and 101, got %.8f", totalWeight)
	}
	for position := range constituents {
		constituents[position].WeightPercent = constituents[position].WeightPercent / totalWeight * 100
	}
	return constituents, nil
}

func isPositiveFinite(value float64) bool {
	return value > 0 && !math.IsNaN(value) && !math.IsInf(value, 0)
}
