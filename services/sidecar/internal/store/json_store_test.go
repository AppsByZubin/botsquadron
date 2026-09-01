package store

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/AppsByZubin/botsquadron/services/sidecar/internal/model"
)

func TestJSONStoreAppendWritesAtomicDocumentAndReloads(t *testing.T) {
	t.Parallel()

	directory := t.TempDir()
	path := filepath.Join(directory, "nested", "dragger-puller.json")
	snapshotStore, err := NewJSONStore(path, 10)
	if err != nil {
		t.Fatalf("NewJSONStore returned error: %v", err)
	}
	first := storeTestSnapshot("2026-08-31", "2026-08-31T09:16:00+05:30", 12.5)
	second := storeTestSnapshot("2026-08-31", "2026-08-31T09:17:00+05:30", 13.5)
	if err := snapshotStore.Append(first); err != nil {
		t.Fatalf("Append(first) returned error: %v", err)
	}
	if err := snapshotStore.Append(second); err != nil {
		t.Fatalf("Append(second) returned error: %v", err)
	}

	diskBytes, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile returned error: %v", err)
	}
	var diskDocument model.SnapshotFile
	if err := json.Unmarshal(diskBytes, &diskDocument); err != nil {
		t.Fatalf("persisted document is not valid JSON: %v", err)
	}
	if diskDocument.SchemaVersion != model.SchemaVersion || diskDocument.ExecutionDate != "2026-08-31" {
		t.Fatalf("persisted metadata = %#v, want schema/date", diskDocument)
	}
	if len(diskDocument.Snapshots) != 2 || diskDocument.Latest == nil || diskDocument.Latest.Timestamp != second.Timestamp {
		t.Fatalf("persisted snapshots/latest = %#v, want both snapshots and second latest", diskDocument)
	}
	if _, err := time.Parse(time.RFC3339Nano, diskDocument.UpdatedAt); err != nil {
		t.Fatalf("UpdatedAt = %q, want RFC3339 timestamp: %v", diskDocument.UpdatedAt, err)
	}
	temporaryFiles, err := filepath.Glob(filepath.Join(filepath.Dir(path), ".dragger-puller-*.tmp"))
	if err != nil {
		t.Fatalf("Glob returned error: %v", err)
	}
	if len(temporaryFiles) != 0 {
		t.Fatalf("temporary files remain after atomic replace: %v", temporaryFiles)
	}

	reloaded, err := NewJSONStore(path, 10)
	if err != nil {
		t.Fatalf("reload NewJSONStore returned error: %v", err)
	}
	latest, exists := reloaded.Latest()
	if !exists {
		t.Fatal("reloaded store has no latest snapshot")
	}
	if latest.Timestamp != second.Timestamp || latest.PullerValue != second.PullerValue {
		t.Fatalf("reloaded latest = %#v, want %#v", latest, second)
	}
	history := reloaded.History(0)
	if len(history.Snapshots) != 2 || history.Snapshots[0].Timestamp != first.Timestamp {
		t.Fatalf("reloaded history = %#v, want append order", history.Snapshots)
	}
}

func TestJSONStoreFailedAtomicAppendDoesNotCommitMemoryOrDisk(t *testing.T) {
	t.Parallel()

	directory := t.TempDir()
	path := filepath.Join(directory, "dragger-puller.json")
	snapshotStore, err := NewJSONStore(path, 10)
	if err != nil {
		t.Fatalf("NewJSONStore returned error: %v", err)
	}
	first := storeTestSnapshot("2026-08-31", "2026-08-31T09:16:00+05:30", 12.5)
	if err := snapshotStore.Append(first); err != nil {
		t.Fatalf("Append(first) returned error: %v", err)
	}

	originalPath := snapshotStore.path
	snapshotStore.path = filepath.Join(directory, "missing", "dragger-puller.json")
	second := storeTestSnapshot("2026-08-31", "2026-08-31T09:17:00+05:30", 99)
	if err := snapshotStore.Append(second); err == nil {
		t.Fatal("Append with missing destination directory returned nil error")
	}
	snapshotStore.path = originalPath

	latest, exists := snapshotStore.Latest()
	if !exists || latest.Timestamp != first.Timestamp {
		t.Fatalf("in-memory latest after failed append = %#v, want first snapshot", latest)
	}
	reloaded, err := NewJSONStore(path, 10)
	if err != nil {
		t.Fatalf("reload NewJSONStore returned error: %v", err)
	}
	diskLatest, exists := reloaded.Latest()
	if !exists || diskLatest.Timestamp != first.Timestamp {
		t.Fatalf("on-disk latest after failed append = %#v, want first snapshot", diskLatest)
	}
}

func TestJSONStoreResetsSnapshotsOnTradingDayChange(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "dragger-puller.json")
	snapshotStore, err := NewJSONStore(path, 10)
	if err != nil {
		t.Fatalf("NewJSONStore returned error: %v", err)
	}
	oldSnapshot := storeTestSnapshot("2026-08-31", "2026-08-31T15:30:00+05:30", 10)
	newSnapshot := storeTestSnapshot("2026-09-01", "2026-09-01T09:16:00+05:30", 20)
	if err := snapshotStore.Append(oldSnapshot); err != nil {
		t.Fatalf("Append(oldSnapshot) returned error: %v", err)
	}
	if err := snapshotStore.Append(newSnapshot); err != nil {
		t.Fatalf("Append(newSnapshot) returned error: %v", err)
	}

	history := snapshotStore.History(0)
	if history.ExecutionDate != "2026-09-01" {
		t.Fatalf("ExecutionDate = %q, want new trading day", history.ExecutionDate)
	}
	if len(history.Snapshots) != 1 || history.Snapshots[0].Timestamp != newSnapshot.Timestamp {
		t.Fatalf("snapshots after day reset = %#v, want only new snapshot", history.Snapshots)
	}
	if history.Latest == nil || history.Latest.Timestamp != newSnapshot.Timestamp {
		t.Fatalf("Latest after day reset = %#v, want new snapshot", history.Latest)
	}
}

func TestNewJSONStoreRejectsCorruptFile(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		content string
		wantErr string
	}{
		{name: "invalid JSON", content: `{"schema_version":1,`, wantErr: "decode sidecar output"},
		{name: "multiple documents", content: `{"schema_version":1,"snapshots":[]} {}`, wantErr: "multiple JSON values"},
		{name: "unknown field", content: `{"schema_version":1,"snapshots":[],"surprise":true}`, wantErr: "unknown field"},
		{name: "unsupported schema", content: `{"schema_version":99,"snapshots":[]}`, wantErr: "unsupported sidecar output schema 99"},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(t.TempDir(), "dragger-puller.json")
			if err := os.WriteFile(path, []byte(test.content), 0o600); err != nil {
				t.Fatalf("WriteFile returned error: %v", err)
			}
			_, err := NewJSONStore(path, 10)
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("NewJSONStore error = %v, want substring %q", err, test.wantErr)
			}
		})
	}
}

func storeTestSnapshot(executionDate, timestamp string, puller float64) model.Snapshot {
	return model.Snapshot{
		ExecutionDate:        executionDate,
		Datetime:             timestamp,
		Timestamp:            timestamp,
		CandleStart:          timestamp,
		PullerValue:          puller,
		DraggerValue:         -5,
		NetValue:             puller - 5,
		MarketClassification: model.Bullish,
		CoverageCount:        50,
		ExpectedCount:        50,
		Contributions: []model.Contribution{
			{Symbol: "TEST", InstrumentKey: "NSE_EQ|TEST", Points: puller},
		},
	}
}
