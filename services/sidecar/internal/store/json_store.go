package store

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/AppsByZubin/botsquadron/services/sidecar/internal/model"
)

// JSONStore keeps the current trading day's snapshots in one atomically
// replaced local JSON file.
type JSONStore struct {
	mu      sync.RWMutex
	path    string
	maximum int
	data    model.SnapshotFile
}

func NewJSONStore(path string, maximum int) (*JSONStore, error) {
	if path == "" {
		return nil, fmt.Errorf("output path is required")
	}
	if maximum <= 0 {
		return nil, fmt.Errorf("maximum snapshot count must be positive")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return nil, fmt.Errorf("create output directory: %w", err)
	}
	store := &JSONStore{
		path: path, maximum: maximum,
		data: model.SnapshotFile{SchemaVersion: model.SchemaVersion, Snapshots: []model.Snapshot{}},
	}
	if err := store.load(); err != nil {
		return nil, err
	}
	return store, nil
}

func (store *JSONStore) load() error {
	file, err := os.Open(store.path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("open sidecar output %q: %w", store.path, err)
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	var data model.SnapshotFile
	if err := decoder.Decode(&data); err != nil {
		return fmt.Errorf("decode sidecar output %q: %w", store.path, err)
	}
	if err := ensureEOF(decoder); err != nil {
		return fmt.Errorf("decode sidecar output %q: %w", store.path, err)
	}
	if data.SchemaVersion != model.SchemaVersion {
		return fmt.Errorf("unsupported sidecar output schema %d", data.SchemaVersion)
	}
	if data.Snapshots == nil {
		data.Snapshots = []model.Snapshot{}
	}
	if len(data.Snapshots) > store.maximum {
		data.Snapshots = append([]model.Snapshot(nil), data.Snapshots[len(data.Snapshots)-store.maximum:]...)
	}
	store.data = data
	return nil
}

func ensureEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); err == io.EOF {
		return nil
	} else if err != nil {
		return err
	}
	return fmt.Errorf("multiple JSON values are not allowed")
}

func (store *JSONStore) Append(snapshot model.Snapshot) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	data := cloneDocument(store.data)
	if data.ExecutionDate != snapshot.ExecutionDate {
		data = model.SnapshotFile{
			SchemaVersion: model.SchemaVersion,
			ExecutionDate: snapshot.ExecutionDate,
			Snapshots:     []model.Snapshot{},
		}
	}
	replaced := false
	for position := range data.Snapshots {
		if data.Snapshots[position].Timestamp == snapshot.Timestamp {
			data.Snapshots[position] = snapshot
			replaced = true
			break
		}
	}
	if !replaced {
		data.Snapshots = append(data.Snapshots, snapshot)
	}
	if len(data.Snapshots) > store.maximum {
		data.Snapshots = append([]model.Snapshot(nil), data.Snapshots[len(data.Snapshots)-store.maximum:]...)
	}
	data.Latest = cloneSnapshot(&snapshot)
	data.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	if err := store.writeAtomic(data); err != nil {
		return err
	}
	store.data = data
	return nil
}

func (store *JSONStore) writeAtomic(data model.SnapshotFile) error {
	directory := filepath.Dir(store.path)
	temporary, err := os.CreateTemp(directory, ".dragger-puller-*.tmp")
	if err != nil {
		return fmt.Errorf("create temporary output: %w", err)
	}
	temporaryPath := temporary.Name()
	cleanup := func() {
		_ = temporary.Close()
		_ = os.Remove(temporaryPath)
	}
	if err := temporary.Chmod(0o640); err != nil {
		cleanup()
		return fmt.Errorf("set temporary output permissions: %w", err)
	}
	encoder := json.NewEncoder(temporary)
	encoder.SetIndent("", "  ")
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(data); err != nil {
		cleanup()
		return fmt.Errorf("encode temporary output: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		cleanup()
		return fmt.Errorf("sync temporary output: %w", err)
	}
	if err := temporary.Close(); err != nil {
		cleanup()
		return fmt.Errorf("close temporary output: %w", err)
	}
	if err := os.Rename(temporaryPath, store.path); err != nil {
		cleanup()
		return fmt.Errorf("replace output %q: %w", store.path, err)
	}
	return nil
}

func (store *JSONStore) Latest() (*model.Snapshot, bool) {
	store.mu.RLock()
	defer store.mu.RUnlock()
	if store.data.Latest == nil {
		return nil, false
	}
	return cloneSnapshot(store.data.Latest), true
}

func (store *JSONStore) History(limit int) model.SnapshotFile {
	store.mu.RLock()
	defer store.mu.RUnlock()
	data := cloneDocument(store.data)
	if limit > 0 && len(data.Snapshots) > limit {
		data.Snapshots = append([]model.Snapshot(nil), data.Snapshots[len(data.Snapshots)-limit:]...)
	}
	return data
}

func cloneDocument(data model.SnapshotFile) model.SnapshotFile {
	result := data
	result.Latest = cloneSnapshot(data.Latest)
	result.Snapshots = append([]model.Snapshot(nil), data.Snapshots...)
	return result
}

func cloneSnapshot(snapshot *model.Snapshot) *model.Snapshot {
	if snapshot == nil {
		return nil
	}
	copyValue := *snapshot
	copyValue.Contributions = append([]model.Contribution(nil), snapshot.Contributions...)
	return &copyValue
}
