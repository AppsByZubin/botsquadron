package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/AppsByZubin/botsquadron/services/sidecar/internal/model"
	"github.com/AppsByZubin/botsquadron/services/sidecar/internal/store"
)

func TestHealthEndpointAndSecurityHeaders(t *testing.T) {
	t.Parallel()

	_, routes, _ := newHTTPTestHandler(t, model.Readiness{})
	response := serveHTTPTestRequest(routes, http.MethodGet, "/healthz")
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", response.Code, response.Body.String())
	}
	if got := response.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("Cache-Control = %q, want no-store", got)
	}
	if got := response.Header().Get("X-Content-Type-Options"); got != "nosniff" {
		t.Fatalf("X-Content-Type-Options = %q, want nosniff", got)
	}
	if got := response.Header().Get("Content-Type"); !strings.HasPrefix(got, "application/json") {
		t.Fatalf("Content-Type = %q, want application/json", got)
	}
	var body map[string]string
	decodeHTTPTestBody(t, response, &body)
	if body["status"] != "ok" {
		t.Fatalf("body = %#v, want status ok", body)
	}
}

func TestReadyEndpointReflectsStateAndLatestDatetime(t *testing.T) {
	t.Parallel()

	readiness := model.Readiness{
		Ready:                 false,
		NATSConnected:         true,
		CalculationReady:      false,
		ExpectedConstituents:  50,
		PreviousClosesLoaded:  49,
		MissingPreviousCloses: []string{"NSE_EQ|MISSING"},
	}
	snapshotStore, routes, readinessPointer := newHTTPTestHandler(t, readiness)

	unreadyResponse := serveHTTPTestRequest(routes, http.MethodGet, "/readyz")
	if unreadyCode := unreadyResponse.Code; unreadyCode != http.StatusServiceUnavailable {
		t.Fatalf("unready status = %d, want 503; body=%s", unreadyCode, unreadyResponse.Body.String())
	}
	var unready model.Readiness
	decodeHTTPTestBody(t, unreadyResponse, &unready)
	if unready.Ready || unready.PreviousClosesLoaded != 49 || len(unready.MissingPreviousCloses) != 1 {
		t.Fatalf("unready body = %#v, want supplied readiness metadata", unready)
	}

	snapshot := httpTestSnapshot("2026-08-31T09:16:00+05:30", 10)
	if err := snapshotStore.Append(snapshot); err != nil {
		t.Fatalf("Append returned error: %v", err)
	}
	*readinessPointer = model.Readiness{
		Ready:                true,
		NATSConnected:        true,
		CalculationReady:     true,
		ExpectedConstituents: 50,
		PreviousClosesLoaded: 50,
		IndexPreviousClose:   true,
	}
	readyResponse := serveHTTPTestRequest(routes, http.MethodGet, "/readyz")
	if readyResponse.Code != http.StatusOK {
		t.Fatalf("ready status = %d, want 200; body=%s", readyResponse.Code, readyResponse.Body.String())
	}
	var ready model.Readiness
	decodeHTTPTestBody(t, readyResponse, &ready)
	if !ready.Ready || ready.LatestDatetime != snapshot.Datetime {
		t.Fatalf("ready body = %#v, want ready with latest datetime %q", ready, snapshot.Datetime)
	}
}

func TestLatestEndpointBeforeAndAfterSnapshot(t *testing.T) {
	t.Parallel()

	snapshotStore, routes, _ := newHTTPTestHandler(t, model.Readiness{})
	missingResponse := serveHTTPTestRequest(routes, http.MethodGet, "/v1/dragger-puller")
	if missingResponse.Code != http.StatusServiceUnavailable {
		t.Fatalf("missing status = %d, want 503; body=%s", missingResponse.Code, missingResponse.Body.String())
	}
	if !strings.Contains(missingResponse.Body.String(), "no completed dragger-puller snapshot") {
		t.Fatalf("missing body = %s, want availability error", missingResponse.Body.String())
	}

	snapshot := httpTestSnapshot("2026-08-31T09:16:00+05:30", 42.5)
	if err := snapshotStore.Append(snapshot); err != nil {
		t.Fatalf("Append returned error: %v", err)
	}
	latestResponse := serveHTTPTestRequest(routes, http.MethodGet, "/v1/dragger-puller")
	if latestResponse.Code != http.StatusOK {
		t.Fatalf("latest status = %d, want 200; body=%s", latestResponse.Code, latestResponse.Body.String())
	}
	var latest model.Snapshot
	decodeHTTPTestBody(t, latestResponse, &latest)
	if latest.Timestamp != snapshot.Timestamp || latest.PullerValue != 42.5 {
		t.Fatalf("latest = %#v, want appended snapshot", latest)
	}

	notFound := serveHTTPTestRequest(routes, http.MethodGet, "/v1/dragger-puller/not-a-route")
	if notFound.Code != http.StatusNotFound {
		t.Fatalf("subpath status = %d, want 404", notFound.Code)
	}
}

func TestHistoryEndpointReturnsAllAndLimitedTail(t *testing.T) {
	t.Parallel()

	snapshotStore, routes, _ := newHTTPTestHandler(t, model.Readiness{})
	timestamps := []string{
		"2026-08-31T09:16:00+05:30",
		"2026-08-31T09:17:00+05:30",
		"2026-08-31T09:18:00+05:30",
	}
	for index, timestamp := range timestamps {
		if err := snapshotStore.Append(httpTestSnapshot(timestamp, float64(index+1))); err != nil {
			t.Fatalf("Append(%d) returned error: %v", index, err)
		}
	}

	allResponse := serveHTTPTestRequest(routes, http.MethodGet, "/v1/dragger-puller/history")
	if allResponse.Code != http.StatusOK {
		t.Fatalf("all history status = %d, want 200; body=%s", allResponse.Code, allResponse.Body.String())
	}
	var all model.SnapshotFile
	decodeHTTPTestBody(t, allResponse, &all)
	if len(all.Snapshots) != 3 {
		t.Fatalf("all history length = %d, want 3", len(all.Snapshots))
	}

	limitedResponse := serveHTTPTestRequest(routes, http.MethodGet, "/v1/dragger-puller/history?limit=2")
	if limitedResponse.Code != http.StatusOK {
		t.Fatalf("limited history status = %d, want 200; body=%s", limitedResponse.Code, limitedResponse.Body.String())
	}
	var limited model.SnapshotFile
	decodeHTTPTestBody(t, limitedResponse, &limited)
	if len(limited.Snapshots) != 2 {
		t.Fatalf("limited history length = %d, want 2", len(limited.Snapshots))
	}
	if limited.Snapshots[0].Timestamp != timestamps[1] || limited.Snapshots[1].Timestamp != timestamps[2] {
		t.Fatalf("limited history timestamps = %#v, want newest tail", limited.Snapshots)
	}
	if limited.Latest == nil || limited.Latest.Timestamp != timestamps[2] {
		t.Fatalf("limited latest = %#v, want newest snapshot", limited.Latest)
	}
}

func TestHistoryEndpointRejectsInvalidLimits(t *testing.T) {
	t.Parallel()

	_, routes, _ := newHTTPTestHandler(t, model.Readiness{})
	for _, limit := range []string{"0", "-1", "601", "not-a-number"} {
		limit := limit
		t.Run(limit, func(t *testing.T) {
			t.Parallel()
			response := serveHTTPTestRequest(routes, http.MethodGet, "/v1/dragger-puller/history?limit="+limit)
			if response.Code != http.StatusBadRequest {
				t.Fatalf("status for limit %q = %d, want 400; body=%s", limit, response.Code, response.Body.String())
			}
			if !strings.Contains(response.Body.String(), "between 1 and 600") {
				t.Fatalf("body for limit %q = %s, want limit guidance", limit, response.Body.String())
			}
		})
	}
}

func TestEndpointsRejectUnsupportedMethods(t *testing.T) {
	t.Parallel()

	_, routes, _ := newHTTPTestHandler(t, model.Readiness{})
	for _, path := range []string{
		"/healthz",
		"/readyz",
		"/v1/dragger-puller",
		"/v1/dragger-puller/history",
	} {
		path := path
		t.Run(path, func(t *testing.T) {
			t.Parallel()
			response := serveHTTPTestRequest(routes, http.MethodPost, path)
			if response.Code != http.StatusMethodNotAllowed {
				t.Fatalf("status = %d, want 405; body=%s", response.Code, response.Body.String())
			}
			if got := response.Header().Get("Allow"); got != http.MethodGet {
				t.Fatalf("Allow = %q, want GET", got)
			}
		})
	}
}

func newHTTPTestHandler(t *testing.T, readiness model.Readiness) (*store.JSONStore, http.Handler, *model.Readiness) {
	t.Helper()
	snapshotStore, err := store.NewJSONStore(filepath.Join(t.TempDir(), "dragger-puller.json"), 600)
	if err != nil {
		t.Fatalf("NewJSONStore returned error: %v", err)
	}
	readinessState := readiness
	handler := NewHandler(snapshotStore, func() model.Readiness { return readinessState })
	return snapshotStore, handler.Routes(), &readinessState
}

func serveHTTPTestRequest(handler http.Handler, method, target string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, target, nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func decodeHTTPTestBody(t *testing.T, response *httptest.ResponseRecorder, target any) {
	t.Helper()
	if err := json.NewDecoder(response.Body).Decode(target); err != nil {
		t.Fatalf("decode response body: %v; body=%s", err, response.Body.String())
	}
}

func httpTestSnapshot(timestamp string, puller float64) model.Snapshot {
	return model.Snapshot{
		ExecutionDate:        "2026-08-31",
		Datetime:             timestamp,
		Timestamp:            timestamp,
		CandleStart:          timestamp,
		PullerValue:          puller,
		DraggerValue:         -1,
		NetValue:             puller - 1,
		MarketClassification: model.Bullish,
		CoverageCount:        50,
		ExpectedCount:        50,
		Contributions:        []model.Contribution{},
	}
}
