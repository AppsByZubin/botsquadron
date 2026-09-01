package httpapi

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/AppsByZubin/botsquadron/services/sidecar/internal/model"
	"github.com/AppsByZubin/botsquadron/services/sidecar/internal/store"
)

type Handler struct {
	store     *store.JSONStore
	readiness func() model.Readiness
}

func NewHandler(snapshotStore *store.JSONStore, readiness func() model.Readiness) *Handler {
	return &Handler{store: snapshotStore, readiness: readiness}
}

func (handler *Handler) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", handler.health)
	mux.HandleFunc("/readyz", handler.ready)
	mux.HandleFunc("/v1/dragger-puller", handler.latest)
	mux.HandleFunc("/v1/dragger-puller/history", handler.history)
	return securityHeaders(mux)
}

func (handler *Handler) health(response http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		methodNotAllowed(response)
		return
	}
	writeJSON(response, http.StatusOK, map[string]string{"status": "ok"})
}

func (handler *Handler) ready(response http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		methodNotAllowed(response)
		return
	}
	status := handler.readiness()
	if latest, exists := handler.store.Latest(); exists {
		status.LatestDatetime = latest.Datetime
	}
	httpStatus := http.StatusOK
	if !status.Ready {
		httpStatus = http.StatusServiceUnavailable
	}
	writeJSON(response, httpStatus, status)
}

func (handler *Handler) latest(response http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		methodNotAllowed(response)
		return
	}
	if strings.TrimSuffix(request.URL.Path, "/") != "/v1/dragger-puller" {
		http.NotFound(response, request)
		return
	}
	latest, exists := handler.store.Latest()
	if !exists {
		writeJSON(response, http.StatusServiceUnavailable, map[string]string{
			"error": "no completed dragger-puller snapshot is available yet",
		})
		return
	}
	writeJSON(response, http.StatusOK, latest)
}

func (handler *Handler) history(response http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		methodNotAllowed(response)
		return
	}
	limit := 0
	if raw := strings.TrimSpace(request.URL.Query().Get("limit")); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 || parsed > 600 {
			writeJSON(response, http.StatusBadRequest, map[string]string{
				"error": "limit must be an integer between 1 and 600",
			})
			return
		}
		limit = parsed
	}
	writeJSON(response, http.StatusOK, handler.store.History(limit))
}

func securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set("Cache-Control", "no-store")
		response.Header().Set("X-Content-Type-Options", "nosniff")
		next.ServeHTTP(response, request)
	})
}

func methodNotAllowed(response http.ResponseWriter) {
	response.Header().Set("Allow", http.MethodGet)
	writeJSON(response, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
}

func writeJSON(response http.ResponseWriter, status int, payload any) {
	response.Header().Set("Content-Type", "application/json")
	response.WriteHeader(status)
	_ = json.NewEncoder(response).Encode(payload)
}
