package httpapi

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/AppsByZubin/botsquadron/services/ordersystem/internal/model"
	"github.com/AppsByZubin/botsquadron/services/ordersystem/internal/service"
)

type Handler struct {
	svc            *service.Service
	requestTimeout time.Duration
}

type errorResponse struct {
	Error string `json:"error"`
}

func New(svc *service.Service, requestTimeout time.Duration) *Handler {
	return &Handler{svc: svc, requestTimeout: requestTimeout}
}

func (h *Handler) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", h.handleHealth)
	mux.HandleFunc("POST /v1/accounts", h.handleCreateAccount)
	mux.HandleFunc("POST /v1/trades", h.handleCreateTrade)
	mux.HandleFunc("POST /v1/trades/{id}/modify", h.handleModifyTrade)
	mux.HandleFunc("GET /v1/trades/{id}", h.handleGetTradeByID)
	return mux
}

func (h *Handler) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) handleCreateAccount(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), h.requestTimeout)
	defer cancel()

	defer r.Body.Close()
	var req model.CreateAccountRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "invalid request body: " + err.Error()})
		return
	}

	resp, err := h.svc.CreateAccount(ctx, req)
	if err != nil {
		statusCode := http.StatusInternalServerError
		if strings.Contains(strings.ToLower(err.Error()), "required") || strings.Contains(strings.ToLower(err.Error()), "must be") {
			statusCode = http.StatusBadRequest
		}
		writeJSON(w, statusCode, errorResponse{Error: err.Error()})
		return
	}

	writeJSON(w, http.StatusCreated, resp)
}

func (h *Handler) handleCreateTrade(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), h.requestTimeout)
	defer cancel()

	defer r.Body.Close()
	var req model.CreateTradeRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "invalid request body: " + err.Error()})
		return
	}

	resp, err := h.svc.CreateTrade(ctx, req)
	if err != nil {
		statusCode := http.StatusInternalServerError
		if strings.Contains(strings.ToLower(err.Error()), "required") || strings.Contains(strings.ToLower(err.Error()), "must be") {
			statusCode = http.StatusBadRequest
		}
		writeJSON(w, statusCode, errorResponse{Error: err.Error()})
		return
	}

	writeJSON(w, http.StatusCreated, resp)
}

func (h *Handler) handleModifyTrade(w http.ResponseWriter, r *http.Request) {
	tradeID := strings.TrimSpace(r.PathValue("id"))
	if tradeID == "" {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "trade id is required"})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), h.requestTimeout)
	defer cancel()

	defer r.Body.Close()
	var req model.ModifyTradeRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "invalid request body: " + err.Error()})
		return
	}

	resp, err := h.svc.ModifyTrade(ctx, tradeID, req)
	if err != nil {
		statusCode := http.StatusInternalServerError
		if strings.Contains(strings.ToLower(err.Error()), "required") || strings.Contains(strings.ToLower(err.Error()), "must be") {
			statusCode = http.StatusBadRequest
		}
		if strings.Contains(strings.ToLower(err.Error()), "no rows") {
			statusCode = http.StatusNotFound
		}
		writeJSON(w, statusCode, errorResponse{Error: err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, resp)
}

func (h *Handler) handleGetTradeByID(w http.ResponseWriter, r *http.Request) {
	tradeID := strings.TrimSpace(r.PathValue("id"))
	if tradeID == "" {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "trade id is required"})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), h.requestTimeout)
	defer cancel()

	trade, err := h.svc.GetTradeByID(ctx, tradeID)
	if err != nil {
		log.Printf("get trade failed for trade_id=%s: %v", tradeID, err)
		if strings.Contains(strings.ToLower(err.Error()), "no rows") {
			writeJSON(w, http.StatusNotFound, errorResponse{Error: "trade not found"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "failed to load trade"})
		return
	}

	writeJSON(w, http.StatusOK, trade)
}

func writeJSON(w http.ResponseWriter, statusCode int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		log.Printf("write json response failed: %v", err)
	}
}
