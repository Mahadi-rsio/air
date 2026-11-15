package control

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
)

type createDatabaseReq struct {
	TenantID string `json:"tenant_id"`
}

// HTTPHandler returns an http.Handler exposing the AIR v1 Control Plane REST API:
//   - GET  /healthz
//   - POST /v1/databases         (Header: X-Account-ID, Idempotency-Key; Body: {"tenant_id": "..."})
//   - GET  /v1/databases/{id}    (Header: X-Account-ID)
//   - DELETE /v1/databases/{id}  (Header: X-Account-ID)
func (cp *ControlPlane) HTTPHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		if err := cp.db.Ping(r.Context()); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "unhealthy", "error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})

	mux.HandleFunc("POST /v1/databases", func(w http.ResponseWriter, r *http.Request) {
		accountID := strings.TrimSpace(r.Header.Get("X-Account-ID"))
		idemKey := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
		if accountID == "" || idemKey == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "X-Account-ID and Idempotency-Key headers are required"})
			return
		}
		var req createDatabaseReq
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16*1024)).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON body"})
			return
		}
		resp, err := cp.ProvisionDatabase(r.Context(), accountID, idemKey, req.TenantID)
		if err != nil {
			switch {
			case errors.Is(err, ErrIdempotencyConflict):
				writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
			case errors.Is(err, ErrForbidden):
				writeJSON(w, http.StatusForbidden, map[string]string{"error": err.Error()})
			default:
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			}
			return
		}
		writeJSON(w, http.StatusCreated, resp)
	})

	mux.HandleFunc("GET /v1/databases/{id}", func(w http.ResponseWriter, r *http.Request) {
		accountID := strings.TrimSpace(r.Header.Get("X-Account-ID"))
		if accountID == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "X-Account-ID header is required"})
			return
		}
		info, err := cp.GetDatabase(r.Context(), accountID, r.PathValue("id"))
		if err != nil {
			switch {
			case errors.Is(err, ErrNotFound):
				writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
			case errors.Is(err, ErrForbidden):
				writeJSON(w, http.StatusForbidden, map[string]string{"error": err.Error()})
			default:
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			}
			return
		}
		writeJSON(w, http.StatusOK, info)
	})

	mux.HandleFunc("DELETE /v1/databases/{id}", func(w http.ResponseWriter, r *http.Request) {
		accountID := strings.TrimSpace(r.Header.Get("X-Account-ID"))
		if accountID == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "X-Account-ID header is required"})
			return
		}
		err := cp.DeleteDatabase(r.Context(), accountID, r.PathValue("id"))
		if err != nil {
			switch {
			case errors.Is(err, ErrNotFound):
				writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
			case errors.Is(err, ErrForbidden):
				writeJSON(w, http.StatusForbidden, map[string]string{"error": err.Error()})
			default:
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			}
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "purged", "tenant_id": r.PathValue("id")})
	})

	return mux
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
