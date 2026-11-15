package control

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
)

type createDatabaseReq struct {
	TenantID string `json:"tenant_id"`
}

type triggerBackupReq struct {
	Type string `json:"type"`
}

type restoreDatabaseReq struct {
	TargetLSN   string `json:"target_lsn"`
	VerifyQuery string `json:"verify_query"`
}

// HTTPHandler returns an http.Handler exposing the AIR Control Plane REST API:
//   - GET    /healthz
//   - POST   /v1/databases         (Header: Authorization: Bearer <token>, Idempotency-Key; Body: {"tenant_id": "..."})
//   - GET    /v1/databases/{id}    (Header: Authorization: Bearer <token>)
//   - DELETE /v1/databases/{id}    (Header: Authorization: Bearer <token>)
//   - DELETE /v1/api-keys/{id}     (Header: Authorization: Bearer <token>)
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
		accountID, ok := cp.authenticateRequest(w, r)
		if !ok {
			return
		}
		idemKey := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
		if idemKey == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Idempotency-Key header is required"})
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
		accountID, ok := cp.authenticateRequest(w, r)
		if !ok {
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
		accountID, ok := cp.authenticateRequest(w, r)
		if !ok {
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

	mux.HandleFunc("DELETE /v1/api-keys/{id}", func(w http.ResponseWriter, r *http.Request) {
		accountID, ok := cp.authenticateRequest(w, r)
		if !ok {
			return
		}
		err := cp.RevokeAPIKey(r.Context(), accountID, r.PathValue("id"))
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
		writeJSON(w, http.StatusOK, map[string]string{"status": "revoked", "key_id": r.PathValue("id")})
	})

	mux.HandleFunc("POST /v1/databases/{id}/backups", func(w http.ResponseWriter, r *http.Request) {
		accountID, ok := cp.authenticateRequest(w, r)
		if !ok {
			return
		}
		var req triggerBackupReq
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16*1024)).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON body"})
			return
		}
		resp, err := cp.TriggerBackup(r.Context(), accountID, r.PathValue("id"), req.Type)
		if err != nil {
			switch {
			case errors.Is(err, ErrNotFound):
				writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
			case errors.Is(err, ErrForbidden):
				writeJSON(w, http.StatusForbidden, map[string]string{"error": err.Error()})
			case errors.Is(err, ErrBackupNotConfigured):
				writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": err.Error()})
			default:
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			}
			return
		}
		writeJSON(w, http.StatusCreated, resp)
	})

	mux.HandleFunc("POST /v1/databases/{id}/restore", func(w http.ResponseWriter, r *http.Request) {
		accountID, ok := cp.authenticateRequest(w, r)
		if !ok {
			return
		}
		var req restoreDatabaseReq
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16*1024)).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON body"})
			return
		}
		resp, err := cp.RestoreDatabase(r.Context(), accountID, r.PathValue("id"), req.TargetLSN, req.VerifyQuery)
		if err != nil {
			switch {
			case errors.Is(err, ErrNotFound):
				writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
			case errors.Is(err, ErrForbidden):
				writeJSON(w, http.StatusForbidden, map[string]string{"error": err.Error()})
			case errors.Is(err, ErrBackupNotConfigured):
				writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": err.Error()})
			default:
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			}
			return
		}
		writeJSON(w, http.StatusOK, resp)
	})

	return mux
}

func (cp *ControlPlane) authenticateRequest(w http.ResponseWriter, r *http.Request) (string, bool) {
	authHeader := strings.TrimSpace(r.Header.Get("Authorization"))
	if len(authHeader) < 8 || !strings.EqualFold(authHeader[:7], "Bearer ") {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": ErrUnauthorized.Error()})
		return "", false
	}
	rawToken := strings.TrimSpace(authHeader[7:])
	accountID, _, err := cp.AuthenticateAPIKey(r.Context(), rawToken)
	if err != nil {
		if errors.Is(err, ErrUnauthorized) {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": err.Error()})
		} else {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "authentication check failed"})
		}
		return "", false
	}
	if hdrAcct := strings.TrimSpace(r.Header.Get("X-Account-ID")); hdrAcct != "" && hdrAcct != accountID {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": ErrForbidden.Error()})
		return "", false
	}
	return accountID, true
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
