package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"
)

// AdminHTTPServer exposes health, Prometheus/JSON metrics, tenant limits/drain,
// and /v1/databases* proxy routes to Control Plane gRPC.
type AdminHTTPServer struct {
	addr          string
	internalToken string
	discovery     *TenantDiscovery
	pools         *PoolManager
	metrics       *MetricsRegistry

	ln  net.Listener
	srv *http.Server
}

func NewAdminHTTPServer(addr, internalToken string, discovery *TenantDiscovery, pools *PoolManager, metrics *MetricsRegistry) *AdminHTTPServer {
	return &AdminHTTPServer{
		addr:          addr,
		internalToken: internalToken,
		discovery:     discovery,
		pools:         pools,
		metrics:       metrics,
	}
}

func (s *AdminHTTPServer) Start() error {
	if s.addr == "" {
		return nil
	}
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	s.ln = ln
	s.srv = &http.Server{
		Handler:      http.HandlerFunc(s.serveHTTP),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 65 * time.Second,
	}
	go func() {
		_ = s.srv.Serve(ln)
	}()
	return nil
}

func (s *AdminHTTPServer) Addr() string {
	if s.ln != nil {
		return s.ln.Addr().String()
	}
	return s.addr
}

func (s *AdminHTTPServer) Stop() {
	if s.srv != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = s.srv.Shutdown(ctx)
		s.srv = nil
	}
}

func writeJSON(w http.ResponseWriter, code int, body string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Connection", "close")
	w.WriteHeader(code)
	if body == "" {
		body = "{}"
	}
	_, _ = io.WriteString(w, body)
}

func (s *AdminHTTPServer) isAuthorized(r *http.Request) bool {
	if s.internalToken == "" {
		return true
	}
	if r.Header.Get("X-Air-Internal-Token") == s.internalToken {
		return true
	}
	auth := r.Header.Get("Authorization")
	if len(auth) > 7 && strings.EqualFold(auth[:7], "Bearer ") && strings.TrimSpace(auth[7:]) == s.internalToken {
		return true
	}
	return false
}

func (s *AdminHTTPServer) serveHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet && r.URL.Path == "/healthz" {
		writeJSON(w, http.StatusOK, `{"status":"ok"}`)
		return
	}
	if s.handleDatabasesRoute(w, r) {
		return
	}
	if s.handleInternalRoute(w, r) {
		return
	}
	writeJSON(w, http.StatusNotFound, `{"error":"not found"}`)
}

func (s *AdminHTTPServer) handleDatabasesRoute(w http.ResponseWriter, r *http.Request) bool {
	dbPath := r.URL.Path
	if strings.HasPrefix(dbPath, "/admin/v1/databases") {
		dbPath = dbPath[len("/admin"):]
	}

	if dbPath == "/v1/databases" && (r.Method == http.MethodPost || r.Method == http.MethodGet) {
		if !s.isAuthorized(r) {
			writeJSON(w, http.StatusUnauthorized, `{"error":"unauthorized"}`)
			return true
		}
		if r.Method == http.MethodPost {
			var payload struct {
				TenantID  string `json:"tenant_id"`
				AccountID string `json:"account_id"`
			}
			body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
			_ = json.Unmarshal(body, &payload)
			code, resp, _ := s.discovery.CreateDatabase(payload.TenantID, payload.AccountID, r.Header.Get("Idempotency-Key"))
			writeJSON(w, code, resp)
			return true
		}
		code, resp, _ := s.discovery.ListDatabases(r.URL.Query().Get("account_id"))
		writeJSON(w, code, resp)
		return true
	}

	const dbPrefix = "/v1/databases/"
	if !strings.HasPrefix(dbPath, dbPrefix) || len(dbPath) <= len(dbPrefix) {
		return false
	}
	if !s.isAuthorized(r) {
		writeJSON(w, http.StatusUnauthorized, `{"error":"unauthorized"}`)
		return true
	}

	rest := dbPath[len(dbPrefix):]
	tid := rest
	action := ""
	if slash := strings.IndexByte(rest, '/'); slash >= 0 {
		tid = rest[:slash]
		action = rest[slash+1:]
	}
	if !ValidateTenantID(tid) {
		writeJSON(w, http.StatusBadRequest, `{"error":"invalid tenant id"}`)
		return true
	}

	if action == "" && (r.Method == http.MethodGet || r.Method == http.MethodDelete) {
		if r.Method == http.MethodDelete {
			if err := s.pools.RequestDrain(tid, 2000*time.Millisecond); err != nil {
				writeJSON(w, http.StatusConflict, makeErrorJSON(err.Error()))
				return true
			}
			s.pools.RemoveTenant(tid)
			s.discovery.EvictCache(tid)
			code, resp, _ := s.discovery.DeleteDatabase(tid)
			writeJSON(w, code, resp)
			return true
		}
		code, resp, _ := s.discovery.GetDatabase(tid)
		writeJSON(w, code, resp)
		return true
	}

	if r.Method == http.MethodPost && (action == "suspend" || action == "start" || action == "wake" || action == "lock" || action == "unlock") {
		if action == "wake" {
			action = "start"
		}
		if action == "suspend" || action == "lock" {
			if err := s.pools.RequestDrain(tid, 3000*time.Millisecond); err != nil {
				writeJSON(w, http.StatusConflict, makeErrorJSON(err.Error()))
				return true
			}
			if action == "lock" {
				s.pools.RemoveTenant(tid)
			}
		}
		var code int
		var resp string
		switch action {
		case "suspend":
			code, resp, _ = s.discovery.SuspendDatabase(tid)
		case "start":
			code, resp, _ = s.discovery.StartDatabase(tid)
		case "lock":
			code, resp, _ = s.discovery.LockDatabase(tid)
		case "unlock":
			code, resp, _ = s.discovery.UnlockDatabase(tid)
		}
		writeJSON(w, code, resp)
		return true
	}

	return false
}

func (s *AdminHTTPServer) handleInternalRoute(w http.ResponseWriter, r *http.Request) bool {
	if r.Method == http.MethodGet && r.URL.Path == "/metrics" {
		if !s.isAuthorized(r) {
			writeJSON(w, http.StatusUnauthorized, `{"error":"unauthorized"}`)
			return true
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		w.Header().Set("Connection", "close")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, s.metrics.ToPrometheus(s.pools.Snapshots()))
		return true
	}

	if r.Method == http.MethodGet && r.URL.Path == "/internal/v1/metrics" {
		if !s.isAuthorized(r) {
			writeJSON(w, http.StatusUnauthorized, `{"error":"unauthorized"}`)
			return true
		}
		q := r.URL.Query()
		writeJSON(w, http.StatusOK, s.metrics.ToJSON(s.pools.Snapshots(), q.Get("tenant_id"), q.Get("user")))
		return true
	}

	if r.Method == http.MethodGet && r.URL.Path == "/internal/v1/tenants" {
		if !s.isAuthorized(r) {
			writeJSON(w, http.StatusUnauthorized, `{"error":"unauthorized"}`)
			return true
		}
		list := s.discovery.ListCached()
		var b strings.Builder
		b.WriteString(`{"tenants":[`)
		for i, t := range list {
			if i > 0 {
				b.WriteByte(',')
			}
			fmt.Fprintf(&b, `{"tenant_id":"%s","state":"%s","port":%d}`, JSONEscape(t.TenantID), JSONEscape(t.State), t.Port)
		}
		b.WriteString(`]}`)
		writeJSON(w, http.StatusOK, b.String())
		return true
	}

	const prefix = "/internal/v1/tenants/"
	const limitsSuffix = "/limits"
	if r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, prefix) && strings.HasSuffix(r.URL.Path, limitsSuffix) &&
		len(r.URL.Path) > len(prefix)+len(limitsSuffix) {
		if !s.isAuthorized(r) {
			writeJSON(w, http.StatusUnauthorized, `{"error":"unauthorized"}`)
			return true
		}
		tid := r.URL.Path[len(prefix) : len(r.URL.Path)-len(limitsSuffix)]
		if !ValidateTenantID(tid) {
			writeJSON(w, http.StatusBadRequest, `{"error":"invalid tenant id"}`)
			return true
		}
		var payload struct {
			MaxClientConns int `json:"max_client_conns"`
			MaxBackends    int `json:"max_backends"`
		}
		body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		_ = json.Unmarshal(body, &payload)
		if payload.MaxClientConns > 0 {
			s.metrics.SetTenantClientLimit(tid, payload.MaxClientConns)
		}
		if payload.MaxBackends > 0 {
			s.pools.SetTenantMaxBackends(tid, payload.MaxBackends)
		}
		out := fmt.Sprintf(`{"status":"updated","tenant_id":"%s","max_client_conns":%d}`,
			JSONEscape(tid), s.metrics.GetTenantClientLimit(tid))
		writeJSON(w, http.StatusOK, out)
		return true
	}

	const drainSuffix = "/drain"
	if r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, prefix) && strings.HasSuffix(r.URL.Path, drainSuffix) &&
		len(r.URL.Path) > len(prefix)+len(drainSuffix) {
		if !s.isAuthorized(r) {
			writeJSON(w, http.StatusUnauthorized, `{"error":"unauthorized"}`)
			return true
		}
		tid := r.URL.Path[len(prefix) : len(r.URL.Path)-len(drainSuffix)]
		if !ValidateTenantID(tid) {
			writeJSON(w, http.StatusBadRequest, `{"error":"invalid tenant id"}`)
			return true
		}
		timeout := 2000 * time.Millisecond
		if tval := r.URL.Query().Get("timeout"); tval != "" {
			if d, err := time.ParseDuration(tval); err == nil && d > 0 {
				timeout = d
			}
		}
		if err := s.pools.RequestDrain(tid, timeout); err != nil {
			writeJSON(w, http.StatusConflict, makeErrorJSON(err.Error()))
			return true
		}
		writeJSON(w, http.StatusOK, fmt.Sprintf(`{"status":"drained","tenant_id":"%s"}`, JSONEscape(tid)))
		return true
	}

	return false
}
