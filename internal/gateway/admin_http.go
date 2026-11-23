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
	reGRPC        *ReadEngineGRPCClient
	psGRPC        *PageserverGRPCClient
	telemetry     *TelemetryStore

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

// SetNamespaceBackends attaches the read-engine and pageserver gRPC clients for /v1/namespaces endpoints.
func (s *AdminHTTPServer) SetNamespaceBackends(reGRPC *ReadEngineGRPCClient, psGRPC *PageserverGRPCClient) {
	s.reGRPC = reGRPC
	s.psGRPC = psGRPC
}

// SetTelemetryStore attaches the CellTelemetry store for /internal/v1/telemetry ingestion.
func (s *AdminHTTPServer) SetTelemetryStore(tel *TelemetryStore) {
	s.telemetry = tel
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
	if s.handleNamespacesRoute(w, r) {
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

func (s *AdminHTTPServer) handleNamespacesRoute(w http.ResponseWriter, r *http.Request) bool {
	if r.URL.Path != "/v1/namespaces" && !strings.HasPrefix(r.URL.Path, "/v1/namespaces/") {
		return false
	}
	if !s.isAuthorized(r) {
		writeJSON(w, http.StatusUnauthorized, `{"error":"unauthorized"}`)
		return true
	}
	suffix := strings.Trim(strings.TrimPrefix(r.URL.Path, "/v1/namespaces"), "/")
	switch {
	case suffix == "" && r.Method == http.MethodGet:
		routes := []TenantRoute{}
		if s.discovery != nil {
			routes = s.discovery.ListCached()
		}
		items := make([]map[string]any, 0, len(routes))
		for _, rt := range routes {
			items = append(items, map[string]any{
				"tenant_id": rt.TenantID,
				"namespace": rt.TenantID,
				"state":     rt.State,
				"air_host":  rt.Host,
				"air_port":  rt.Port,
			})
		}
		b, _ := json.Marshal(map[string]any{
			"service":    "air-gateway",
			"namespaces": items,
		})
		writeJSON(w, http.StatusOK, string(b))
		return true

	case r.Method == http.MethodPost:
		var req struct {
			TenantID       string `json:"tenant_id"`
			Namespace      string `json:"namespace"`
			State          string `json:"state"`
			Host           string `json:"host"`
			Port           int    `json:"port"`
			SockDir        string `json:"sock_dir"`
			AppPassword    string `json:"app_password"`
			ReadEngineHost string `json:"read_engine_host"`
			ReadEnginePort int    `json:"read_engine_port"`
			Generation     string `json:"generation"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		tid := strings.TrimSpace(suffix)
		if tid == "" {
			tid = strings.TrimSpace(req.TenantID)
		}
		if tid == "" {
			tid = strings.TrimSpace(req.Namespace)
		}
		if tid == "" {
			writeJSON(w, http.StatusBadRequest, `{"error":"tenant_id or namespace is required"}`)
			return true
		}
		route := TenantRoute{
			TenantID:    tid,
			State:       req.State,
			Host:        req.Host,
			Port:        req.Port,
			SockDir:     req.SockDir,
			AppPassword: req.AppPassword,
		}
		if route.State == "" {
			route.State = "ACTIVE"
		}
		airConnected := route.Port > 0 && route.AppPassword != ""
		if !airConnected && s.discovery != nil {
			if resolved, _, err := s.discovery.ResolveTenant(tid); err == nil {
				if route.Host == "" {
					route.Host = resolved.Host
				}
				if route.Port == 0 {
					route.Port = resolved.Port
				}
				if route.SockDir == "" {
					route.SockDir = resolved.SockDir
				}
				if route.AppPassword == "" {
					route.AppPassword = resolved.AppPassword
				}
				if resolved.State != "" {
					route.State = resolved.State
				}
				airConnected = route.Port > 0
			}
		}
		if s.discovery != nil {
			s.discovery.UpsertRoute(route)
		}
		if s.pools != nil {
			s.pools.UpsertDiscovered(route)
			if req.ReadEnginePort > 0 {
				tp := s.pools.GetOrCreate(route, nil)
				tp.SetReadEngineEndpoint(req.ReadEngineHost, "", req.ReadEnginePort)
			}
		}
		reConnected := false
		if s.reGRPC != nil {
			if _, err := s.reGRPC.EnsureTenant(r.Context(), tid); err == nil {
				reConnected = true
			}
		}
		psConnected := false
		if s.psGRPC != nil {
			if info, err := s.psGRPC.GetTenantInfo(r.Context(), tid); err == nil && info.GetExists() {
				psConnected = true
			} else if _, err := s.psGRPC.EnsureTenant(r.Context(), tid, req.Generation); err == nil {
				psConnected = true
			}
		}
		b, _ := json.Marshal(map[string]any{
			"status":                "registered",
			"service":               "air-gateway",
			"tenant_id":             tid,
			"namespace":             tid,
			"air_connected":         airConnected,
			"read_engine_connected": reConnected,
			"pageserver_connected":  psConnected,
		})
		writeJSON(w, http.StatusCreated, string(b))
		return true

	case suffix != "" && r.Method == http.MethodGet:
		var foundRoute *TenantRoute
		if s.discovery != nil {
			for _, rt := range s.discovery.ListCached() {
				if rt.TenantID == suffix {
					cp := rt
					foundRoute = &cp
					break
				}
			}
		}
		resp := map[string]any{
			"service":   "air-gateway",
			"tenant_id": suffix,
			"namespace": suffix,
		}
		if foundRoute != nil {
			resp["state"] = foundRoute.State
			resp["air_host"] = foundRoute.Host
			resp["air_port"] = foundRoute.Port
			resp["air_connected"] = foundRoute.Port > 0
		}
		if s.reGRPC != nil {
			if rr, err := s.reGRPC.GetTenantRoute(r.Context(), suffix); err == nil {
				resp["read_engine_host"] = rr.GetHost()
				resp["read_engine_port"] = rr.GetPgPort()
				resp["read_engine_connected"] = true
			}
		}
		if s.psGRPC != nil {
			if info, err := s.psGRPC.GetTenantInfo(r.Context(), suffix); err == nil {
				resp["pageserver_exists"] = info.GetExists()
				resp["pageserver_generation"] = info.GetGeneration()
				resp["pageserver_last_lsn"] = info.GetLastLsn()
				resp["pageserver_connected"] = info.GetExists()
			}
		}
		if foundRoute == nil && resp["pageserver_exists"] != true {
			writeJSON(w, http.StatusNotFound, `{"error":"namespace not found"}`)
			return true
		}
		b, _ := json.Marshal(resp)
		writeJSON(w, http.StatusOK, string(b))
		return true

	case suffix != "" && r.Method == http.MethodDelete:
		if s.discovery != nil {
			s.discovery.EvictCache(suffix)
		}
		if s.pools != nil {
			s.pools.RemoveTenant(suffix)
		}
		b, _ := json.Marshal(map[string]any{
			"status":    "deleted",
			"service":   "air-gateway",
			"tenant_id": suffix,
		})
		writeJSON(w, http.StatusOK, string(b))
		return true
	}
	writeJSON(w, http.StatusMethodNotAllowed, `{"error":"method not allowed"}`)
	return true
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

	if r.Method == http.MethodPost && r.URL.Path == "/internal/v1/telemetry" {
		if !s.isAuthorized(r) {
			writeJSON(w, http.StatusUnauthorized, `{"error":"unauthorized"}`)
			return true
		}
		if s.telemetry == nil {
			writeJSON(w, http.StatusServiceUnavailable, `{"error":"telemetry store not configured"}`)
			return true
		}
		var snap CellTelemetry
		body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if err := json.Unmarshal(body, &snap); err != nil {
			writeJSON(w, http.StatusBadRequest, makeErrorJSON(err.Error()))
			return true
		}
		s.telemetry.Update(snap, time.Now())
		writeJSON(w, http.StatusOK, `{"status":"accepted"}`)
		return true
	}

	if r.Method == http.MethodGet && r.URL.Path == "/internal/v1/write-governor" {
		if !s.isAuthorized(r) {
			writeJSON(w, http.StatusUnauthorized, `{"error":"unauthorized"}`)
			return true
		}
		gov := s.pools.WriteGovernor()
		if gov == nil {
			writeJSON(w, http.StatusOK, `{"enabled":false}`)
			return true
		}
		b, _ := json.Marshal(gov.Snapshot())
		writeJSON(w, http.StatusOK, string(b))
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
