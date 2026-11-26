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

	"google.golang.org/grpc/status"
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
		Handler:           http.HandlerFunc(s.serveHTTP),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      65 * time.Second,
		MaxHeaderBytes:    1 << 20,
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
	if ConstantTimeEquals(strings.TrimSpace(r.Header.Get("X-Air-Internal-Token")), s.internalToken) {
		return true
	}
	auth := r.Header.Get("Authorization")
	if len(auth) > 7 && strings.EqualFold(auth[:7], "Bearer ") && ConstantTimeEquals(strings.TrimSpace(auth[7:]), s.internalToken) {
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
	if r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/v1/api-keys/") {
		if !s.isAuthorized(r) {
			writeJSON(w, http.StatusUnauthorized, `{"error":"unauthorized"}`)
			return
		}
		keyID := strings.TrimPrefix(r.URL.Path, "/v1/api-keys/")
		if !ValidateTenantID(keyID) {
			writeJSON(w, http.StatusBadRequest, `{"error":"invalid key id"}`)
			return
		}
		code, resp, _ := s.discovery.RevokeAPIKey(keyID)
		writeJSON(w, code, resp)
		return
	}
	if s.handleClusterAndBackendRoutes(w, r) {
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
		if body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20)); err == nil && len(body) > 0 {
			if err := json.Unmarshal(body, &req); err != nil {
				writeJSON(w, http.StatusBadRequest, `{"error":"invalid JSON body"}`)
				return true
			}
		}
		tid := strings.TrimSpace(suffix)
		if tid == "" {
			tid = strings.TrimSpace(req.TenantID)
		}
		if tid == "" {
			tid = strings.TrimSpace(req.Namespace)
		}
		if !ValidateTenantID(tid) {
			writeJSON(w, http.StatusBadRequest, `{"error":"invalid tenant_id or namespace"}`)
			return true
		}
		if req.Generation != "" && !ValidateTenantID(req.Generation) {
			writeJSON(w, http.StatusBadRequest, `{"error":"invalid generation"}`)
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
		if !ValidateTenantID(suffix) {
			writeJSON(w, http.StatusBadRequest, `{"error":"invalid namespace"}`)
			return true
		}
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
		if !ValidateTenantID(suffix) {
			writeJSON(w, http.StatusBadRequest, `{"error":"invalid namespace"}`)
			return true
		}
		if s.pools != nil {
			if err := s.pools.RequestDrain(suffix, 2000*time.Millisecond); err != nil {
				writeJSON(w, http.StatusConflict, makeErrorJSON(err.Error()))
				return true
			}
			s.pools.RemoveTenant(suffix)
		}
		if s.discovery != nil {
			s.discovery.EvictCache(suffix)
		}
		reDeleted := false
		if s.reGRPC != nil {
			if _, err := s.reGRPC.DeleteTenant(r.Context(), suffix); err != nil {
				writeJSON(w, http.StatusBadGateway, `{"error":"read engine purge failed"}`)
				return true
			}
			reDeleted = true
		}
		psPurged := false
		if s.psGRPC != nil {
			if _, err := s.psGRPC.PurgeTenant(r.Context(), suffix, ""); err != nil {
				writeJSON(w, http.StatusBadGateway, `{"error":"pageserver purge failed"}`)
				return true
			}
			psPurged = true
		}
		b, _ := json.Marshal(map[string]any{
			"status":             "deleted",
			"service":            "air-gateway",
			"tenant_id":          suffix,
			"read_engine_purged": reDeleted,
			"pageserver_purged":  psPurged,
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
				Namespace string `json:"namespace"`
				AccountID string `json:"account_id"`
			}
			body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
			_ = json.Unmarshal(body, &payload)
			tid := strings.TrimSpace(payload.TenantID)
			if tid == "" {
				tid = strings.TrimSpace(payload.Namespace)
			}
			code, resp, ok := s.discovery.CreateDatabase(tid, payload.AccountID, r.Header.Get("Idempotency-Key"))
			if ok && tid != "" {
				s.ensureNamespaceBackends(r.Context(), tid)
			}
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
			var ok bool
			code, resp, ok = s.discovery.StartDatabase(tid)
			if ok && s.reGRPC != nil {
				_, _ = s.reGRPC.EnsureTenant(r.Context(), tid)
			}
		case "lock":
			code, resp, _ = s.discovery.LockDatabase(tid)
		case "unlock":
			code, resp, _ = s.discovery.UnlockDatabase(tid)
		}
		writeJSON(w, code, resp)
		return true
	}

	if action == "backups" && (r.Method == http.MethodPost || r.Method == http.MethodGet) {
		if r.Method == http.MethodPost {
			var payload struct {
				Type string `json:"type"`
			}
			body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
			_ = json.Unmarshal(body, &payload)
			code, resp, _ := s.discovery.TriggerBackup(tid, payload.Type)
			writeJSON(w, code, resp)
			return true
		}
		code, resp, _ := s.discovery.ListBackups(tid)
		writeJSON(w, code, resp)
		return true
	}

	if r.Method == http.MethodPost && strings.HasPrefix(action, "backups/") && strings.HasSuffix(action, "/verify") {
		backupID := strings.TrimSuffix(strings.TrimPrefix(action, "backups/"), "/verify")
		if !ValidateTenantID(backupID) {
			writeJSON(w, http.StatusBadRequest, `{"error":"invalid backup id"}`)
			return true
		}
		var payload struct {
			StreamHash  bool `json:"stream_hash"`
			DeepRestore bool `json:"deep_restore"`
		}
		body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		_ = json.Unmarshal(body, &payload)
		code, resp, _ := s.discovery.VerifyBackup(tid, backupID, payload.StreamHash, payload.DeepRestore)
		writeJSON(w, code, resp)
		return true
	}

	if r.Method == http.MethodPost && action == "restore" {
		var payload struct {
			BackupID    string `json:"backup_id"`
			TargetLSN   string `json:"target_lsn"`
			TargetTime  string `json:"target_time"`
			NewTenantID string `json:"new_tenant_id"`
			VerifyQuery string `json:"verify_query"`
		}
		body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		_ = json.Unmarshal(body, &payload)
		code, resp, _ := s.discovery.RestoreDatabase(tid, payload.BackupID, payload.TargetLSN, payload.TargetTime, payload.NewTenantID, payload.VerifyQuery)
		writeJSON(w, code, resp)
		return true
	}

	if r.Method == http.MethodGet && action == "offload" {
		code, resp, _ := s.discovery.GetOffloadStatus(tid)
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

func (s *AdminHTTPServer) ensureNamespaceBackends(ctx context.Context, tid string) {
	if s.reGRPC != nil {
		_, _ = s.reGRPC.EnsureTenant(ctx, tid)
	}
	if s.psGRPC != nil {
		if info, err := s.psGRPC.GetTenantInfo(ctx, tid); err != nil || !info.GetExists() {
			_, _ = s.psGRPC.EnsureTenant(ctx, tid, "")
		}
	}
}

func (s *AdminHTTPServer) handleClusterAndBackendRoutes(w http.ResponseWriter, r *http.Request) bool {
	path := r.URL.Path
	switch {
	case (path == "/v1/cluster/health" || path == "/v1/status") && r.Method == http.MethodGet:
		if !s.isAuthorized(r) {
			writeJSON(w, http.StatusUnauthorized, `{"error":"unauthorized"}`)
			return true
		}
		services := map[string]any{
			"air-gateway": map[string]any{
				"status":  "ok",
				"tenants": len(s.discovery.ListCached()),
			},
		}
		if s.discovery != nil {
			code, _, ok := s.discovery.ListDatabases("")
			if ok {
				services["air-server"] = map[string]any{"status": "ok"}
			} else {
				services["air-server"] = map[string]any{"status": "degraded", "http_code": code}
			}
		}
		if s.reGRPC != nil {
			if h, err := s.reGRPC.Health(r.Context()); err == nil {
				services["air-read-engine"] = map[string]any{
					"status":          h.GetStatus(),
					"state":           h.GetState(),
					"pg_port":         h.GetPgPort(),
					"queries_allowed": h.GetQueriesAllowed(),
					"writes_blocked":  h.GetWritesBlocked(),
				}
			} else {
				services["air-read-engine"] = map[string]any{"status": "unavailable", "error": err.Error()}
			}
		}
		if s.psGRPC != nil {
			if h, err := s.psGRPC.Health(r.Context()); err == nil {
				services["air-pageserver"] = map[string]any{
					"status":         h.GetStatus(),
					"active_tenants": h.GetActiveTenants(),
				}
			} else {
				services["air-pageserver"] = map[string]any{"status": "unavailable", "error": err.Error()}
			}
		}
		b, _ := json.Marshal(map[string]any{
			"status":   "ok",
			"services": services,
		})
		writeJSON(w, http.StatusOK, string(b))
		return true

	case path == "/v1/pageserver/flush" && r.Method == http.MethodPost:
		if !s.isAuthorized(r) {
			writeJSON(w, http.StatusUnauthorized, `{"error":"unauthorized"}`)
			return true
		}
		if s.psGRPC == nil {
			writeJSON(w, http.StatusServiceUnavailable, `{"error":"pageserver gRPC not configured"}`)
			return true
		}
		resp, err := s.psGRPC.FlushAll(r.Context())
		if err != nil {
			st := status.Convert(err)
			writeJSON(w, grpcToHTTP(st.Code()), makeErrorJSON(st.Message()))
			return true
		}
		b, _ := json.Marshal(map[string]any{"status": resp.GetStatus()})
		writeJSON(w, http.StatusOK, string(b))
		return true

	case path == "/v1/pageserver/tenants" && r.Method == http.MethodGet:
		if !s.isAuthorized(r) {
			writeJSON(w, http.StatusUnauthorized, `{"error":"unauthorized"}`)
			return true
		}
		if s.psGRPC == nil {
			writeJSON(w, http.StatusServiceUnavailable, `{"error":"pageserver gRPC not configured"}`)
			return true
		}
		resp, err := s.psGRPC.ListTenants(r.Context())
		if err != nil {
			st := status.Convert(err)
			writeJSON(w, grpcToHTTP(st.Code()), makeErrorJSON(st.Message()))
			return true
		}
		items := make([]map[string]any, 0, len(resp.GetTenants()))
		for _, t := range resp.GetTenants() {
			items = append(items, map[string]any{
				"tenant_id":  t.GetTenantId(),
				"generation": t.GetGeneration(),
				"last_lsn":   t.GetLastLsn(),
				"exists":     t.GetExists(),
			})
		}
		b, _ := json.Marshal(map[string]any{"tenants": items})
		writeJSON(w, http.StatusOK, string(b))
		return true

	case strings.HasPrefix(path, "/v1/pageserver/tenants/"):
		if !s.isAuthorized(r) {
			writeJSON(w, http.StatusUnauthorized, `{"error":"unauthorized"}`)
			return true
		}
		if s.psGRPC == nil {
			writeJSON(w, http.StatusServiceUnavailable, `{"error":"pageserver gRPC not configured"}`)
			return true
		}
		rest := strings.Trim(strings.TrimPrefix(path, "/v1/pageserver/tenants/"), "/")
		parts := strings.Split(rest, "/")
		tid := parts[0]
		if !ValidateTenantID(tid) {
			writeJSON(w, http.StatusBadRequest, `{"error":"invalid tenant id"}`)
			return true
		}
		if len(parts) == 1 && r.Method == http.MethodGet {
			info, err := s.psGRPC.GetTenantInfo(r.Context(), tid)
			if err != nil {
				st := status.Convert(err)
				writeJSON(w, grpcToHTTP(st.Code()), makeErrorJSON(st.Message()))
				return true
			}
			if !info.GetExists() {
				writeJSON(w, http.StatusNotFound, `{"error":"tenant not found"}`)
				return true
			}
			b, _ := json.Marshal(map[string]any{
				"tenant_id":  info.GetTenantId(),
				"generation": info.GetGeneration(),
				"last_lsn":   info.GetLastLsn(),
				"exists":     info.GetExists(),
			})
			writeJSON(w, http.StatusOK, string(b))
			return true
		}
		if len(parts) == 2 && parts[1] == "flush" && r.Method == http.MethodPost {
			resp, err := s.psGRPC.FlushTenant(r.Context(), tid)
			if err != nil {
				st := status.Convert(err)
				writeJSON(w, grpcToHTTP(st.Code()), makeErrorJSON(st.Message()))
				return true
			}
			b, _ := json.Marshal(map[string]any{
				"tenant_id": resp.GetTenantId(),
				"flushed":   resp.GetFlushed(),
				"file_name": resp.GetFileName(),
				"start_lsn": resp.GetStartLsn(),
				"end_lsn":   resp.GetEndLsn(),
			})
			writeJSON(w, http.StatusOK, string(b))
			return true
		}
		if len(parts) == 1 && r.Method == http.MethodDelete {
			gen := strings.TrimSpace(r.URL.Query().Get("generation"))
			if gen != "" && !ValidateTenantID(gen) {
				writeJSON(w, http.StatusBadRequest, `{"error":"invalid generation"}`)
				return true
			}
			resp, err := s.psGRPC.PurgeTenant(r.Context(), tid, gen)
			if err != nil {
				st := status.Convert(err)
				writeJSON(w, grpcToHTTP(st.Code()), makeErrorJSON(st.Message()))
				return true
			}
			b, _ := json.Marshal(map[string]any{
				"status":    resp.GetStatus(),
				"tenant_id": resp.GetTenantId(),
			})
			writeJSON(w, http.StatusOK, string(b))
			return true
		}
		return false

	case path == "/v1/read-engine/tenants" && r.Method == http.MethodGet:
		if !s.isAuthorized(r) {
			writeJSON(w, http.StatusUnauthorized, `{"error":"unauthorized"}`)
			return true
		}
		if s.reGRPC == nil {
			writeJSON(w, http.StatusServiceUnavailable, `{"error":"read-engine gRPC not configured"}`)
			return true
		}
		resp, err := s.reGRPC.ListTenants(r.Context())
		if err != nil {
			st := status.Convert(err)
			writeJSON(w, grpcToHTTP(st.Code()), makeErrorJSON(st.Message()))
			return true
		}
		b, _ := json.Marshal(map[string]any{"tenants": resp.GetTenantIds()})
		writeJSON(w, http.StatusOK, string(b))
		return true

	case strings.HasPrefix(path, "/v1/read-engine/tenants/"):
		if !s.isAuthorized(r) {
			writeJSON(w, http.StatusUnauthorized, `{"error":"unauthorized"}`)
			return true
		}
		if s.reGRPC == nil {
			writeJSON(w, http.StatusServiceUnavailable, `{"error":"read-engine gRPC not configured"}`)
			return true
		}
		rest := strings.Trim(strings.TrimPrefix(path, "/v1/read-engine/tenants/"), "/")
		parts := strings.Split(rest, "/")
		tid := parts[0]
		if !ValidateTenantID(tid) {
			writeJSON(w, http.StatusBadRequest, `{"error":"invalid tenant id"}`)
			return true
		}
		if len(parts) == 1 && r.Method == http.MethodGet {
			route, err := s.reGRPC.GetTenantRoute(r.Context(), tid)
			if err != nil {
				st := status.Convert(err)
				writeJSON(w, grpcToHTTP(st.Code()), makeErrorJSON(st.Message()))
				return true
			}
			b, _ := json.Marshal(map[string]any{
				"tenant_id": route.GetTenantId(),
				"engine":    route.GetEngine(),
				"host":      route.GetHost(),
				"pg_port":   route.GetPgPort(),
				"state":     route.GetState(),
			})
			writeJSON(w, http.StatusOK, string(b))
			return true
		}
		if len(parts) == 2 && parts[1] == "sync" && r.Method == http.MethodPost {
			var payload struct {
				SQL             string `json:"sql"`
				LSN             string `json:"lsn"`
				PrimaryConninfo string `json:"primary_conninfo"`
			}
			body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
			_ = json.Unmarshal(body, &payload)
			resp, err := s.reGRPC.SyncTenant(r.Context(), tid, payload.SQL, payload.LSN, payload.PrimaryConninfo)
			if err != nil {
				st := status.Convert(err)
				writeJSON(w, grpcToHTTP(st.Code()), makeErrorJSON(st.Message()))
				return true
			}
			b, _ := json.Marshal(map[string]any{
				"tenant_id": resp.GetTenantId(),
				"status":    resp.GetStatus(),
			})
			writeJSON(w, http.StatusOK, string(b))
			return true
		}
		if len(parts) == 1 && r.Method == http.MethodDelete {
			resp, err := s.reGRPC.DeleteTenant(r.Context(), tid)
			if err != nil {
				st := status.Convert(err)
				writeJSON(w, grpcToHTTP(st.Code()), makeErrorJSON(st.Message()))
				return true
			}
			b, _ := json.Marshal(map[string]any{
				"tenant_id": resp.GetTenantId(),
				"status":    resp.GetStatus(),
			})
			writeJSON(w, http.StatusOK, string(b))
			return true
		}
		return false
	}
	return false
}
