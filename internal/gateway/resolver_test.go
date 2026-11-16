package gateway

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestHTTPRouteResolverAndAdminHandler(t *testing.T) {
	var routeCalls atomic.Int32
	var wakeCalls atomic.Int32
	var activityCalls atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Air-Internal-Token") != "secret-tok" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/internal/v1/tenants/t_demo/route":
			routeCalls.Add(1)
			_ = json.NewEncoder(w).Encode(TenantRoute{
				TenantID:    "t_demo",
				State:       "SUSPENDED",
				Port:        15444,
				SockDir:     "/tmp/sock",
				AppPassword: "pw",
			})
		case r.Method == http.MethodPost && r.URL.Path == "/internal/v1/tenants/t_demo/wake":
			wakeCalls.Add(1)
			_ = json.NewEncoder(w).Encode(TenantRoute{
				TenantID:    "t_demo",
				State:       "ACTIVE",
				Port:        15445,
				SockDir:     "/tmp/sock2",
				AppPassword: "pw",
			})
		case r.Method == http.MethodPost && r.URL.Path == "/internal/v1/tenants/t_demo/activity":
			activityCalls.Add(1)
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	resolver, err := NewHTTPRouteResolver(srv.URL, HTTPRouteResolverOptions{
		InternalToken: "secret-tok",
		BackendHost:   "10.0.0.12",
		TouchInterval: time.Second,
	})
	if err != nil {
		t.Fatalf("NewHTTPRouteResolver: %v", err)
	}

	route, err := resolver.GetTenantRouteAndCredentials(context.Background(), "t_demo")
	if err != nil {
		t.Fatalf("GetTenantRouteAndCredentials: %v", err)
	}
	if route.State != "SUSPENDED" || route.Port != 15444 || route.Host != "10.0.0.12" {
		t.Fatalf("unexpected route: %+v", route)
	}

	wRoute, err := resolver.WakeTenant(context.Background(), "t_demo")
	if err != nil {
		t.Fatalf("WakeTenant: %v", err)
	}
	if wRoute.State != "ACTIVE" || wRoute.Port != 15445 || wRoute.Host != "10.0.0.12" {
		t.Fatalf("unexpected wake route: %+v", wRoute)
	}

	// TouchTenantActivity should debounce rapid calls.
	resolver.TouchTenantActivity("t_demo")
	resolver.TouchTenantActivity("t_demo")
	deadline := time.Now().Add(2 * time.Second)
	for activityCalls.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if got := activityCalls.Load(); got != 1 {
		t.Fatalf("expected 1 debounced activity call, got %d", got)
	}
	if routeCalls.Load() != 1 || wakeCalls.Load() != 1 {
		t.Fatalf("unexpected call counts route=%d wake=%d", routeCalls.Load(), wakeCalls.Load())
	}

	gw, err := NewGatewayWithOptions("127.0.0.1:0", resolver, GatewayOptions{
		PoolMode:      true,
		InternalToken: "secret-tok",
	})
	if err != nil {
		t.Fatalf("NewGatewayWithOptions: %v", err)
	}
	defer func() { _ = gw.Close() }()

	admin := gw.AdminHandler()

	// Unauthorized drain should be rejected.
	req := httptest.NewRequest(http.MethodPost, "/internal/v1/tenants/t_demo/drain", nil)
	rec := httptest.NewRecorder()
	admin.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for missing token, got %d", rec.Code)
	}

	// Authorized drain should succeed.
	req = httptest.NewRequest(http.MethodPost, "/internal/v1/tenants/t_demo/drain?timeout=500ms", nil)
	req.Header.Set("X-Air-Internal-Token", "secret-tok")
	rec = httptest.NewRecorder()
	admin.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 for authorized drain, got %d (%s)", rec.Code, rec.Body.String())
	}

	// Healthz should return 200 OK.
	req = httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec = httptest.NewRecorder()
	admin.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 for /healthz, got %d", rec.Code)
	}
}
