package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// HTTPRouteResolver resolves tenant routes, wakes suspended databases, and reports activity
// to a remote or local air-server Control Plane over HTTP.
type HTTPRouteResolver struct {
	baseURL       string
	internalToken string
	backendHost   string
	client        *http.Client

	touchMu       sync.Mutex
	lastTouchSent map[string]time.Time
	touchInterval time.Duration
}

// HTTPRouteResolverOptions configures HTTPRouteResolver.
type HTTPRouteResolverOptions struct {
	InternalToken string
	BackendHost   string
	HTTPClient    *http.Client
	TouchInterval time.Duration
}

// NewHTTPRouteResolver creates a RouteResolver backed by air-server's /internal/v1 HTTP endpoints.
func NewHTTPRouteResolver(baseURL string, opts HTTPRouteResolverOptions) (*HTTPRouteResolver, error) {
	trimmed := strings.TrimRight(strings.TrimSpace(baseURL), "/")
	if trimmed == "" {
		return nil, fmt.Errorf("control plane base URL is required")
	}
	client := opts.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 35 * time.Second}
	}
	interval := opts.TouchInterval
	if interval <= 0 {
		interval = 5 * time.Second
	}
	return &HTTPRouteResolver{
		baseURL:       trimmed,
		internalToken: opts.InternalToken,
		backendHost:   strings.TrimSpace(opts.BackendHost),
		client:        client,
		lastTouchSent: make(map[string]time.Time),
		touchInterval: interval,
	}, nil
}

// GetTenantRouteAndCredentials fetches tenant routing state, backend endpoint, and app_user password.
func (r *HTTPRouteResolver) GetTenantRouteAndCredentials(ctx context.Context, tenantID string) (TenantRoute, error) {
	endpoint := fmt.Sprintf("%s/internal/v1/tenants/%s/route", r.baseURL, url.PathEscape(tenantID))
	return r.doRouteRequest(ctx, http.MethodGet, endpoint)
}

// WakeTenant asks air-server to wake a suspended tenant and returns the active route.
func (r *HTTPRouteResolver) WakeTenant(ctx context.Context, tenantID string) (TenantRoute, error) {
	endpoint := fmt.Sprintf("%s/internal/v1/tenants/%s/wake", r.baseURL, url.PathEscape(tenantID))
	return r.doRouteRequest(ctx, http.MethodPost, endpoint)
}

// TouchTenantActivity asynchronously notifies air-server of client activity, debounced per tenant.
func (r *HTTPRouteResolver) TouchTenantActivity(tenantID string) {
	if tenantID == "" {
		return
	}
	now := time.Now()
	r.touchMu.Lock()
	if prev, ok := r.lastTouchSent[tenantID]; ok && now.Sub(prev) < r.touchInterval {
		r.touchMu.Unlock()
		return
	}
	r.lastTouchSent[tenantID] = now
	r.touchMu.Unlock()

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		endpoint := fmt.Sprintf("%s/internal/v1/tenants/%s/activity", r.baseURL, url.PathEscape(tenantID))
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, nil)
		if err != nil {
			return
		}
		if r.internalToken != "" {
			req.Header.Set("X-Air-Internal-Token", r.internalToken)
		}
		resp, err := r.client.Do(req)
		if err != nil {
			return
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()
}

func (r *HTTPRouteResolver) doRouteRequest(ctx context.Context, method, endpoint string) (TenantRoute, error) {
	req, err := http.NewRequestWithContext(ctx, method, endpoint, nil)
	if err != nil {
		return TenantRoute{}, err
	}
	if r.internalToken != "" {
		req.Header.Set("X-Air-Internal-Token", r.internalToken)
	}
	resp, err := r.client.Do(req)
	if err != nil {
		return TenantRoute{}, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return TenantRoute{}, fmt.Errorf("control plane %s %s failed (%d): %s", method, endpoint, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var route TenantRoute
	if err := json.NewDecoder(resp.Body).Decode(&route); err != nil {
		return TenantRoute{}, fmt.Errorf("decode route response: %w", err)
	}
	if r.backendHost != "" {
		route.Host = r.backendHost
	}
	return route, nil
}
