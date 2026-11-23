package cell

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"
)

// TelemetryPublisher periodically samples cgroup v2 and Linux PSI resource telemetry
// from a Cell Agent and pushes it to the AIR Gateway's /internal/v1/telemetry endpoint.
// Supports both localhost and cross-machine HTTP/HTTPS deployments.
type TelemetryPublisher struct {
	agent         *Agent
	gatewayURL    string
	internalToken string
	tenantID      string // optional: if set, samples tenant cgroup; otherwise samples cell cgroupRoot
	interval      time.Duration
	client        *http.Client
}

// NewTelemetryPublisher creates a TelemetryPublisher targeting gatewayAdminURL.
func NewTelemetryPublisher(agent *Agent, gatewayAdminURL, internalToken, tenantID string, interval time.Duration) *TelemetryPublisher {
	if interval <= 0 {
		interval = time.Second
	}
	return &TelemetryPublisher{
		agent:         agent,
		gatewayURL:    strings.TrimRight(gatewayAdminURL, "/"),
		internalToken: internalToken,
		tenantID:      tenantID,
		interval:      interval,
		client: &http.Client{
			Timeout: 3 * time.Second,
		},
	}
}

// PushOnce collects a single ResourceSnapshot and POSTs it to the gateway.
func (p *TelemetryPublisher) PushOnce(ctx context.Context) (ResourceSnapshot, error) {
	if p == nil || p.agent == nil {
		return ResourceSnapshot{}, fmt.Errorf("telemetry publisher not initialized")
	}
	var snap ResourceSnapshot
	if p.tenantID != "" {
		snap = p.agent.CollectTenantTelemetry(p.tenantID)
	} else {
		snap = p.agent.CollectCellTelemetry()
	}
	body, err := json.Marshal(snap)
	if err != nil {
		return snap, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.gatewayURL+"/internal/v1/telemetry", bytes.NewReader(body))
	if err != nil {
		return snap, err
	}
	req.Header.Set("Content-Type", "application/json")
	if p.internalToken != "" {
		req.Header.Set("X-Air-Internal-Token", p.internalToken)
	}
	resp, err := p.client.Do(req)
	if err != nil {
		return snap, err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return snap, fmt.Errorf("gateway telemetry push returned HTTP %d", resp.StatusCode)
	}
	return snap, nil
}

// Start runs the periodic telemetry push loop until ctx is cancelled.
func (p *TelemetryPublisher) Start(ctx context.Context) {
	if p == nil || p.gatewayURL == "" {
		return
	}
	go func() {
		ticker := time.NewTicker(p.interval)
		defer ticker.Stop()
		// Push immediately on startup so the AdaptiveController has fresh telemetry.
		if _, err := p.PushOnce(ctx); err != nil && ctx.Err() == nil {
			log.Printf("[cell-telemetry] initial push failed: %v", err)
		}
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if _, err := p.PushOnce(ctx); err != nil && ctx.Err() == nil {
					log.Printf("[cell-telemetry] push failed: %v", err)
				}
			}
		}
	}()
}
