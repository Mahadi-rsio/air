package gateway

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Mahadi-rsio/air/internal/meshtransport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func init() {
	encoding.RegisterCodec(jsonGRPCCodec{})
}

type jsonGRPCCodec struct{}

func (jsonGRPCCodec) Marshal(v any) ([]byte, error)   { return json.Marshal(v) }
func (jsonGRPCCodec) Unmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }
func (jsonGRPCCodec) Name() string                    { return "airjson" }

// ReadinessCheck describes a single subsystem readiness probe.
type ReadinessCheck struct {
	OK     bool   `json:"ok"`
	Detail string `json:"detail,omitempty"`
}

// ReadinessReport summarizes Gateway readiness for traffic admission.
type ReadinessReport struct {
	Ready     bool                      `json:"ready"`
	Status    string                    `json:"status"`
	GatewayID string                    `json:"gateway_id"`
	Cordoned  bool                      `json:"cordoned"`
	Draining  bool                      `json:"draining"`
	Checks    map[string]ReadinessCheck `json:"checks"`
	Metering  *MeteringStatus           `json:"metering,omitempty"`
}

// DrainInstanceResult is the operation-bound acknowledgement returned by DrainInstance.
type DrainInstanceResult struct {
	OpID              string    `json:"op_id"`
	GatewayID         string    `json:"gateway_id"`
	Drained           bool      `json:"drained"`
	MeteringConfirmed bool      `json:"metering_confirmed"`
	ActiveClients     int64     `json:"active_clients"`
	ActiveBackends    int       `json:"active_backends"`
	PendingBatches    int       `json:"pending_batches"`
	QuarantinedCount  int       `json:"quarantined_count"`
	LegacyPending     int       `json:"legacy_pending_count"`
	Blocked           bool      `json:"blocked"`
	Error             string    `json:"error,omitempty"`
	Timestamp         time.Time `json:"timestamp"`
}

// GatewayLifecycle coordinates readiness, cordoning, and graceful instance drain.
type GatewayLifecycle struct {
	gatewayID    string
	requireTLS   bool
	tlsCtx       *TLSContext
	discovery    *TenantDiscovery
	pools        *PoolManager
	metrics      *MetricsRegistry
	metering     *MeteringLogger
	reGRPC       *ReadEngineGRPCClient
	psGRPC       *PageserverGRPCClient
	cordoned     atomic.Bool
	draining     atomic.Bool
	shuttingDown atomic.Bool
}

// NewGatewayLifecycle constructs a lifecycle controller for a Gateway instance.
func NewGatewayLifecycle(
	gatewayID string,
	requireTLS bool,
	tlsCtx *TLSContext,
	discovery *TenantDiscovery,
	pools *PoolManager,
	metrics *MetricsRegistry,
	metering *MeteringLogger,
	reGRPC *ReadEngineGRPCClient,
	psGRPC *PageserverGRPCClient,
) *GatewayLifecycle {
	if gatewayID == "" && metering != nil {
		gatewayID = metering.Status(context.Background()).GatewayID
	}
	return &GatewayLifecycle{
		gatewayID:  gatewayID,
		requireTLS: requireTLS,
		tlsCtx:     tlsCtx,
		discovery:  discovery,
		pools:      pools,
		metrics:    metrics,
		metering:   metering,
		reGRPC:     reGRPC,
		psGRPC:     psGRPC,
	}
}

// Cordon stops new traffic admission on this Gateway without dropping open sessions.
func (g *GatewayLifecycle) Cordon() {
	if g != nil {
		g.cordoned.Store(true)
	}
}

// Uncordon restores traffic admission if the Gateway is healthy and not shutting down.
func (g *GatewayLifecycle) Uncordon(ctx context.Context) error {
	if g == nil {
		return nil
	}
	if g.shuttingDown.Load() {
		return fmt.Errorf("gateway is shutting down")
	}
	if g.metering != nil {
		st := g.metering.Status(ctx)
		if st.Blocked {
			return fmt.Errorf("cannot uncordon: metering is blocked (%s)", st.LastError)
		}
	}
	g.draining.Store(false)
	g.cordoned.Store(false)
	return nil
}

// MarkShuttingDown marks the Gateway as shutting down so readiness fails immediately.
func (g *GatewayLifecycle) MarkShuttingDown() {
	if g != nil {
		g.shuttingDown.Store(true)
		g.cordoned.Store(true)
	}
}

// IsCordoned returns true if the Gateway is cordoned.
func (g *GatewayLifecycle) IsCordoned() bool {
	return g != nil && g.cordoned.Load()
}

// Readiness evaluates TLS, tenant discovery, backend connectivity, metering sink/spool, and cordon state.
func (g *GatewayLifecycle) Readiness(ctx context.Context) ReadinessReport {
	rep := ReadinessReport{
		GatewayID: g.gatewayID,
		Cordoned:  g.cordoned.Load(),
		Draining:  g.draining.Load(),
		Checks:    make(map[string]ReadinessCheck),
	}

	// 1. TLS check
	tlsOK := true
	tlsDetail := "ephemeral/optional"
	if g.requireTLS {
		if g.tlsCtx == nil || g.tlsCtx.cfg == nil || len(g.tlsCtx.cfg.Certificates) == 0 {
			tlsOK = false
			tlsDetail = "TLS required but not initialized"
		} else {
			tlsDetail = "configured"
		}
	}
	rep.Checks["tls"] = ReadinessCheck{OK: tlsOK, Detail: tlsDetail}

	// 2. Tenant discovery check
	discOK := false
	discDetail := "not configured"
	if g.discovery != nil {
		if g.discovery.Synced() {
			discOK = true
			discDetail = "synced"
		} else {
			discDetail = "initial sync pending"
		}
	}
	rep.Checks["discovery"] = ReadinessCheck{OK: discOK, Detail: discDetail}

	// 3. Backend connectivity check
	backOK := true
	backDetail := "ok"
	if g.discovery != nil && !g.discovery.Synced() {
		backOK = false
		backDetail = "control plane discovery not synced"
	}
	if g.reGRPC != nil {
		hctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		if _, err := g.reGRPC.Health(hctx); err != nil {
			backOK = false
			backDetail = fmt.Sprintf("read-engine unhealthy: %v", err)
		}
		cancel()
	}
	rep.Checks["backends"] = ReadinessCheck{OK: backOK, Detail: backDetail}

	// 4. Metering sink and spool check
	metOK := true
	metDetail := "disabled"
	if g.metering != nil {
		st := g.metering.Status(ctx)
		rep.Metering = &st
		if st.Blocked {
			metOK = false
			if st.LastError != "" {
				metDetail = st.LastError
			} else if st.SpoolFull {
				metDetail = "spool full"
			} else if st.QuarantinedCount > 0 {
				metDetail = fmt.Sprintf("%d quarantined batch(es)", st.QuarantinedCount)
			} else if st.LegacyPendingCount > 0 {
				metDetail = fmt.Sprintf("%d legacy pending batch(es)", st.LegacyPendingCount)
			} else if st.BillingEnabled && !st.SinkHealthy {
				metDetail = "metering sink unreachable"
			} else {
				metDetail = "metering blocked"
			}
		} else {
			metDetail = fmt.Sprintf("pending=%d spool_bytes=%d", st.PendingBatches, st.SpoolBytes)
		}
	}
	rep.Checks["metering"] = ReadinessCheck{OK: metOK, Detail: metDetail}

	allOK := tlsOK && discOK && backOK && metOK && !rep.Cordoned && !rep.Draining && !g.shuttingDown.Load()
	rep.Ready = allOK
	switch {
	case g.shuttingDown.Load():
		rep.Status = "shutting_down"
	case rep.Draining:
		rep.Status = "draining"
	case rep.Cordoned:
		rep.Status = "cordoned"
	case allOK:
		rep.Status = "ready"
	default:
		rep.Status = "not_ready"
	}
	return rep
}

// DrainInstance cordons the Gateway, waits for all open client sessions and active
// backends to finish without forcing disconnects, seals and flushes the metering
// spool, and returns an operation-bound acknowledgement.
func (g *GatewayLifecycle) DrainInstance(ctx context.Context, opID string, timeout time.Duration) DrainInstanceResult {
	res := DrainInstanceResult{
		OpID:      opID,
		GatewayID: g.gatewayID,
		Timestamp: time.Now().UTC(),
	}
	if strings.TrimSpace(opID) == "" {
		res.Error = "op_id is required"
		return res
	}
	if timeout <= 0 {
		timeout = 30 * time.Second
	}

	g.cordoned.Store(true)
	g.draining.Store(true)

	activeCounts := func() (int64, int) {
		var clients int64
		var backends int
		if g.metrics != nil {
			clients = g.metrics.ActiveClients()
		}
		if g.pools != nil {
			backends = g.pools.ActiveWriterBackends()
		}
		return clients, backends
	}

	deadline := time.Now().Add(timeout)
	for {
		c, b := activeCounts()
		if c == 0 && b == 0 {
			break
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			res.ActiveClients = c
			res.ActiveBackends = b
			res.Error = fmt.Sprintf("drain timeout with active_clients=%d active_backends=%d", c, b)
			return res
		}
		select {
		case <-ctx.Done():
			c, b = activeCounts()
			res.ActiveClients = c
			res.ActiveBackends = b
			res.Error = ctx.Err().Error()
			return res
		case <-time.After(25 * time.Millisecond):
		}
	}

	if g.metering != nil {
		if err := g.metering.SealAndFlush(ctx); err != nil {
			st := g.metering.Status(ctx)
			res.PendingBatches = st.PendingBatches
			res.QuarantinedCount = st.QuarantinedCount
			res.LegacyPending = st.LegacyPendingCount
			res.Blocked = st.Blocked
			res.Error = fmt.Sprintf("metering seal and flush failed: %v", err)
			return res
		}
		st := g.metering.Status(ctx)
		res.PendingBatches = st.PendingBatches
		res.QuarantinedCount = st.QuarantinedCount
		res.LegacyPending = st.LegacyPendingCount
		res.Blocked = st.Blocked
		if st.PendingBatches > 0 || st.QuarantinedCount > 0 || st.LegacyPendingCount > 0 || st.Blocked {
			res.Error = fmt.Sprintf("metering not clean: pending=%d quarantined=%d legacy=%d blocked=%v",
				st.PendingBatches, st.QuarantinedCount, st.LegacyPendingCount, st.Blocked)
			return res
		}
		if st.BillingEnabled && !st.SinkHealthy {
			res.Error = "metering sink unhealthy after flush"
			return res
		}
	}

	res.Drained = true
	res.MeteringConfirmed = true
	res.Timestamp = time.Now().UTC()
	return res
}

// CancelRouter routes PostgreSQL CancelRequest locally or across peer Gateways
// over the authenticated private mesh.
type CancelRouter struct {
	local         *CancelRegistry
	internalToken string
	peers         []string
	discoveryHost string
	peerPort      int
	mu            sync.Mutex
	conns         map[string]*grpc.ClientConn
}

// NewCancelRouter creates a CancelRouter for cross-Gateway cancellation forwarding.
func NewCancelRouter(local *CancelRegistry, internalToken string, peers []string, discoveryHost string, peerPort int) *CancelRouter {
	if peerPort <= 0 {
		peerPort = 9093
	}
	return &CancelRouter{
		local:         local,
		internalToken: internalToken,
		peers:         append([]string(nil), peers...),
		discoveryHost: strings.TrimSpace(discoveryHost),
		peerPort:      peerPort,
		conns:         make(map[string]*grpc.ClientConn),
	}
}

// SetPeers updates the static peer list dynamically (useful in tests or peer refresh).
func (cr *CancelRouter) SetPeers(peers []string) {
	cr.mu.Lock()
	defer cr.mu.Unlock()
	cr.peers = append([]string(nil), peers...)
}

// CancelLocal validates (gwPID, gwSecret) against this Gateway's CancelRegistry and cancels if found.
func (cr *CancelRouter) CancelLocal(gwPID, gwSecret uint32) bool {
	if cr == nil || cr.local == nil {
		return false
	}
	sessionFound := cr.local.CancelSession(gwPID, gwSecret)
	target, targetFound := cr.local.Lookup(gwPID, gwSecret)
	if targetFound && target.Port > 0 {
		sendBackendCancel(target.Host, target.SockDir, target.Port, target.BackendPID, target.BackendSecret, target.TLS)
		return true
	}
	return sessionFound
}

// ForwardCancel attempts local cancellation first, then forwards to peer Gateways over the private mesh.
func (cr *CancelRouter) ForwardCancel(ctx context.Context, gwPID, gwSecret uint32) bool {
	if cr.CancelLocal(gwPID, gwSecret) {
		return true
	}
	peers := cr.resolvePeers(ctx)
	if len(peers) == 0 {
		return false
	}
	if ctx == nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
	}
	for _, addr := range peers {
		ok, err := cr.sendPeerCancel(ctx, addr, gwPID, gwSecret)
		if err == nil && ok {
			return true
		}
	}
	return false
}

func (cr *CancelRouter) resolvePeers(ctx context.Context) []string {
	cr.mu.Lock()
	out := append([]string(nil), cr.peers...)
	host := cr.discoveryHost
	port := cr.peerPort
	cr.mu.Unlock()

	if host != "" {
		rCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		addrs, err := net.DefaultResolver.LookupHost(rCtx, host)
		cancel()
		if err == nil {
			for _, ip := range addrs {
				out = append(out, net.JoinHostPort(ip, fmt.Sprintf("%d", port)))
			}
		}
	}
	return out
}

func (cr *CancelRouter) getConn(addr string) (*grpc.ClientConn, error) {
	cr.mu.Lock()
	if c, ok := cr.conns[addr]; ok && c != nil {
		cr.mu.Unlock()
		return c, nil
	}
	cr.mu.Unlock()

	dialOpt, err := meshtransport.DialOption(addr)
	if err != nil {
		return nil, err
	}
	conn, err := grpc.NewClient(addr, dialOpt)
	if err != nil {
		return nil, err
	}
	cr.mu.Lock()
	if existing, ok := cr.conns[addr]; ok && existing != nil {
		cr.mu.Unlock()
		_ = conn.Close()
		return existing, nil
	}
	cr.conns[addr] = conn
	cr.mu.Unlock()
	return conn, nil
}

func (cr *CancelRouter) sendPeerCancel(ctx context.Context, addr string, gwPID, gwSecret uint32) (bool, error) {
	conn, err := cr.getConn(addr)
	if err != nil {
		return false, err
	}
	if cr.internalToken != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-air-internal-token", cr.internalToken)
	}
	req := &MeshCancelRequest{PID: gwPID, Secret: gwSecret}
	var resp MeshCancelResponse
	err = conn.Invoke(ctx, "/air.gateway.v1.GatewayMesh/ForwardCancel", req, &resp, grpc.CallContentSubtype("airjson"))
	if err != nil {
		return false, err
	}
	return resp.Cancelled, nil
}

// Close closes all cached peer gRPC connections.
func (cr *CancelRouter) Close() {
	if cr == nil {
		return
	}
	cr.mu.Lock()
	defer cr.mu.Unlock()
	for k, c := range cr.conns {
		if c != nil {
			_ = c.Close()
		}
		delete(cr.conns, k)
	}
}

// MeshCancelRequest is the JSON-encoded gRPC payload for cross-Gateway CancelRequest.
type MeshCancelRequest struct {
	PID    uint32 `json:"pid"`
	Secret uint32 `json:"secret"`
}

// MeshCancelResponse is the JSON-encoded gRPC response for ForwardCancel.
type MeshCancelResponse struct {
	Cancelled bool `json:"cancelled"`
}

// MeshDrainRequest is the JSON-encoded gRPC payload for DrainInstance.
type MeshDrainRequest struct {
	OpID      string `json:"op_id"`
	TimeoutMS int    `json:"timeout_ms"`
}

type gatewayMeshService struct {
	internalToken string
	cancelReg     *CancelRegistry
	lifecycle     *GatewayLifecycle
}

func (s *gatewayMeshService) authorize(ctx context.Context) error {
	if s.internalToken == "" {
		return nil
	}
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return status.Error(codes.PermissionDenied, "unauthorized")
	}
	for _, key := range []string{"x-air-internal-token", "x-internal-token"} {
		for _, v := range md.Get(key) {
			if subtle.ConstantTimeCompare([]byte(strings.TrimSpace(v)), []byte(s.internalToken)) == 1 {
				return nil
			}
		}
	}
	return status.Error(codes.PermissionDenied, "unauthorized")
}

func (s *gatewayMeshService) ForwardCancel(ctx context.Context, req *MeshCancelRequest) (*MeshCancelResponse, error) {
	if err := s.authorize(ctx); err != nil {
		return nil, err
	}
	if req == nil || req.PID == 0 {
		return &MeshCancelResponse{Cancelled: false}, nil
	}
	cr := &CancelRouter{local: s.cancelReg}
	return &MeshCancelResponse{Cancelled: cr.CancelLocal(req.PID, req.Secret)}, nil
}

func (s *gatewayMeshService) GetReadiness(ctx context.Context, _ *struct{}) (*ReadinessReport, error) {
	if err := s.authorize(ctx); err != nil {
		return nil, err
	}
	if s.lifecycle == nil {
		return &ReadinessReport{Ready: false, Status: "uninitialized"}, nil
	}
	rep := s.lifecycle.Readiness(ctx)
	return &rep, nil
}

func (s *gatewayMeshService) DrainInstance(ctx context.Context, req *MeshDrainRequest) (*DrainInstanceResult, error) {
	if err := s.authorize(ctx); err != nil {
		return nil, err
	}
	if s.lifecycle == nil {
		return &DrainInstanceResult{Error: "lifecycle not configured"}, nil
	}
	timeout := 30 * time.Second
	if req != nil && req.TimeoutMS > 0 {
		timeout = time.Duration(req.TimeoutMS) * time.Millisecond
	}
	opID := ""
	if req != nil {
		opID = req.OpID
	}
	res := s.lifecycle.DrainInstance(ctx, opID, timeout)
	return &res, nil
}

func registerGatewayMeshService(srv *grpc.Server, svc *gatewayMeshService) {
	desc := grpc.ServiceDesc{
		ServiceName: "air.gateway.v1.GatewayMesh",
		HandlerType: (*any)(nil),
		Methods: []grpc.MethodDesc{
			{
				MethodName: "ForwardCancel",
				Handler: func(srvImpl any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
					in := new(MeshCancelRequest)
					if err := dec(in); err != nil {
						return nil, err
					}
					return srvImpl.(*gatewayMeshService).ForwardCancel(ctx, in)
				},
			},
			{
				MethodName: "GetReadiness",
				Handler: func(srvImpl any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
					in := new(struct{})
					if err := dec(in); err != nil {
						return nil, err
					}
					return srvImpl.(*gatewayMeshService).GetReadiness(ctx, in)
				},
			},
			{
				MethodName: "DrainInstance",
				Handler: func(srvImpl any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
					in := new(MeshDrainRequest)
					if err := dec(in); err != nil {
						return nil, err
					}
					return srvImpl.(*gatewayMeshService).DrainInstance(ctx, in)
				},
			},
		},
	}
	srv.RegisterService(&desc, svc)
}
