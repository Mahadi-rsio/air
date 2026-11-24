package gateway

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Mahadi-rsio/air/internal/pb"
	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// TenantRoute holds cached routing metadata and backend credentials for a single tenant database.
type TenantRoute struct {
	TenantID    string
	State       string
	Host        string
	Port        int
	SockDir     string
	AppPassword string
}

// TenantDiscovery maintains the live tenant routing table via Control Plane gRPC
// (ListDatabases, WatchTenantChanges stream, GetTenantRoute, WakeTenant, BatchTouchActivity)
// with optional PostgreSQL metadata-DB fallback polling.
type TenantDiscovery struct {
	cfg     Config
	conn    *grpc.ClientConn
	client  pb.AirDBControlClient
	metrics *MetricsRegistry

	running atomic.Bool
	cancel  context.CancelFunc
	wg      sync.WaitGroup

	pgMu   sync.Mutex
	pgConn *pgx.Conn

	cacheMu          sync.RWMutex
	cache            map[string]TenantRoute
	aliases          map[string]BlueGreenAlias
	lastEventVersion atomic.Int64

	cbMu     sync.Mutex
	onUpsert func(TenantRoute)
	onRemove func(string)

	touchMu        sync.Mutex
	lastTouch      map[string]time.Time
	pendingTouches map[string]struct{}
	touchCh        chan struct{}
}

func NewTenantDiscovery(cfg Config) (*TenantDiscovery, error) {
	conn, err := grpc.NewClient(
		cfg.GRPCAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                30 * time.Second,
			Timeout:             5 * time.Second,
			PermitWithoutStream: true,
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create gRPC client for %s: %w", cfg.GRPCAddr, err)
	}
	return &TenantDiscovery{
		cfg:            cfg,
		conn:           conn,
		client:         pb.NewAirDBControlClient(conn),
		cache:          make(map[string]TenantRoute),
		lastTouch:      make(map[string]time.Time),
		pendingTouches: make(map[string]struct{}),
		touchCh:        make(chan struct{}, 1),
	}, nil
}

func (d *TenantDiscovery) SetMetrics(m *MetricsRegistry) {
	d.metrics = m
}

func (d *TenantDiscovery) SetCallbacks(onUpsert func(TenantRoute), onRemove func(string)) {
	d.cbMu.Lock()
	defer d.cbMu.Unlock()
	d.onUpsert = onUpsert
	d.onRemove = onRemove
}

func (d *TenantDiscovery) Start() {
	if d.running.Swap(true) {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	d.cancel = cancel

	// Initial synchronous discovery attempt via gRPC ListDatabases (non-fatal if control plane is booting).
	if !d.syncDatabasesGRPC(ctx) && d.cfg.MetaDSN != "" {
		d.pollMetadataOnce(ctx)
	}

	d.wg.Add(2)
	go func() {
		defer d.wg.Done()
		d.discoveryLoop(ctx)
	}()
	go func() {
		defer d.wg.Done()
		d.touchLoop(ctx)
	}()
}

func (d *TenantDiscovery) Stop() {
	if !d.running.Swap(false) {
		return
	}
	if d.cancel != nil {
		d.cancel()
	}
	select {
	case d.touchCh <- struct{}{}:
	default:
	}
	d.wg.Wait()

	d.pgMu.Lock()
	if d.pgConn != nil {
		_ = d.pgConn.Close(context.Background())
		d.pgConn = nil
	}
	d.pgMu.Unlock()

	if d.conn != nil {
		_ = d.conn.Close()
	}
}

func (d *TenantDiscovery) ListCached() []TenantRoute {
	d.cacheMu.RLock()
	defer d.cacheMu.RUnlock()
	out := make([]TenantRoute, 0, len(d.cache))
	for _, r := range d.cache {
		out = append(out, r)
	}
	return out
}

func (d *TenantDiscovery) withCtx(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(parent, timeout)
	if d.cfg.InternalToken != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-air-internal-token", d.cfg.InternalToken)
	}
	return ctx, cancel
}

func (d *TenantDiscovery) fromProtoRoute(resp *pb.TenantRouteResponse) TenantRoute {
	host := resp.Host
	if d.cfg.BackendHost != "" {
		host = d.cfg.BackendHost
	}
	return TenantRoute{
		TenantID:    resp.TenantId,
		State:       resp.State,
		Host:        host,
		Port:        int(resp.Port),
		SockDir:     resp.SockDir,
		AppPassword: resp.AppPassword,
	}
}

// ResolveTenant resolves a tenant route from cache or Control Plane gRPC, waking SUSPENDED tenants if needed.
func (d *TenantDiscovery) ResolveTenant(tenantID string) (TenantRoute, codes.Code, error) {
	return d.resolveTenant(tenantID, true)
}

// ResolveTenantNoWake resolves a tenant route from cache or Control Plane gRPC without waking SUSPENDED tenants.
func (d *TenantDiscovery) ResolveTenantNoWake(tenantID string) (TenantRoute, codes.Code, error) {
	return d.resolveTenant(tenantID, false)
}

func (d *TenantDiscovery) resolveTenant(tenantID string, wakeIfSuspended bool) (TenantRoute, codes.Code, error) {
	d.cacheMu.RLock()
	targetID := d.resolveAliasLocked(tenantID)
	if r, ok := d.cache[targetID]; ok && r.AppPassword != "" {
		if (r.State == "ACTIVE" && r.Port > 0) || (!wakeIfSuspended && r.State == "SUSPENDED") {
			d.cacheMu.RUnlock()
			return r, codes.OK, nil
		}
	}
	d.cacheMu.RUnlock()

	req := &pb.TenantRequest{TenantId: targetID}
	var resp *pb.TenantRouteResponse
	var err error
	for attempt := 0; attempt < 2; attempt++ {
		ctx, cancel := d.withCtx(context.Background(), 10*time.Second)
		resp, err = d.client.GetTenantRoute(ctx, req)
		cancel()
		if err == nil || status.Code(err) != codes.Unavailable || attempt == 1 {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if err != nil {
		code := status.Code(err)
		if d.metrics != nil {
			d.metrics.RecordRPCError()
		}
		if code == codes.NotFound {
			d.removeCache(tenantID)
		}
		return TenantRoute{}, code, fmt.Errorf("%s", status.Convert(err).Message())
	}

	route := d.fromProtoRoute(resp)
	if route.State == "SUSPENDED" {
		if wakeIfSuspended {
			wroute, werr := d.WakeTenant(tenantID)
			if werr != nil {
				return TenantRoute{}, status.Code(werr), werr
			}
			return wroute, codes.OK, nil
		}
		d.upsertCache(route)
		return route, codes.OK, nil
	}
	d.upsertCache(route)
	return route, codes.OK, nil
}

// WakeTenant explicitly wakes a SUSPENDED tenant via Control Plane gRPC.
func (d *TenantDiscovery) WakeTenant(tenantID string) (TenantRoute, error) {
	ctx, cancel := d.withCtx(context.Background(), 35*time.Second)
	defer cancel()
	resp, err := d.client.WakeTenant(ctx, &pb.TenantRequest{TenantId: tenantID})
	if err != nil {
		if d.metrics != nil {
			d.metrics.RecordRPCError()
		}
		return TenantRoute{}, fmt.Errorf("%s", status.Convert(err).Message())
	}
	route := d.fromProtoRoute(resp)
	if route.State == "" {
		route.State = "ACTIVE"
	}
	d.upsertCache(route)
	return route, nil
}

func grpcToHTTP(c codes.Code) int {
	switch c {
	case codes.NotFound:
		return http.StatusNotFound
	case codes.PermissionDenied:
		return http.StatusForbidden
	case codes.Unauthenticated:
		return http.StatusUnauthorized
	case codes.AlreadyExists, codes.FailedPrecondition:
		return http.StatusConflict
	case codes.InvalidArgument:
		return http.StatusBadRequest
	case codes.OutOfRange:
		return http.StatusUnprocessableEntity
	case codes.Unavailable:
		return http.StatusServiceUnavailable
	default:
		return http.StatusBadGateway
	}
}

func makeErrorJSON(msg string) string {
	return fmt.Sprintf(`{"error":"%s"}`, JSONEscape(msg))
}

func appendDBInfoJSON(b *strings.Builder, d *pb.DatabaseInfo) {
	fmt.Fprintf(b,
		`{"account_id":"%s","tenant_id":"%s","direct_port":%d,"system_identifier":"%s","fencing_epoch":%d,"state":"%s"}`,
		JSONEscape(d.AccountId), JSONEscape(d.TenantId), d.DirectPort,
		JSONEscape(d.SystemIdentifier), d.FencingEpoch, JSONEscape(d.State),
	)
}

func (d *TenantDiscovery) CreateDatabase(tenantID, accountID, idemKey string) (int, string, bool) {
	ctx, cancel := d.withCtx(context.Background(), 60*time.Second)
	defer cancel()
	resp, err := d.client.CreateDatabase(ctx, &pb.CreateDatabaseRequest{
		TenantId:       tenantID,
		AccountId:      accountID,
		IdempotencyKey: idemKey,
	})
	if err != nil {
		st := status.Convert(err)
		return grpcToHTTP(st.Code()), makeErrorJSON(st.Message()), false
	}
	out := fmt.Sprintf(
		`{"tenant_id":"%s","state":"%s","direct_port":%d,"host":"%s","user":"%s","password":"%s"}`,
		JSONEscape(resp.TenantId), JSONEscape(resp.State), resp.DirectPort,
		JSONEscape(resp.Host), JSONEscape(resp.User), JSONEscape(resp.Password),
	)
	_, _, _ = d.ResolveTenant(resp.TenantId)
	return http.StatusCreated, out, true
}

func (d *TenantDiscovery) ListDatabases(accountID string) (int, string, bool) {
	ctx, cancel := d.withCtx(context.Background(), 15*time.Second)
	defer cancel()
	resp, err := d.client.ListDatabases(ctx, &pb.ListDatabasesRequest{AccountId: accountID})
	if err != nil {
		st := status.Convert(err)
		return grpcToHTTP(st.Code()), makeErrorJSON(st.Message()), false
	}
	var b strings.Builder
	b.WriteString(`{"databases":[`)
	for i, db := range resp.Databases {
		if i > 0 {
			b.WriteByte(',')
		}
		appendDBInfoJSON(&b, db)
	}
	b.WriteString(`]}`)
	return http.StatusOK, b.String(), true
}

func (d *TenantDiscovery) GetDatabase(tenantID string) (int, string, bool) {
	ctx, cancel := d.withCtx(context.Background(), 15*time.Second)
	defer cancel()
	resp, err := d.client.GetDatabase(ctx, &pb.TenantRequest{TenantId: tenantID})
	if err != nil {
		st := status.Convert(err)
		return grpcToHTTP(st.Code()), makeErrorJSON(st.Message()), false
	}
	var b strings.Builder
	appendDBInfoJSON(&b, resp)
	return http.StatusOK, b.String(), true
}

func (d *TenantDiscovery) callTenantStatusRPC(
	tenantID string,
	timeout time.Duration,
	newCachedState string,
	rpcFn func(context.Context, *pb.TenantRequest) (*pb.StatusResponse, error),
) (int, string, bool) {
	ctx, cancel := d.withCtx(context.Background(), timeout)
	defer cancel()
	resp, err := rpcFn(ctx, &pb.TenantRequest{TenantId: tenantID})
	if err != nil {
		st := status.Convert(err)
		return grpcToHTTP(st.Code()), makeErrorJSON(st.Message()), false
	}
	if newCachedState != "" {
		d.SetCachedState(tenantID, newCachedState)
	}
	out := fmt.Sprintf(`{"status":"%s","tenant_id":"%s"}`, JSONEscape(resp.Status), JSONEscape(resp.TenantId))
	return http.StatusOK, out, true
}

func (d *TenantDiscovery) DeleteDatabase(tenantID string) (int, string, bool) {
	return d.callTenantStatusRPC(tenantID, 30*time.Second, "", func(ctx context.Context, req *pb.TenantRequest) (*pb.StatusResponse, error) {
		return d.client.DeleteDatabase(ctx, req)
	})
}

func (d *TenantDiscovery) SuspendDatabase(tenantID string) (int, string, bool) {
	return d.callTenantStatusRPC(tenantID, 30*time.Second, "SUSPENDED", func(ctx context.Context, req *pb.TenantRequest) (*pb.StatusResponse, error) {
		return d.client.SuspendDatabase(ctx, req)
	})
}

func (d *TenantDiscovery) StartDatabase(tenantID string) (int, string, bool) {
	ctx, cancel := d.withCtx(context.Background(), 35*time.Second)
	defer cancel()
	resp, err := d.client.StartDatabase(ctx, &pb.TenantRequest{TenantId: tenantID})
	if err != nil {
		st := status.Convert(err)
		return grpcToHTTP(st.Code()), makeErrorJSON(st.Message()), false
	}
	r := d.fromProtoRoute(resp)
	if r.State == "" {
		r.State = "ACTIVE"
	}
	d.upsertCache(r)
	out := fmt.Sprintf(
		`{"tenant_id":"%s","state":"%s","host":"%s","port":%d}`,
		JSONEscape(r.TenantID), JSONEscape(r.State), JSONEscape(r.Host), r.Port,
	)
	return http.StatusOK, out, true
}

func (d *TenantDiscovery) LockDatabase(tenantID string) (int, string, bool) {
	return d.callTenantStatusRPC(tenantID, 30*time.Second, "LOCKED", func(ctx context.Context, req *pb.TenantRequest) (*pb.StatusResponse, error) {
		return d.client.LockDatabase(ctx, req)
	})
}

func (d *TenantDiscovery) UnlockDatabase(tenantID string) (int, string, bool) {
	return d.callTenantStatusRPC(tenantID, 15*time.Second, "SUSPENDED", func(ctx context.Context, req *pb.TenantRequest) (*pb.StatusResponse, error) {
		return d.client.UnlockDatabase(ctx, req)
	})
}

func (d *TenantDiscovery) callJSONRPC(
	timeout time.Duration,
	successCode int,
	rpcFn func(context.Context) (*pb.JsonResponse, error),
) (int, string, bool) {
	ctx, cancel := d.withCtx(context.Background(), timeout)
	defer cancel()
	resp, err := rpcFn(ctx)
	if err != nil {
		st := status.Convert(err)
		return grpcToHTTP(st.Code()), makeErrorJSON(st.Message()), false
	}
	return successCode, resp.GetJson(), true
}

func (d *TenantDiscovery) TriggerBackup(tenantID, backupType string) (int, string, bool) {
	return d.callJSONRPC(120*time.Second, http.StatusCreated, func(ctx context.Context) (*pb.JsonResponse, error) {
		return d.client.TriggerBackup(ctx, &pb.TriggerBackupRequest{
			TenantId: tenantID,
			Type:     backupType,
		})
	})
}

func (d *TenantDiscovery) ListBackups(tenantID string) (int, string, bool) {
	return d.callJSONRPC(15*time.Second, http.StatusOK, func(ctx context.Context) (*pb.JsonResponse, error) {
		return d.client.ListBackups(ctx, &pb.TenantRequest{TenantId: tenantID})
	})
}

func (d *TenantDiscovery) VerifyBackup(tenantID, backupID string, streamHash, deepRestore bool) (int, string, bool) {
	return d.callJSONRPC(120*time.Second, http.StatusOK, func(ctx context.Context) (*pb.JsonResponse, error) {
		return d.client.VerifyBackup(ctx, &pb.VerifyBackupRequest{
			TenantId:    tenantID,
			BackupId:    backupID,
			StreamHash:  streamHash,
			DeepRestore: deepRestore,
		})
	})
}

func (d *TenantDiscovery) RestoreDatabase(tenantID, backupID, targetLSN, targetTime, newTenantID, verifyQuery string) (int, string, bool) {
	return d.callJSONRPC(120*time.Second, http.StatusOK, func(ctx context.Context) (*pb.JsonResponse, error) {
		return d.client.RestoreDatabase(ctx, &pb.RestoreDatabaseRequest{
			TenantId:    tenantID,
			BackupId:    backupID,
			TargetLsn:   targetLSN,
			TargetTime:  targetTime,
			NewTenantId: newTenantID,
			VerifyQuery: verifyQuery,
		})
	})
}

func (d *TenantDiscovery) GetOffloadStatus(tenantID string) (int, string, bool) {
	return d.callJSONRPC(15*time.Second, http.StatusOK, func(ctx context.Context) (*pb.JsonResponse, error) {
		return d.client.GetOffloadStatus(ctx, &pb.TenantRequest{TenantId: tenantID})
	})
}

func (d *TenantDiscovery) RevokeAPIKey(keyID string) (int, string, bool) {
	ctx, cancel := d.withCtx(context.Background(), 15*time.Second)
	defer cancel()
	_, err := d.client.RevokeAPIKey(ctx, &pb.RevokeAPIKeyRequest{KeyId: keyID})
	if err != nil {
		st := status.Convert(err)
		return grpcToHTTP(st.Code()), makeErrorJSON(st.Message()), false
	}
	return http.StatusOK, fmt.Sprintf(`{"status":"revoked","key_id":"%s"}`, JSONEscape(keyID)), true
}

func (d *TenantDiscovery) SetCachedState(tenantID, state string) {
	d.cacheMu.Lock()
	r, found := d.cache[tenantID]
	if found {
		r.State = state
		if state != "ACTIVE" {
			r.Port = 0
		}
		d.cache[tenantID] = r
	}
	d.cacheMu.Unlock()

	if found {
		d.cbMu.Lock()
		cb := d.onUpsert
		d.cbMu.Unlock()
		if cb != nil {
			cb(r)
		}
	}
}

func (d *TenantDiscovery) EvictCache(tenantID string) {
	d.removeCache(tenantID)
}

func (d *TenantDiscovery) UpsertRoute(r TenantRoute) {
	d.upsertCache(r)
}

// TouchActivity debounces and enqueues an activity heartbeat for tenantID (scale-to-zero keepalive).
func (d *TenantDiscovery) TouchActivity(tenantID string) {
	if tenantID == "" || !d.running.Load() {
		return
	}
	now := time.Now()
	d.touchMu.Lock()
	if prev, ok := d.lastTouch[tenantID]; ok && now.Sub(prev) < 5*time.Second {
		d.touchMu.Unlock()
		return
	}
	d.lastTouch[tenantID] = now
	d.pendingTouches[tenantID] = struct{}{}
	d.touchMu.Unlock()

	select {
	case d.touchCh <- struct{}{}:
	default:
	}
}

func (d *TenantDiscovery) touchLoop(ctx context.Context) {
	for {
		d.touchMu.Lock()
		hasPending := len(d.pendingTouches) > 0
		if !hasPending && !d.running.Load() {
			d.touchMu.Unlock()
			return
		}
		if hasPending {
			batch := make([]string, 0, len(d.pendingTouches))
			for tid := range d.pendingTouches {
				batch = append(batch, tid)
			}
			d.pendingTouches = make(map[string]struct{})
			d.touchMu.Unlock()

			rpcCtx, cancel := d.withCtx(ctx, 2*time.Second)
			_, err := d.client.BatchTouchActivity(rpcCtx, &pb.BatchTouchActivityRequest{TenantIds: batch})
			cancel()
			if err != nil && d.running.Load() {
				log.Printf("[air-gateway] BatchTouchActivity failed: %v", err)
			}
			continue
		}
		d.touchMu.Unlock()

		select {
		case <-ctx.Done():
			return
		case <-d.touchCh:
		}
	}
}

func (d *TenantDiscovery) syncDatabasesGRPC(parent context.Context) bool {
	ctx, cancel := d.withCtx(parent, 10*time.Second)
	defer cancel()
	resp, err := d.client.ListDatabases(ctx, &pb.ListDatabasesRequest{})
	if err != nil {
		return false
	}
	discovered := make([]TenantRoute, 0, len(resp.Databases))
	for _, db := range resp.Databases {
		if db.State == "PURGED" {
			continue
		}
		r := TenantRoute{
			TenantID: db.TenantId,
			State:    db.State,
			Port:     int(db.DirectPort),
			Host:     d.cfg.BackendHost,
		}
		discovered = append(discovered, r)
	}
	d.reconcileSnapshot(discovered)
	for {
		cur := d.lastEventVersion.Load()
		if resp.SnapshotVersion <= cur || d.lastEventVersion.CompareAndSwap(cur, resp.SnapshotVersion) {
			break
		}
	}
	return true
}

func (d *TenantDiscovery) watchTenantStream(parent context.Context) {
	ctx := parent
	if d.cfg.InternalToken != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-air-internal-token", d.cfg.InternalToken)
	}
	stream, err := d.client.WatchTenantChanges(ctx, &pb.WatchTenantsRequest{
		AfterVersion: d.lastEventVersion.Load(),
	})
	if err != nil {
		return
	}
	for d.running.Load() {
		ev, err := stream.Recv()
		if err != nil {
			return
		}
		for {
			cur := d.lastEventVersion.Load()
			if ev.Version <= cur || d.lastEventVersion.CompareAndSwap(cur, ev.Version) {
				break
			}
		}
		if ev.Type == pb.TenantEventType_TENANT_EVENT_RESYNC_REQUIRED {
			return
		}
		if ev.Type == pb.TenantEventType_TENANT_EVENT_DELETE && ev.Route != nil {
			d.removeCache(ev.Route.TenantId)
		} else if ev.Type == pb.TenantEventType_TENANT_EVENT_UPSERT && ev.Route != nil {
			d.upsertCache(d.fromProtoRoute(ev.Route))
		}
	}
}

func (d *TenantDiscovery) discoveryLoop(ctx context.Context) {
	backoff := 500 * time.Millisecond
	for d.running.Load() {
		if d.syncDatabasesGRPC(ctx) {
			backoff = 500 * time.Millisecond
			d.watchTenantStream(ctx)
			if !d.running.Load() {
				return
			}
		} else if d.cfg.MetaDSN != "" {
			d.pollMetadataOnce(ctx)
		}
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		backoff *= 2
		if backoff > 5*time.Second {
			backoff = 5 * time.Second
		}
	}
}

func (d *TenantDiscovery) upsertCache(r TenantRoute) {
	d.cacheMu.Lock()
	if cur, ok := d.cache[r.TenantID]; ok && r.AppPassword == "" && cur.AppPassword != "" {
		r.AppPassword = cur.AppPassword
	}
	d.cache[r.TenantID] = r
	d.cacheMu.Unlock()

	d.cbMu.Lock()
	cb := d.onUpsert
	d.cbMu.Unlock()
	if cb != nil {
		cb(r)
	}
}

func (d *TenantDiscovery) removeCache(tenantID string) {
	if tenantID == "" {
		return
	}
	d.cacheMu.Lock()
	delete(d.cache, tenantID)
	d.cacheMu.Unlock()

	d.cbMu.Lock()
	cb := d.onRemove
	d.cbMu.Unlock()
	if cb != nil {
		cb(tenantID)
	}
}

func (d *TenantDiscovery) pollMetadataOnce(parent context.Context) {
	d.pgMu.Lock()
	defer d.pgMu.Unlock()

	ctx, cancel := context.WithTimeout(parent, 3*time.Second)
	defer cancel()

	if d.pgConn == nil || d.pgConn.Ping(ctx) != nil {
		if d.pgConn != nil {
			_ = d.pgConn.Close(context.Background())
			d.pgConn = nil
		}
		conn, err := pgx.Connect(ctx, d.cfg.MetaDSN)
		if err != nil {
			return
		}
		d.pgConn = conn
	}

	rows, err := d.pgConn.Query(ctx, "SELECT tenant_id, state, direct_port FROM clusters")
	if err != nil {
		_ = d.pgConn.Close(context.Background())
		d.pgConn = nil
		return
	}
	defer rows.Close()

	var discovered []TenantRoute
	for rows.Next() {
		var r TenantRoute
		if err := rows.Scan(&r.TenantID, &r.State, &r.Port); err != nil {
			continue
		}
		r.Host = d.cfg.BackendHost
		discovered = append(discovered, r)
	}
	if rows.Err() != nil {
		return
	}
	d.reconcileSnapshot(discovered)
}

func (d *TenantDiscovery) reconcileSnapshot(discovered []TenantRoute) {
	seen := make(map[string]struct{}, len(discovered))
	var changed []TenantRoute
	var removed []string

	d.cacheMu.Lock()
	for _, r := range discovered {
		seen[r.TenantID] = struct{}{}
		cur, ok := d.cache[r.TenantID]
		if !ok {
			d.cache[r.TenantID] = r
			changed = append(changed, r)
			log.Printf("[air-gateway] discovered tenant db=%s state=%s port=%d", r.TenantID, r.State, r.Port)
		} else if cur.State != r.State || cur.Port != r.Port {
			cur.State = r.State
			cur.Port = r.Port
			if r.Host != "" {
				cur.Host = r.Host
			}
			d.cache[r.TenantID] = cur
			changed = append(changed, cur)
		}
	}
	for tid := range d.cache {
		if _, ok := seen[tid]; !ok {
			removed = append(removed, tid)
			delete(d.cache, tid)
			log.Printf("[air-gateway] removed deleted tenant db=%s", tid)
		}
	}
	d.cacheMu.Unlock()

	d.cbMu.Lock()
	upsertCB := d.onUpsert
	removeCB := d.onRemove
	d.cbMu.Unlock()

	if upsertCB != nil {
		for _, r := range changed {
			upsertCB(r)
		}
	}
	if removeCB != nil {
		for _, tid := range removed {
			removeCB(tid)
		}
	}
}
