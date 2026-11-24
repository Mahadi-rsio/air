package gateway

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Mahadi-rsio/air/internal/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestConfigParsing(t *testing.T) {
	args := []string{
		"--gateway-addr", "0.0.0.0:6432",
		"--pool-max-backends", "32",
		"--max-client-conns", "256",
		"--max-tenant-client-conns", "64",
		"--max-user-client-conns", "16",
		"--read-engine-port", "15433",
		"--require-tls",
		"--no-pool-mode",
	}
	cfg := LoadConfig(args)
	if cfg.GatewayAddr != "0.0.0.0:6432" {
		t.Fatalf("unexpected GatewayAddr: %s", cfg.GatewayAddr)
	}
	if cfg.PoolMaxBackends != 32 || cfg.MaxClientConns != 256 || cfg.MaxTenantClientConns != 64 || cfg.MaxUserClientConns != 16 {
		t.Fatalf("unexpected pool/conn limits: %+v", cfg)
	}
	if cfg.ReadEnginePort != 15433 || !cfg.RequireTLS || cfg.PoolMode {
		t.Fatalf("unexpected flags: %+v", cfg)
	}
	if cfg.MetaDSN == "" {
		t.Fatal("expected non-empty MetaDSN")
	}
}

func TestTenantIDSNIAuthCancel(t *testing.T) {
	if !ValidateTenantID("tenant_123-abc") {
		t.Fatal("expected valid tenant id")
	}
	if ValidateTenantID("") || ValidateTenantID("bad;drop") {
		t.Fatal("expected invalid tenant id to fail")
	}
	if got := ParseSNITenant("t_demo.air.local"); got != "t_demo" {
		t.Fatalf("expected t_demo, got %q", got)
	}
	if ParseSNITenant("localhost") != "" || ParseSNITenant("127.0.0.1") != "" {
		t.Fatal("expected empty SNI tenant for localhost/IPv4")
	}

	params := map[string]string{"database": "postgres", "user": "app_user"}
	if got := ExtractTenant("t_demo.air.local", params); got != "t_demo" {
		t.Fatalf("expected t_demo, got %q", got)
	}
	params["database"] = "t_explicit"
	if got := ExtractTenant("t_demo.air.local", params); got != "t_explicit" {
		t.Fatalf("expected t_explicit, got %q", got)
	}

	if !ConstantTimeEquals("secret123", "secret123") {
		t.Fatal("expected equal")
	}
	if ConstantTimeEquals("secret123", "secret124") || ConstantTimeEquals("secret123", "secret12") {
		t.Fatal("expected not equal")
	}

	creq := BuildCancelRequest(1234, 5678)
	if len(creq) != 16 ||
		binary.BigEndian.Uint32(creq[0:4]) != 16 ||
		binary.BigEndian.Uint32(creq[4:8]) != CancelRequestCode ||
		binary.BigEndian.Uint32(creq[8:12]) != 1234 ||
		binary.BigEndian.Uint32(creq[12:16]) != 5678 {
		t.Fatalf("malformed cancel request: %x", creq)
	}

	creg := NewCancelRegistry()
	creg.Register(100, 200, CancelTarget{Host: "127.0.0.1", Port: 15432, BackendPID: 9001, BackendSecret: 42})
	ct, ok := creg.Lookup(100, 200)
	if !ok || ct.BackendPID != 9001 || ct.BackendSecret != 42 {
		t.Fatalf("unexpected cancel target: %+v ok=%v", ct, ok)
	}
	creg.Unregister(100, 200)
	if _, ok := creg.Lookup(100, 200); ok {
		t.Fatal("expected cancel target to be unregistered")
	}
}

func TestExtendedQueryRewriting(t *testing.T) {
	var parseBody []byte
	parseBody = appendCStr(parseBody, "stmt1")
	parseBody = appendCStr(parseBody, "SELECT $1::int")
	parseBody = appendBE16(parseBody, 1)
	parseBody = appendBE32(parseBody, 23)

	rewritten, cname, query, oids := RewriteParseBody(parseBody, "_air_s_1_1")
	if cname != "stmt1" || query != "SELECT $1::int" || len(oids) != 1 || oids[0] != 23 {
		t.Fatalf("unexpected parse rewrite: cname=%q query=%q oids=%v", cname, query, oids)
	}
	off := 0
	if got := ReadCStr(rewritten, &off); got != "_air_s_1_1" {
		t.Fatalf("expected _air_s_1_1, got %q", got)
	}

	stmts := map[string]PreparedDef{
		"stmt1": {ClientName: "stmt1", GlobalName: "_air_s_1_1", Query: query, ParamOIDs: oids},
	}

	var bindBody []byte
	bindBody = appendCStr(bindBody, "")
	bindBody = appendCStr(bindBody, "stmt1")
	bindBody = appendBE16(bindBody, 0)
	rbind := RewriteBindBody(bindBody, stmts)
	off = 0
	if ReadCStr(rbind, &off) != "" || ReadCStr(rbind, &off) != "_air_s_1_1" {
		t.Fatal("unexpected bind rewrite")
	}

	closeBody := append([]byte{'S'}, appendCStr(nil, "stmt1")...)
	rclose, erased := RewriteNamedOpBody(closeBody, stmts, true)
	if erased != "stmt1" || rclose[0] != 'S' {
		t.Fatalf("unexpected close rewrite: erased=%q", erased)
	}
	off = 1
	if ReadCStr(rclose, &off) != "_air_s_1_1" {
		t.Fatal("expected _air_s_1_1 in close rewrite")
	}
}

func TestSQLRegexes(t *testing.T) {
	if sm := reSetStmt.FindStringSubmatch("SET search_path = public, app;"); sm == nil || sm[1] != "search_path" {
		t.Fatalf("reSetStmt failed: %v", sm)
	}
	if sm := rePrepareStmt.FindStringSubmatch("PREPARE p1 (int) AS SELECT $1 + 1;"); sm == nil || sm[1] != "p1" {
		t.Fatalf("rePrepareStmt failed: %v", sm)
	}
	if sm := reExecuteStmt.FindStringSubmatch("EXECUTE p1(41);"); sm == nil || sm[2] != "p1" {
		t.Fatalf("reExecuteStmt failed: %v", sm)
	}
	if !IsSessionPinningSQL("LISTEN my_chan;") ||
		!IsSessionPinningSQL("CREATE TEMP TABLE t (x int);") ||
		!IsSessionPinningSQL("SELECT pg_advisory_lock(42);") ||
		IsSessionPinningSQL("SELECT 1;") {
		t.Fatal("IsSessionPinningSQL mismatch")
	}
	if sm := reResetStmt.FindStringSubmatch("RESET work_mem;"); sm == nil || sm[1] != "work_mem" {
		t.Fatalf("reResetStmt failed: %v", sm)
	}
	if sm := reResetStmt.FindStringSubmatch("RESET ALL;"); sm == nil || sm[1] != "ALL" {
		t.Fatalf("reResetStmt ALL failed: %v", sm)
	}
}

func TestPoolManagerDrainAndTLS(t *testing.T) {
	pm := NewPoolManager(8, 300)
	r := TenantRoute{TenantID: "t_demo", State: "ACTIVE", Host: "127.0.0.1", Port: 15444, AppPassword: "pw"}
	pool := pm.GetOrCreate(r, nil)
	if pool == nil {
		t.Fatal("expected non-nil pool")
	}
	if err := pm.RequestDrain("t_demo", 100*time.Millisecond); err != nil {
		t.Fatalf("RequestDrain failed: %v", err)
	}

	var tlsCtx TLSContext
	if err := tlsCtx.Init("", ""); err != nil {
		t.Fatalf("TLSContext.Init failed: %v", err)
	}
}

func TestMetricsLimitsAndExporters(t *testing.T) {
	reg := NewMetricsRegistry(3, 2, 1)

	if !reg.TryAcquireGlobal() {
		t.Fatal("1st global should succeed")
	}
	if err := reg.TryAcquireTenantUser("t_a", "alice"); err != nil {
		t.Fatalf("alice 1st conn failed: %v", err)
	}

	if !reg.TryAcquireGlobal() {
		t.Fatal("2nd global should succeed")
	}
	if err := reg.TryAcquireTenantUser("t_a", "alice"); err == nil || !strings.Contains(err.Error(), `user "alice"`) {
		t.Fatalf("expected user limit error, got %v", err)
	}

	if err := reg.TryAcquireTenantUser("t_a", "bob"); err != nil {
		t.Fatalf("bob conn failed: %v", err)
	}

	if !reg.TryAcquireGlobal() {
		t.Fatal("3rd global should succeed")
	}
	if err := reg.TryAcquireTenantUser("t_a", "charlie"); err == nil || !strings.Contains(err.Error(), `tenant "t_a"`) {
		t.Fatalf("expected tenant limit error, got %v", err)
	}

	if reg.TryAcquireGlobal() {
		t.Fatal("4th global should fail")
	}

	reg.SetTenantClientLimit("t_a", 5)
	if reg.GetTenantClientLimit("t_a") != 5 {
		t.Fatal("expected updated tenant client limit 5")
	}
	if err := reg.TryAcquireTenantUser("t_a", "charlie"); err != nil {
		t.Fatalf("charlie after limit increase failed: %v", err)
	}

	reg.RecordQuery("t_a", "alice", 1200, 150, 64, 512, 10, 400, false, 1)
	reg.RecordQuery("t_a", "alice", 4800, 50, 128, 1024, 20, 800, true, 1)
	reg.RecordQuery("t_a", "bob", 600, 0, 32, 128, 1, 64, false, 1)

	pm := NewPoolManager(8, 300)
	pm.GetOrCreate(TenantRoute{TenantID: "t_a", State: "ACTIVE", Host: "127.0.0.1", Port: 15444, AppPassword: "pw"}, nil)
	pm.SetTenantMaxBackends("t_a", 24)
	snaps := pm.Snapshots()
	if len(snaps) != 1 || snaps[0].MaxBackends != 24 {
		t.Fatalf("unexpected snapshots: %+v", snaps)
	}

	js := reg.ToJSON(snaps, "", "")
	for _, want := range []string{`"query_count":3`, `"error_count":1`, `"bytes_in":224`, `"bytes_out":1664`, `"rows_returned":31`, `"rejected_connections":3`} {
		if !strings.Contains(js, want) {
			t.Fatalf("JSON missing %s: %s", want, js)
		}
	}

	prom := reg.ToPrometheus(snaps)
	if !strings.Contains(prom, `air_gateway_queries_total{tenant="t_a",user="alice"} 2`) ||
		!strings.Contains(prom, `air_gateway_bytes_out_total{tenant="t_a",user="bob"} 128`) {
		t.Fatalf("Prometheus missing expected metrics: %s", prom)
	}

	reg.ReleaseTenantUser("t_a", "alice")
	reg.ReleaseTenantUser("t_a", "bob")
	reg.ReleaseTenantUser("t_a", "charlie")
	reg.ReleaseGlobal()
	reg.ReleaseGlobal()
	reg.ReleaseGlobal()
}

type mockControlService struct {
	pb.UnimplementedAirDBControlServer
	getRouteCalls atomic.Int32
	touchMu       sync.Mutex
	batchTouched  []string
}

func (m *mockControlService) ListDatabases(ctx context.Context, req *pb.ListDatabasesRequest) (*pb.ListDatabasesResponse, error) {
	return &pb.ListDatabasesResponse{
		Databases: []*pb.DatabaseInfo{
			{TenantId: "t_init", State: "ACTIVE", DirectPort: 15433},
		},
		SnapshotVersion: 10,
	}, nil
}

func (m *mockControlService) WatchTenantChanges(req *pb.WatchTenantsRequest, stream pb.AirDBControl_WatchTenantChangesServer) error {
	if req.AfterVersion != 10 {
		return status.Errorf(codes.InvalidArgument, "unexpected after_version: %d", req.AfterVersion)
	}
	if err := stream.Send(&pb.TenantChangeEvent{
		Version: 11,
		Type:    pb.TenantEventType_TENANT_EVENT_UPSERT,
		Route: &pb.TenantRouteResponse{
			TenantId: "t_stream",
			State:    "ACTIVE",
			Host:     "127.0.0.1",
			Port:     15434,
		},
	}); err != nil {
		return err
	}
	if err := stream.Send(&pb.TenantChangeEvent{
		Version: 12,
		Type:    pb.TenantEventType_TENANT_EVENT_DELETE,
		Route:   &pb.TenantRouteResponse{TenantId: "t_init"},
	}); err != nil {
		return err
	}
	<-stream.Context().Done()
	return nil
}

func (m *mockControlService) GetTenantRoute(ctx context.Context, req *pb.TenantRequest) (*pb.TenantRouteResponse, error) {
	m.getRouteCalls.Add(1)
	return &pb.TenantRouteResponse{
		TenantId:    req.TenantId,
		State:       "ACTIVE",
		Host:        "127.0.0.1",
		Port:        15434,
		AppPassword: "secret_pw",
	}, nil
}

func (m *mockControlService) BatchTouchActivity(ctx context.Context, req *pb.BatchTouchActivityRequest) (*pb.Empty, error) {
	m.touchMu.Lock()
	m.batchTouched = append(m.batchTouched, req.TenantIds...)
	m.touchMu.Unlock()
	return &pb.Empty{}, nil
}

func (m *mockControlService) TriggerBackup(_ context.Context, req *pb.TriggerBackupRequest) (*pb.JsonResponse, error) {
	return &pb.JsonResponse{Json: fmt.Sprintf(`{"backup_id":"bkp_1","tenant_id":"%s","type":"%s"}`, req.TenantId, req.Type)}, nil
}

func (m *mockControlService) ListBackups(_ context.Context, req *pb.TenantRequest) (*pb.JsonResponse, error) {
	return &pb.JsonResponse{Json: fmt.Sprintf(`{"backups":[{"backup_id":"bkp_1","tenant_id":"%s"}]}`, req.TenantId)}, nil
}

func (m *mockControlService) VerifyBackup(_ context.Context, req *pb.VerifyBackupRequest) (*pb.JsonResponse, error) {
	return &pb.JsonResponse{Json: fmt.Sprintf(`{"backup_id":"%s","valid":true,"stream_hash":%v,"deep_restore":%v}`, req.BackupId, req.StreamHash, req.DeepRestore)}, nil
}

func (m *mockControlService) RestoreDatabase(_ context.Context, req *pb.RestoreDatabaseRequest) (*pb.JsonResponse, error) {
	return &pb.JsonResponse{Json: fmt.Sprintf(`{"tenant_id":"%s","new_tenant_id":"%s","status":"restored"}`, req.TenantId, req.NewTenantId)}, nil
}

func (m *mockControlService) GetOffloadStatus(_ context.Context, req *pb.TenantRequest) (*pb.JsonResponse, error) {
	return &pb.JsonResponse{Json: fmt.Sprintf(`{"tenant_id":"%s","state":"COMPLETED"}`, req.TenantId)}, nil
}

func (m *mockControlService) RevokeAPIKey(_ context.Context, _ *pb.RevokeAPIKeyRequest) (*pb.StatusResponse, error) {
	return &pb.StatusResponse{Status: "revoked"}, nil
}

func TestTenantDiscoveryGRPC(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	svc := &mockControlService{}
	srv := grpc.NewServer()
	pb.RegisterAirDBControlServer(srv, svc)
	go func() { _ = srv.Serve(ln) }()
	defer srv.Stop()

	cfg := DefaultConfig()
	cfg.GRPCAddr = ln.Addr().String()
	cfg.MetaDSN = ""
	cfg.BackendHost = "127.0.0.1"

	disc, err := NewTenantDiscovery(cfg)
	if err != nil {
		t.Fatal(err)
	}
	disc.Start()
	defer disc.Stop()

	deadline := time.Now().Add(3 * time.Second)
	streamApplied := false
	for time.Now().Before(deadline) {
		cached := disc.ListCached()
		if len(cached) == 1 && cached[0].TenantID == "t_stream" && cached[0].Port == 15434 {
			streamApplied = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !streamApplied {
		t.Fatalf("WatchTenantChanges did not apply upsert and delete; cached=%+v", disc.ListCached())
	}

	r1, _, err := disc.ResolveTenant("t_stream")
	if err != nil || r1.AppPassword != "secret_pw" {
		t.Fatalf("first ResolveTenant failed: route=%+v err=%v", r1, err)
	}
	r2, _, err := disc.ResolveTenant("t_stream")
	if err != nil || r2.AppPassword != "secret_pw" {
		t.Fatalf("second ResolveTenant failed: route=%+v err=%v", r2, err)
	}
	if svc.getRouteCalls.Load() != 1 {
		t.Fatalf("expected 1 GetTenantRoute RPC, got %d", svc.getRouteCalls.Load())
	}

	disc.TouchActivity("t_stream")
	disc.TouchActivity("t_other")
	touchedOK := false
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		svc.touchMu.Lock()
		n := len(svc.batchTouched)
		svc.touchMu.Unlock()
		if n >= 2 {
			touchedOK = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !touchedOK {
		t.Fatal("BatchTouchActivity did not deliver batched tenant_ids")
	}
}

func TestAdminGRPCAndHTTP(t *testing.T) {
	pools := NewPoolManager(8, 300)
	pools.UpsertDiscovered(TenantRoute{TenantID: "t_admin", Host: "127.0.0.1", Port: 15432, AppPassword: "pw", State: "ACTIVE"})
	metrics := NewMetricsRegistry(100, 10, 5)

	adminGRPC := NewAdminGRPCServer("127.0.0.1:0", "adm-tok", pools, metrics)
	port, err := adminGRPC.Start()
	if err != nil || port <= 0 {
		t.Fatalf("AdminGRPCServer.Start failed: port=%d err=%v", port, err)
	}
	defer adminGRPC.Stop()

	conn, err := grpc.NewClient(fmt.Sprintf("127.0.0.1:%d", port), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	stub := pb.NewGatewayAdminClient(conn)

	// Unauthorized call fails with PermissionDenied
	_, err = stub.DrainTenant(context.Background(), &pb.DrainTenantRequest{TenantId: "t_admin", TimeoutMs: 500})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied, got %v", err)
	}

	// Authorized DrainTenant succeeds
	authCtx := metadata.AppendToOutgoingContext(context.Background(), "x-internal-token", "adm-tok")
	resp, err := stub.DrainTenant(authCtx, &pb.DrainTenantRequest{TenantId: "t_admin", TimeoutMs: 500})
	if err != nil || resp.Status != "drained" || resp.TenantId != "t_admin" {
		t.Fatalf("DrainTenant failed: resp=%+v err=%v", resp, err)
	}

	// Authorized SetTenantLimits updates metrics
	resp, err = stub.SetTenantLimits(authCtx, &pb.SetTenantLimitsRequest{TenantId: "t_admin", MaxClientConns: 42, MaxBackends: 6})
	if err != nil || resp.Status != "updated" {
		t.Fatalf("SetTenantLimits failed: resp=%+v err=%v", resp, err)
	}
	if metrics.GetTenantClientLimit("t_admin") != 42 {
		t.Fatalf("expected limit 42, got %d", metrics.GetTenantClientLimit("t_admin"))
	}

	// AdminHTTPServer test
	cfg := DefaultConfig()
	cfg.GRPCAddr = "127.0.0.1:1"
	disc, err := NewTenantDiscovery(cfg)
	if err != nil {
		t.Fatal(err)
	}
	disc.UpsertRoute(TenantRoute{TenantID: "t_admin", State: "ACTIVE", Port: 15432})
	httpSrv := NewAdminHTTPServer("127.0.0.1:0", "adm-tok", disc, pools, metrics)
	if err := httpSrv.Start(); err != nil {
		t.Fatal(err)
	}
	defer httpSrv.Stop()

	res, err := http.Get("http://" + httpSrv.Addr() + "/healthz")
	if err != nil || res.StatusCode != 200 {
		t.Fatalf("healthz failed: res=%v err=%v", res, err)
	}
	res.Body.Close()

	res, err = http.Get("http://" + httpSrv.Addr() + "/internal/v1/tenants")
	if err != nil || res.StatusCode != 401 {
		t.Fatalf("expected 401 without token, got %v err=%v", res, err)
	}
	res.Body.Close()

	req, _ := http.NewRequest(http.MethodGet, "http://"+httpSrv.Addr()+"/internal/v1/tenants", nil)
	req.Header.Set("X-Air-Internal-Token", "adm-tok")
	res, err = http.DefaultClient.Do(req)
	if err != nil || res.StatusCode != 200 {
		t.Fatalf("tenants with token failed: res=%v err=%v", res, err)
	}
	bodyBytes, _ := io.ReadAll(res.Body)
	res.Body.Close()
	if !strings.Contains(string(bodyBytes), "t_admin") {
		t.Fatalf("expected t_admin in tenants response: %s", string(bodyBytes))
	}
}

func TestProtocolRegressions(t *testing.T) {
	// 9a. Fail-closed when expected_pass is empty
	{
		srv, cli := net.Pipe()
		pool := NewTenantPool("t_auth", "127.0.0.1", "", 1, "", 4, 300)
		reg := NewMetricsRegistry(10, 5, 5)
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			pool.ServeClient(srv, "app_user", "", nil, reg, nil, 0)
			_ = srv.Close()
		}()
		mtype, _, err := ReadPGPacket(cli)
		if err != nil || mtype != 'E' {
			t.Fatalf("expected ErrorResponse 'E', got %c err=%v", mtype, err)
		}
		_ = cli.Close()
		wg.Wait()
		if !strings.Contains(reg.ToJSON(nil, "", ""), `"auth_failures":1`) {
			t.Fatal("expected auth_failures:1")
		}
	}

	// 9b. Wrong password rejected
	{
		srv, cli := net.Pipe()
		pool := NewTenantPool("t_auth", "127.0.0.1", "", 1, "correct_pw", 4, 300)
		reg := NewMetricsRegistry(10, 5, 5)
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			pool.ServeClient(srv, "app_user", "correct_pw", nil, reg, nil, 0)
			_ = srv.Close()
		}()
		mtype, mbody, err := ReadPGPacket(cli)
		if err != nil || mtype != 'R' || binary.BigEndian.Uint32(mbody[:4]) != 3 {
			t.Fatalf("expected cleartext auth request, got %c err=%v", mtype, err)
		}
		if _, err := cli.Write(BuildPasswordMessage("wrong_pw")); err != nil {
			t.Fatal(err)
		}
		mtype, _, err = ReadPGPacket(cli)
		if err != nil || mtype != 'E' {
			t.Fatalf("expected ErrorResponse 'E', got %c err=%v", mtype, err)
		}
		_ = cli.Close()
		wg.Wait()
		if !strings.Contains(reg.ToJSON(nil, "", ""), `"auth_failures":1`) {
			t.Fatal("expected auth_failures:1")
		}
	}

	// 9c. Full mock PG backend protocol test: Flush ('H'), COPY FROM STDIN, and unsupported message rejection
	{
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		pgPort := ln.Addr().(*net.TCPAddr).Port

		backendErr := make(chan error, 1)
		go func() {
			defer ln.Close()
			bs, err := ln.Accept()
			if err != nil {
				backendErr <- err
				return
			}
			defer bs.Close()

			var hdr [8]byte
			if _, err := io.ReadFull(bs, hdr[:]); err != nil {
				backendErr <- err
				return
			}
			mlen := binary.BigEndian.Uint32(hdr[0:4])
			sbody := make([]byte, mlen-8)
			if _, err := io.ReadFull(bs, sbody); err != nil {
				backendErr <- err
				return
			}
			_, _ = bs.Write(BuildAuthOK())
			_, _ = bs.Write(BuildBackendKeyData(4321, 8765))
			_, _ = bs.Write(BuildReadyForQuery('I'))

			// 1) Parse + Describe + Flush
			for _, want := range []byte{'P', 'D', 'H'} {
				mt, _, err := ReadPGPacket(bs)
				if err != nil || mt != want {
					backendErr <- fmt.Errorf("expected %c, got %c err=%v", want, mt, err)
					return
				}
			}
			_, _ = bs.Write(BuildPGMessage('1', nil))
			_, _ = bs.Write(BuildPGMessage('t', []byte{0, 0}))
			_, _ = bs.Write(BuildPGMessage('n', nil))

			// 2) Sync
			if mt, _, err := ReadPGPacket(bs); err != nil || mt != 'S' {
				backendErr <- fmt.Errorf("expected S, got %c err=%v", mt, err)
				return
			}
			_, _ = bs.Write(BuildReadyForQuery('I'))

			// 2b) Pooler liveness ping Q("") on re-acquire
			if mt, _, err := ReadPGPacket(bs); err != nil || mt != 'Q' {
				backendErr <- fmt.Errorf("expected Q ping, got %c err=%v", mt, err)
				return
			}
			_, _ = bs.Write(BuildPGMessage('I', nil))
			_, _ = bs.Write(BuildReadyForQuery('I'))

			// 3) SimpleQuery COPY FROM STDIN
			if mt, _, err := ReadPGPacket(bs); err != nil || mt != 'Q' {
				backendErr <- fmt.Errorf("expected Q COPY, got %c err=%v", mt, err)
				return
			}
			_, _ = bs.Write(BuildPGMessage('G', []byte{0, 0, 1, 0, 0}))
			if mt, _, err := ReadPGPacket(bs); err != nil || mt != 'd' {
				backendErr <- fmt.Errorf("expected d, got %c err=%v", mt, err)
				return
			}
			if mt, _, err := ReadPGPacket(bs); err != nil || mt != 'c' {
				backendErr <- fmt.Errorf("expected c, got %c err=%v", mt, err)
				return
			}
			_, _ = bs.Write(BuildPGMessage('C', []byte("COPY 1\x00")))
			_, _ = bs.Write(BuildReadyForQuery('I'))
			backendErr <- nil
		}()

		srv, cli := net.Pipe()
		pool := NewTenantPool("t_proto", "127.0.0.1", "", pgPort, "pw123", 4, 300)
		reg := NewMetricsRegistry(10, 5, 5)
		creg := NewCancelRegistry()

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			pool.ServeClient(srv, "app_user", "pw123", nil, reg, creg, 0)
			_ = srv.Close()
		}()

		mt, _, err := ReadPGPacket(cli)
		if err != nil || mt != 'R' {
			t.Fatalf("expected auth req, got %c err=%v", mt, err)
		}
		if _, err := cli.Write(BuildPasswordMessage("pw123")); err != nil {
			t.Fatal(err)
		}
		for {
			mt, _, err = ReadPGPacket(cli)
			if err != nil {
				t.Fatal(err)
			}
			if mt == 'Z' {
				break
			}
		}

		// Unsupported message type '?'
		_, _ = cli.Write(BuildPGMessage('?', []byte{1, 2}))
		if mt, _, err := ReadPGPacket(cli); err != nil || mt != 'E' {
			t.Fatalf("expected E, got %c err=%v", mt, err)
		}
		if mt, _, err := ReadPGPacket(cli); err != nil || mt != 'Z' {
			t.Fatalf("expected Z, got %c err=%v", mt, err)
		}

		// Parse + Describe + Flush ('H')
		_, _ = cli.Write(BuildParseMessage("s1", "SELECT 1", nil))
		_, _ = cli.Write(BuildPGMessage('D', append([]byte{'S'}, appendCStr(nil, "s1")...)))
		_, _ = cli.Write(BuildPGMessage('H', nil))

		for _, want := range []byte{'1', 't', 'n'} {
			if mt, _, err := ReadPGPacket(cli); err != nil || mt != want {
				t.Fatalf("expected %c, got %c err=%v", want, mt, err)
			}
		}

		// Sync
		_, _ = cli.Write(BuildSyncMessage())
		if mt, _, err := ReadPGPacket(cli); err != nil || mt != 'Z' {
			t.Fatalf("expected Z, got %c err=%v", mt, err)
		}

		// COPY FROM STDIN
		_, _ = cli.Write(BuildSimpleQuery("COPY t FROM STDIN"))
		if mt, _, err := ReadPGPacket(cli); err != nil || mt != 'G' {
			t.Fatalf("expected G, got %c err=%v", mt, err)
		}
		_, _ = cli.Write(BuildPGMessage('d', []byte("42\n")))
		_, _ = cli.Write(BuildPGMessage('c', nil))
		if mt, _, err := ReadPGPacket(cli); err != nil || mt != 'C' {
			t.Fatalf("expected C, got %c err=%v", mt, err)
		}
		if mt, _, err := ReadPGPacket(cli); err != nil || mt != 'Z' {
			t.Fatalf("expected Z, got %c err=%v", mt, err)
		}

		_, _ = cli.Write(BuildPGMessage('X', nil))
		_ = cli.Close()
		wg.Wait()
		if err := <-backendErr; err != nil {
			t.Fatalf("mock backend failed: %v", err)
		}
		if !strings.Contains(reg.ToJSON(nil, "", ""), `"protocol_errors":1`) {
			t.Fatal("expected protocol_errors:1")
		}
	}
}

func TestSQLRouterAndFallback(t *testing.T) {
	if ClassifySQLForRouting("SELECT 1").Target != RouteTargetReadEngine {
		t.Fatal("SELECT -> ReadEngine")
	}
	if ClassifySQLForRouting("/* INSERT INTO t */ -- DELETE FROM t\nSELECT 'UPDATE x', $$DELETE y$$ FROM items").Target != RouteTargetReadEngine {
		t.Fatal("comments/literals ignored -> ReadEngine")
	}
	if ClassifySQLForRouting("WITH cte AS (SELECT id FROM items) SELECT * FROM cte").Target != RouteTargetReadEngine {
		t.Fatal("read CTE -> ReadEngine")
	}
	if ClassifySQLForRouting("EXPLAIN ANALYZE SELECT count(*) FROM items").Target != RouteTargetReadEngine {
		t.Fatal("EXPLAIN -> ReadEngine")
	}
	if ClassifySQLForRouting("COPY (SELECT * FROM items) TO STDOUT").Target != RouteTargetReadEngine {
		t.Fatal("COPY TO -> ReadEngine")
	}

	writeCases := []string{
		"INSERT INTO items VALUES (1)",
		"UPDATE items SET val = 'x'",
		"DELETE FROM items WHERE id = 1",
		"CREATE TABLE foo (id int)",
		"WITH d AS (DELETE FROM items RETURNING *) SELECT * FROM d",
		"SELECT * FROM items WHERE id = 1 FOR UPDATE",
		"SELECT * INTO new_items FROM items",
		"SELECT nextval('items_id_seq')",
		"COPY items FROM STDIN",
		"SELECT 1; INSERT INTO items VALUES (2);",
	}
	for _, sql := range writeCases {
		if ClassifySQLForRouting(sql).Target != RouteTargetWriteEngine {
			t.Fatalf("expected WriteEngine for %q", sql)
		}
	}

	var rmetrics RoutingMetrics
	healthyCtx := SessionRouteContext{
		ReadEngineHealthy: true,
		SessionWriteLSN:   100,
		ReadEngineLSN:     100,
	}
	if RouteQuery("SELECT * FROM items", healthyCtx, &rmetrics).Target != RouteTargetReadEngine {
		t.Fatal("healthy caught-up -> ReadEngine")
	}

	lagCtx := healthyCtx
	lagCtx.ReadEngineLSN = 90
	lagDec := RouteQuery("SELECT * FROM items", lagCtx, &rmetrics)
	if lagDec.Target != RouteTargetWriteEngine || lagDec.Reason != RouteReasonCausalLagFallback {
		t.Fatalf("expected CausalLagFallback, got %+v", lagDec)
	}

	downCtx := healthyCtx
	downCtx.ReadEngineHealthy = false
	downDec := RouteQuery("SELECT * FROM items", downCtx, &rmetrics)
	if downDec.Target != RouteTargetWriteEngine || downDec.Reason != RouteReasonReadEngineUnavailableFallback {
		t.Fatalf("expected ReadEngineUnavailableFallback, got %+v", downDec)
	}

	txWriteCtx := healthyCtx
	txWriteCtx.InTransaction = true
	txWriteCtx.TxHasWrites = true
	if RouteQuery("SELECT * FROM items", txWriteCtx, &rmetrics).Target != RouteTargetWriteEngine {
		t.Fatal("tx_has_writes -> WriteEngine")
	}

	RecordReadFallback(&rmetrics)
	if rmetrics.ReadRouted.Load() != 1 || rmetrics.WriteRouted.Load() != 3 ||
		rmetrics.FallbacksToWrite.Load() != 3 || rmetrics.ReadErrorsRetried.Load() != 1 {
		t.Fatalf("unexpected routing metrics: read=%d write=%d fallbacks=%d retries=%d",
			rmetrics.ReadRouted.Load(), rmetrics.WriteRouted.Load(),
			rmetrics.FallbacksToWrite.Load(), rmetrics.ReadErrorsRetried.Load())
	}
}

func TestTLSUpgradeHandshake(t *testing.T) {
	var tlsCtx TLSContext
	if err := tlsCtx.Init("", ""); err != nil {
		t.Fatal(err)
	}
	srv, cli := net.Pipe()
	defer srv.Close()
	defer cli.Close()

	errCh := make(chan error, 1)
	sniCh := make(chan string, 1)
	go func() {
		tc, sni, err := tlsCtx.UpgradeConn(srv)
		if err != nil {
			errCh <- err
			return
		}
		sniCh <- sni
		_ = tc.Close()
		errCh <- nil
	}()

	cliTLS := tls.Client(cli, &tls.Config{
		InsecureSkipVerify: true,
		ServerName:         "t_tls.air.local",
	})
	if err := cliTLS.Handshake(); err != nil {
		t.Fatalf("client TLS handshake failed: %v", err)
	}
	_ = cliTLS.Close()

	if err := <-errCh; err != nil {
		t.Fatalf("server TLS upgrade failed: %v", err)
	}
	if sni := <-sniCh; sni != "t_tls.air.local" {
		t.Fatalf("expected SNI t_tls.air.local, got %q", sni)
	}
}

func TestAdminHTTPControlPlaneGRPCProxy(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	svc := &mockControlService{}
	srv := grpc.NewServer()
	pb.RegisterAirDBControlServer(srv, svc)
	go func() { _ = srv.Serve(ln) }()
	defer srv.Stop()

	cfg := DefaultConfig()
	cfg.GRPCAddr = ln.Addr().String()
	cfg.MetaDSN = ""

	disc, err := NewTenantDiscovery(cfg)
	if err != nil {
		t.Fatal(err)
	}
	disc.Start()
	defer disc.Stop()

	metrics := NewMetricsRegistry(100, 10, 10)
	pools := NewPoolManager(4, 30)
	defer pools.CloseAll()

	adminSrv := NewAdminHTTPServer("127.0.0.1:0", "gw-secret", disc, pools, metrics)
	if err := adminSrv.Start(); err != nil {
		t.Fatal(err)
	}
	defer adminSrv.Stop()

	doReq := func(method, path, body string) (int, string) {
		t.Helper()
		var r io.Reader
		if body != "" {
			r = strings.NewReader(body)
		}
		req, err := http.NewRequest(method, "http://"+adminSrv.Addr()+path, r)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer gw-secret")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b)
	}

	if code, body := doReq(http.MethodPost, "/v1/databases/t1/backups", `{"type":"full"}`); code != http.StatusCreated || !strings.Contains(body, `"backup_id":"bkp_1"`) {
		t.Fatalf("POST backups failed: code=%d body=%s", code, body)
	}
	if code, body := doReq(http.MethodGet, "/v1/databases/t1/backups", ""); code != http.StatusOK || !strings.Contains(body, `"bkp_1"`) {
		t.Fatalf("GET backups failed: code=%d body=%s", code, body)
	}
	if code, body := doReq(http.MethodPost, "/v1/databases/t1/backups/bkp_1/verify", `{"stream_hash":true}`); code != http.StatusOK || !strings.Contains(body, `"valid":true`) {
		t.Fatalf("POST verify failed: code=%d body=%s", code, body)
	}
	if code, body := doReq(http.MethodPost, "/v1/databases/t1/restore", `{"backup_id":"bkp_1","new_tenant_id":"t1_restored"}`); code != http.StatusOK || !strings.Contains(body, `"t1_restored"`) {
		t.Fatalf("POST restore failed: code=%d body=%s", code, body)
	}
	if code, body := doReq(http.MethodGet, "/v1/databases/t1/offload", ""); code != http.StatusOK || !strings.Contains(body, `"COMPLETED"`) {
		t.Fatalf("GET offload failed: code=%d body=%s", code, body)
	}
	if code, body := doReq(http.MethodDelete, "/v1/api-keys/key_123", ""); code != http.StatusOK || !strings.Contains(body, `"revoked"`) {
		t.Fatalf("DELETE api-keys failed: code=%d body=%s", code, body)
	}
}
