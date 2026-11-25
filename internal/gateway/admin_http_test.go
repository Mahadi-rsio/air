package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Mahadi-rsio/air/internal/pb"
	"google.golang.org/grpc"
)

type mockAirDBControl struct {
	pb.UnimplementedAirDBControlServer
	created []string
	deleted []string
}

func (m *mockAirDBControl) CreateDatabase(_ context.Context, req *pb.CreateDatabaseRequest) (*pb.CreateDatabaseResponse, error) {
	m.created = append(m.created, req.GetTenantId())
	return &pb.CreateDatabaseResponse{
		TenantId:   req.GetTenantId(),
		State:      "ACTIVE",
		DirectPort: 18001,
		Host:       "127.0.0.1",
		User:       "app_user",
		Password:   "pw",
	}, nil
}

func (m *mockAirDBControl) ListDatabases(_ context.Context, _ *pb.ListDatabasesRequest) (*pb.ListDatabasesResponse, error) {
	return &pb.ListDatabasesResponse{}, nil
}

func (m *mockAirDBControl) DeleteDatabase(_ context.Context, req *pb.TenantRequest) (*pb.StatusResponse, error) {
	m.deleted = append(m.deleted, req.GetTenantId())
	return &pb.StatusResponse{Status: "purged", TenantId: req.GetTenantId()}, nil
}

func (m *mockAirDBControl) GetTenantRoute(_ context.Context, req *pb.TenantRequest) (*pb.TenantRouteResponse, error) {
	return &pb.TenantRouteResponse{
		TenantId:    req.GetTenantId(),
		State:       "ACTIVE",
		Host:        "127.0.0.1",
		Port:        18001,
		AppPassword: "pw",
	}, nil
}

type mockReadEngineSvc struct {
	pb.UnimplementedReadEngineServiceServer
	ensured []string
	deleted []string
	synced  []string
}

func (m *mockReadEngineSvc) Health(_ context.Context, _ *pb.ReadEngineEmpty) (*pb.ReadEngineHealthResponse, error) {
	return &pb.ReadEngineHealthResponse{Status: "ok", Service: "air-read-engine", State: "ACTIVE", PgPort: 5433}, nil
}

func (m *mockReadEngineSvc) EnsureTenant(_ context.Context, req *pb.ReadEngineTenantRequest) (*pb.ReadEngineTenantRouteResponse, error) {
	m.ensured = append(m.ensured, req.GetTenantId())
	return &pb.ReadEngineTenantRouteResponse{TenantId: req.GetTenantId(), Engine: "sqlite", Host: "127.0.0.1", PgPort: 5433, State: "ACTIVE"}, nil
}

func (m *mockReadEngineSvc) GetTenantRoute(_ context.Context, req *pb.ReadEngineTenantRequest) (*pb.ReadEngineTenantRouteResponse, error) {
	return &pb.ReadEngineTenantRouteResponse{TenantId: req.GetTenantId(), Engine: "sqlite", Host: "127.0.0.1", PgPort: 5433, State: "ACTIVE"}, nil
}

func (m *mockReadEngineSvc) DeleteTenant(_ context.Context, req *pb.ReadEngineTenantRequest) (*pb.ReadEngineStatusResponse, error) {
	m.deleted = append(m.deleted, req.GetTenantId())
	return &pb.ReadEngineStatusResponse{TenantId: req.GetTenantId(), Status: "deleted"}, nil
}

func (m *mockReadEngineSvc) ListTenants(_ context.Context, _ *pb.ReadEngineEmpty) (*pb.ReadEngineListTenantsResponse, error) {
	return &pb.ReadEngineListTenantsResponse{TenantIds: m.ensured}, nil
}

func (m *mockReadEngineSvc) SyncTenant(_ context.Context, req *pb.SyncTenantRequest) (*pb.ReadEngineStatusResponse, error) {
	m.synced = append(m.synced, req.GetTenantId())
	return &pb.ReadEngineStatusResponse{TenantId: req.GetTenantId(), Status: "synced"}, nil
}

type mockPageserverSvc struct {
	pb.UnimplementedPageserverServiceServer
	ensured   []string
	purged    []string
	flushed   []string
	flushAlls int
}

func (m *mockPageserverSvc) Health(_ context.Context, _ *pb.PageserverEmpty) (*pb.PageserverHealthResponse, error) {
	return &pb.PageserverHealthResponse{Status: "ok", Service: "air-pageserver", ActiveTenants: int32(len(m.ensured))}, nil
}

func (m *mockPageserverSvc) GetTenantInfo(_ context.Context, req *pb.PageserverTenantRequest) (*pb.PageserverTenantInfoResponse, error) {
	for _, id := range m.ensured {
		if id == req.GetTenantId() {
			return &pb.PageserverTenantInfoResponse{TenantId: id, Generation: "gen_1", LastLsn: 100, Exists: true}, nil
		}
	}
	return &pb.PageserverTenantInfoResponse{TenantId: req.GetTenantId(), Exists: false}, nil
}

func (m *mockPageserverSvc) EnsureTenant(_ context.Context, req *pb.EnsureTenantRequest) (*pb.EnsureTenantResponse, error) {
	m.ensured = append(m.ensured, req.GetTenantId())
	return &pb.EnsureTenantResponse{TenantId: req.GetTenantId(), LastLsn: 100, Generation: "gen_1"}, nil
}

func (m *mockPageserverSvc) ListTenants(_ context.Context, _ *pb.PageserverEmpty) (*pb.PageserverListTenantsResponse, error) {
	items := make([]*pb.PageserverTenantInfoResponse, 0, len(m.ensured))
	for _, id := range m.ensured {
		items = append(items, &pb.PageserverTenantInfoResponse{TenantId: id, Generation: "gen_1", LastLsn: 100, Exists: true})
	}
	return &pb.PageserverListTenantsResponse{Tenants: items}, nil
}

func (m *mockPageserverSvc) FlushTenant(_ context.Context, req *pb.PageserverTenantRequest) (*pb.FlushTenantResponse, error) {
	m.flushed = append(m.flushed, req.GetTenantId())
	return &pb.FlushTenantResponse{TenantId: req.GetTenantId(), Flushed: true, EndLsn: 100}, nil
}

func (m *mockPageserverSvc) FlushAll(_ context.Context, _ *pb.PageserverEmpty) (*pb.PageserverStatusResponse, error) {
	m.flushAlls++
	return &pb.PageserverStatusResponse{Status: "flushed"}, nil
}

func (m *mockPageserverSvc) PurgeTenant(_ context.Context, req *pb.PurgeTenantRequest) (*pb.PageserverStatusResponse, error) {
	m.purged = append(m.purged, req.GetTenantId())
	return &pb.PageserverStatusResponse{Status: "purged", TenantId: req.GetTenantId()}, nil
}

func startTestGRPCServer(t *testing.T, register func(*grpc.Server)) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	register(srv)
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(srv.Stop)
	return ln.Addr().String()
}

func doTestHTTP(t *testing.T, method, url, token string, body any) (int, map[string]any) {
	t.Helper()
	var rdr io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, url, rdr)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	if token != "" {
		req.Header.Set("X-Air-Internal-Token", token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var out map[string]any
	_ = json.Unmarshal(raw, &out)
	return resp.StatusCode, out
}

func TestAdminHTTPServer_UnifiedControlPlane(t *testing.T) {
	airMock := &mockAirDBControl{}
	reMock := &mockReadEngineSvc{}
	psMock := &mockPageserverSvc{}

	airAddr := startTestGRPCServer(t, func(s *grpc.Server) { pb.RegisterAirDBControlServer(s, airMock) })
	reAddr := startTestGRPCServer(t, func(s *grpc.Server) { pb.RegisterReadEngineServiceServer(s, reMock) })
	psAddr := startTestGRPCServer(t, func(s *grpc.Server) { pb.RegisterPageserverServiceServer(s, psMock) })

	cfg := DefaultConfig()
	cfg.GRPCAddr = airAddr
	cfg.InternalToken = "tok-unified"
	disc, err := NewTenantDiscovery(cfg)
	if err != nil {
		t.Fatalf("NewTenantDiscovery: %v", err)
	}
	reClient, err := NewReadEngineGRPCClient(reAddr, cfg.InternalToken)
	if err != nil {
		t.Fatalf("NewReadEngineGRPCClient: %v", err)
	}
	defer reClient.Close()
	psClient, err := NewPageserverGRPCClient(psAddr, cfg.InternalToken)
	if err != nil {
		t.Fatalf("NewPageserverGRPCClient: %v", err)
	}
	defer psClient.Close()

	pools := NewPoolManager(4, 60)
	metrics := NewMetricsRegistry(100, 10, 10)
	adm := NewAdminHTTPServer("127.0.0.1:0", cfg.InternalToken, disc, pools, metrics)
	adm.SetNamespaceBackends(reClient, psClient)

	ts := httptest.NewServer(http.HandlerFunc(adm.serveHTTP))
	defer ts.Close()

	// 1. Cluster health aggregates all 4 services
	code, health := doTestHTTP(t, http.MethodGet, ts.URL+"/v1/cluster/health", cfg.InternalToken, nil)
	if code != http.StatusOK {
		t.Fatalf("cluster health status=%d body=%+v", code, health)
	}
	services, _ := health["services"].(map[string]any)
	for _, svcName := range []string{"air-gateway", "air-server", "air-read-engine", "air-pageserver"} {
		if services[svcName] == nil {
			t.Fatalf("missing service %s in cluster health: %+v", svcName, services)
		}
	}

	// 2. Creating DB via gateway orchestrates air-server, read-engine, and pageserver over gRPC
	code, created := doTestHTTP(t, http.MethodPost, ts.URL+"/v1/databases", cfg.InternalToken, map[string]string{"tenant_id": "t_unified"})
	if code != http.StatusCreated || created["tenant_id"] != "t_unified" {
		t.Fatalf("create db failed: code=%d body=%+v", code, created)
	}
	if len(airMock.created) != 1 || len(reMock.ensured) != 1 || len(psMock.ensured) != 1 {
		t.Fatalf("expected all 3 backends called on create: air=%v re=%v ps=%v", airMock.created, reMock.ensured, psMock.ensured)
	}

	// 3. Pageserver and Read Engine admin operations work through gateway
	code, psFlush := doTestHTTP(t, http.MethodPost, ts.URL+"/v1/pageserver/tenants/t_unified/flush", cfg.InternalToken, nil)
	if code != http.StatusOK || psFlush["flushed"] != true {
		t.Fatalf("pageserver flush failed: code=%d body=%+v", code, psFlush)
	}
	code, psAll := doTestHTTP(t, http.MethodPost, ts.URL+"/v1/pageserver/flush", cfg.InternalToken, nil)
	if code != http.StatusOK || psAll["status"] != "flushed" || psMock.flushAlls != 1 {
		t.Fatalf("pageserver flush all failed: code=%d body=%+v", code, psAll)
	}
	code, reSync := doTestHTTP(t, http.MethodPost, ts.URL+"/v1/read-engine/tenants/t_unified/sync", cfg.InternalToken, map[string]string{"sql": "SELECT 1"})
	if code != http.StatusOK || reSync["status"] != "synced" {
		t.Fatalf("read-engine sync failed: code=%d body=%+v", code, reSync)
	}

	// 4. Deleting DB via gateway cascades to air-server, read-engine, and pageserver
	code, _ = doTestHTTP(t, http.MethodDelete, ts.URL+"/v1/databases/t_unified", cfg.InternalToken, nil)
	if code != http.StatusOK {
		t.Fatalf("delete db failed: code=%d", code)
	}
	if len(airMock.deleted) != 1 || len(reMock.deleted) != 1 || len(psMock.purged) != 1 {
		t.Fatalf("expected all 3 backends cleaned up on delete: air=%v re=%v ps=%v", airMock.deleted, reMock.deleted, psMock.purged)
	}
}
