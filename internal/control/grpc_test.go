package control

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/Mahadi-rsio/air/internal/cell"
	"github.com/Mahadi-rsio/air/internal/pb"
	"github.com/Mahadi-rsio/air/internal/tenant"
	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestMapGRPCError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want codes.Code
	}{
		{"nil", nil, codes.OK},
		{"not_found", ErrNotFound, codes.NotFound},
		{"pgx_no_rows", fmt.Errorf("query: %w", pgx.ErrNoRows), codes.NotFound},
		{"forbidden", ErrForbidden, codes.PermissionDenied},
		{"unauthorized", ErrUnauthorized, codes.Unauthenticated},
		{"idempotency", ErrIdempotencyConflict, codes.AlreadyExists},
		{"drain_vetoed", cell.ErrDrainVetoed, codes.FailedPrecondition},
		{"deadline", context.DeadlineExceeded, codes.DeadlineExceeded},
		{"canceled", context.Canceled, codes.Canceled},
		{"invalid_tenant", tenant.ValidateID("bad;id"), codes.InvalidArgument},
		{"internal_unknown", errors.New("db connection refused"), codes.Internal},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := mapGRPCError(tc.err)
			if tc.want == codes.OK {
				if got != nil {
					t.Fatalf("expected nil, got %v", got)
				}
				return
			}
			st, ok := status.FromError(got)
			if !ok {
				t.Fatalf("expected gRPC status error, got %v", got)
			}
			if st.Code() != tc.want {
				t.Fatalf("code = %v, want %v (err=%v)", st.Code(), tc.want, got)
			}
		})
	}
}

func TestGRPCAuthUnaryAndStreamContext(t *testing.T) {
	cp := &ControlPlane{internalToken: "secret-token"}

	if err := cp.authorizeGRPCContext(context.Background()); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("expected Unauthenticated without metadata, got %v", err)
	}

	badCtx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-air-internal-token", "wrong"))
	if err := cp.authorizeGRPCContext(badCtx); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("expected Unauthenticated for wrong token, got %v", err)
	}

	goodCtx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-air-internal-token", " secret-token "))
	if err := cp.authorizeGRPCContext(goodCtx); err != nil {
		t.Fatalf("expected nil for valid token, got %v", err)
	}

	_, err := cp.grpcAuthInterceptor(badCtx, &pb.Empty{}, &grpc.UnaryServerInfo{}, func(ctx context.Context, req any) (any, error) {
		t.Fatal("handler should not be called on unauthenticated unary request")
		return nil, nil
	})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("expected Unauthenticated from unary interceptor, got %v", err)
	}

	err = cp.grpcStreamAuthInterceptor(nil, &fakeServerStream{ctx: badCtx}, &grpc.StreamServerInfo{}, func(srv any, ss grpc.ServerStream) error {
		t.Fatal("handler should not be called on unauthenticated stream request")
		return nil
	})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("expected Unauthenticated from stream interceptor, got %v", err)
	}
}

type fakeServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (f *fakeServerStream) Context() context.Context { return f.ctx }

func startTestGRPCClient(tb testing.TB, cp *ControlPlane) (pb.AirDBControlClient, context.Context) {
	tb.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatal(err)
	}
	srv := NewGRPCServer(cp)
	go func() { _ = srv.Serve(lis) }()
	tb.Cleanup(srv.Stop)

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = conn.Close() })
	ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs("x-air-internal-token", cp.internalToken))
	return pb.NewAirDBControlClient(conn), ctx
}

func TestBatchTouchActivity(t *testing.T) {
	cp := &ControlPlane{internalToken: "test-token"}
	client, ctx := startTestGRPCClient(t, cp)

	if _, err := client.BatchTouchActivity(ctx, &pb.BatchTouchActivityRequest{
		TenantIds: []string{"tenant_a", "tenant_b", "tenant_c"},
	}); err != nil {
		t.Fatalf("BatchTouchActivity failed: %v", err)
	}

	cp.activityMu.Lock()
	for _, id := range []string{"tenant_a", "tenant_b", "tenant_c"} {
		if cp.lastActivity[id].IsZero() {
			cp.activityMu.Unlock()
			t.Fatalf("expected lastActivity recorded for %s", id)
		}
	}
	cp.activityMu.Unlock()

	_, err := client.BatchTouchActivity(ctx, &pb.BatchTouchActivityRequest{
		TenantIds: []string{"tenant_a", "bad;id"},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v", err)
	}
}

func TestWatchTenantChangesBacklogAndLive(t *testing.T) {
	cp := &ControlPlane{internalToken: "test-token", backendHost: "127.0.0.1"}
	client, ctx := startTestGRPCClient(t, cp)

	// Emit one event before the stream connects (backlog).
	cp.emitTenantEvent(context.Background(), "tenant_1", tenantEventUpsert, "ACTIVE", 15433, 1)

	streamCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	stream, err := client.WatchTenantChanges(streamCtx, &pb.WatchTenantsRequest{AfterVersion: 0})
	if err != nil {
		t.Fatalf("WatchTenantChanges failed: %v", err)
	}

	ev1, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv backlog event failed: %v", err)
	}
	if ev1.GetVersion() != 1 || ev1.GetType() != pb.TenantEventType_TENANT_EVENT_UPSERT ||
		ev1.GetRoute().GetTenantId() != "tenant_1" || ev1.GetRoute().GetPort() != 15433 {
		t.Fatalf("unexpected backlog event: %+v", ev1)
	}

	// Emit live UPSERT and DELETE events.
	cp.emitTenantEvent(context.Background(), "tenant_1", tenantEventUpsert, "SUSPENDED", 0, 1)
	cp.emitTenantEvent(context.Background(), "tenant_1", tenantEventDelete, "PURGED", 0, 1)

	ev2, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv live upsert failed: %v", err)
	}
	if ev2.GetVersion() != 2 || ev2.GetRoute().GetState() != "SUSPENDED" {
		t.Fatalf("unexpected live upsert event: %+v", ev2)
	}

	ev3, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv live delete failed: %v", err)
	}
	if ev3.GetVersion() != 3 || ev3.GetType() != pb.TenantEventType_TENANT_EVENT_DELETE {
		t.Fatalf("unexpected live delete event: %+v", ev3)
	}
}

func TestWatchTenantChangesResyncRequired(t *testing.T) {
	cp := &ControlPlane{internalToken: "test-token", backendHost: "127.0.0.1"}
	client, ctx := startTestGRPCClient(t, cp)

	// Exceed maxEventHistory (1024) so after_version=1 is pruned.
	for i := 0; i < maxEventHistory+10; i++ {
		cp.emitTenantEvent(context.Background(), "tenant_roll", tenantEventUpsert, "ACTIVE", 15433, 1)
	}

	streamCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	stream, err := client.WatchTenantChanges(streamCtx, &pb.WatchTenantsRequest{AfterVersion: 1})
	if err != nil {
		t.Fatalf("WatchTenantChanges failed: %v", err)
	}
	ev, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv resync event failed: %v", err)
	}
	if ev.GetType() != pb.TenantEventType_TENANT_EVENT_RESYNC_REQUIRED {
		t.Fatalf("expected RESYNC_REQUIRED, got %v", ev.GetType())
	}
}

func BenchmarkGRPCTouchActivityUnary(b *testing.B) {
	cp := &ControlPlane{internalToken: "bench-token"}
	client, ctx := startTestGRPCClient(b, cp)
	req := &pb.TenantRequest{TenantId: "tenant_bench_1"}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := client.TouchActivity(ctx, req); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGRPCBatchTouchActivity16(b *testing.B) {
	cp := &ControlPlane{internalToken: "bench-token"}
	client, ctx := startTestGRPCClient(b, cp)
	ids := make([]string, 16)
	for i := range ids {
		ids[i] = fmt.Sprintf("tenant_bench_%d", i)
	}
	req := &pb.BatchTouchActivityRequest{TenantIds: ids}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := client.BatchTouchActivity(ctx, req); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGRPCProtoMarshalUnmarshalRoute(b *testing.B) {
	resp := &pb.TenantRouteResponse{
		TenantId:    "tenant_bench_001",
		State:       "ACTIVE",
		Host:        "127.0.0.1",
		Port:        15433,
		SockDir:     "/var/lib/air/tenants/active/tenant_bench_001/sock",
		AppPassword: "rand_password_1234567890",
	}
	var out pb.TenantRouteResponse
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		raw, err := proto.Marshal(resp)
		if err != nil {
			b.Fatal(err)
		}
		if err := proto.Unmarshal(raw, &out); err != nil {
			b.Fatal(err)
		}
	}
}
