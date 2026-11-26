package gateway

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
)

func proxyBidirectional(client, backend net.Conn) {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, _ = io.Copy(backend, client)
		_ = backend.Close()
		_ = client.Close()
	}()
	_, _ = io.Copy(client, backend)
	_ = backend.Close()
	_ = client.Close()
	wg.Wait()
}

func forwardCancelRequest(client net.Conn, cancelReg *CancelRegistry) {
	var cancelPayload [8]byte
	if _, err := io.ReadFull(client, cancelPayload[:]); err != nil {
		return
	}
	gwPID := binary.BigEndian.Uint32(cancelPayload[0:4])
	gwSecret := binary.BigEndian.Uint32(cancelPayload[4:8])
	cancelReg.CancelSession(gwPID, gwSecret)
	target, ok := cancelReg.Lookup(gwPID, gwSecret)
	if !ok || target.Port <= 0 {
		return
	}
	sendBackendCancel(target.Host, target.SockDir, target.Port, target.BackendPID, target.BackendSecret, target.TLS)
}

// HandleClientConnection handles an accepted PostgreSQL client socket end-to-end:
//  1. Pre-startup GSSENCRequest / SSLRequest / CancelRequest handling
//  2. StartupMessage validation, tenant extraction & SNI consistency check
//  3. Per-tenant/per-user connection limit enforcement
//  4. Control Plane tenant resolution & SQLSTATE error mapping
//  5. Pooled serving (pool_mode=true) or direct bidirectional proxying
func HandleClientConnection(
	conn net.Conn,
	cfg Config,
	tlsCtx *TLSContext,
	discovery *TenantDiscovery,
	pools *PoolManager,
	metrics *MetricsRegistry,
	cancelReg *CancelRegistry,
) {
	defer metrics.ReleaseGlobal()
	defer conn.Close()

	if cfg.AuthTimeoutMS > 0 {
		_ = conn.SetDeadline(time.Now().Add(time.Duration(cfg.AuthTimeoutMS) * time.Millisecond))
	}

	var hdr [8]byte
	if _, err := io.ReadFull(conn, hdr[:]); err != nil {
		metrics.RecordTimeout()
		return
	}
	msgLen := binary.BigEndian.Uint32(hdr[0:4])
	code := binary.BigEndian.Uint32(hdr[4:8])

	var sniHost string
	tlsNegotiated := false

	// Negotiate optional GSSENCRequest (declined) and SSLRequest (TLS upgrade).
	for pre := 0; pre < 2; pre++ {
		if msgLen == 8 && code == GSSENCRequestCode {
			if _, err := conn.Write([]byte{'N'}); err != nil {
				return
			}
		} else if msgLen == 8 && code == SSLRequestCode && !tlsNegotiated {
			if _, err := conn.Write([]byte{'S'}); err != nil {
				return
			}
			tlsConn, sni, err := tlsCtx.UpgradeConn(conn)
			if err != nil {
				metrics.RecordProtocolError()
				return
			}
			conn = tlsConn
			sniHost = sni
			tlsNegotiated = true
		} else {
			break
		}
		if _, err := io.ReadFull(conn, hdr[:]); err != nil {
			return
		}
		msgLen = binary.BigEndian.Uint32(hdr[0:4])
		code = binary.BigEndian.Uint32(hdr[4:8])
	}

	if msgLen == 16 && code == CancelRequestCode {
		forwardCancelRequest(conn, cancelReg)
		return
	}

	if cfg.RequireTLS && !tlsNegotiated {
		metrics.RecordAuthFailure()
		_, _ = conn.Write(BuildErrorResponse("FATAL", "28000", "SSL/TLS connection is required"))
		return
	}

	if msgLen < 8 || msgLen > MaxStartupMessageBytes {
		metrics.RecordProtocolError()
		_, _ = conn.Write(BuildErrorResponse("FATAL", "08P01", "StartupMessage length exceeds 8KB security limit"))
		return
	}

	if code != PGProtocolVersion3 {
		metrics.RecordProtocolError()
		_, _ = conn.Write(BuildErrorResponse("FATAL", "0A000", "unsupported frontend protocol version"))
		return
	}

	body := make([]byte, msgLen-8)
	if len(body) > 0 {
		if _, err := io.ReadFull(conn, body); err != nil {
			return
		}
	}

	params := ParseStartupParams(body)
	startupTenant := ExtractTenant(sniHost, params)
	if startupTenant == "" || !ValidateTenantID(startupTenant) {
		metrics.RecordAuthFailure()
		_, _ = conn.Write(BuildErrorResponse("FATAL", "28000", "missing or invalid tenant identifier"))
		return
	}

	reqUser := strings.TrimSpace(params["user"])
	if reqUser == "" || strings.EqualFold(reqUser, "postgres") || (cfg.PoolMode && reqUser != startupTenant && reqUser != "app_user") {
		metrics.RecordAuthFailure()
		_, _ = conn.Write(BuildErrorResponse("FATAL", "28000", fmt.Sprintf("user %q is not permitted through tenant gateway", reqUser)))
		return
	}

	if sniHost != "" {
		sniTenant := ParseSNITenant(sniHost)
		dbTenant := strings.TrimSpace(params["database"])
		if sniTenant != "" && dbTenant != "" && dbTenant != "postgres" && sniTenant != dbTenant {
			metrics.RecordAuthFailure()
			_, _ = conn.Write(BuildErrorResponse("FATAL", "28000",
				fmt.Sprintf("SNI tenant (%s) does not match StartupMessage database (%s)", sniTenant, dbTenant)))
			return
		}
	}

	if err := metrics.TryAcquireTenantUser(startupTenant, reqUser); err != nil {
		_, _ = conn.Write(BuildErrorResponse("FATAL", "53300", err.Error()))
		return
	}
	var releaseTenantOnce sync.Once
	defer releaseTenantOnce.Do(func() {
		metrics.ReleaseTenantUser(startupTenant, reqUser)
	})

	var route TenantRoute
	var rpcCode codes.Code
	var err error
	if cfg.PoolMode {
		route, rpcCode, err = discovery.ResolveTenantNoWake(startupTenant)
	} else {
		route, rpcCode, err = discovery.ResolveTenant(startupTenant)
	}
	if err != nil {
		sqlstate := "08006"
		switch rpcCode {
		case codes.NotFound:
			sqlstate = "3D000"
		case codes.Unavailable, codes.DeadlineExceeded:
			sqlstate = "57P03"
		}
		log.Printf("[air-gateway] routing failed tenant=%s grpc_code=%v err=%v", startupTenant, rpcCode, err)
		_, _ = conn.Write(BuildErrorResponse("FATAL", sqlstate, fmt.Sprintf("tenant database %q unavailable: %v", startupTenant, err)))
		return
	}
	canServeSuspended := cfg.PoolMode && route.State == "SUSPENDED"
	if !canServeSuspended && (route.State != "ACTIVE" || route.Port <= 0) {
		_, _ = conn.Write(BuildErrorResponse("FATAL", "57P03", fmt.Sprintf("tenant database %q is in state %s", startupTenant, route.State)))
		return
	}

	if route.State == "ACTIVE" {
		discovery.TouchActivity(startupTenant)
	}

	if cfg.PoolMode {
		pool := pools.GetOrCreate(route, func() (TenantRoute, error) {
			return discovery.WakeTenant(startupTenant)
		})
		pool.ServeClient(conn, reqUser, route.AppPassword, func() {
			discovery.TouchActivity(startupTenant)
		}, metrics, cancelReg, cfg.ClientIdleTimeoutMS)
		return
	}

	_ = conn.SetDeadline(time.Time{})
	backend, err := dialBackendSocket(route.Host, route.SockDir, route.Port)
	if err != nil {
		discovery.EvictCache(startupTenant)
		_, _ = conn.Write(BuildErrorResponse("FATAL", "08006", "failed to dial cell backend"))
		return
	}
	defer backend.Close()

	params["database"] = startupTenant
	if _, err := backend.Write(BuildStartupMessage(params)); err != nil {
		return
	}
	proxyBidirectional(conn, backend)
}
