package gateway

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var latencyUpperBoundsUs = [12]uint64{
	500, 1000, 5000, 10000, 25000, 50000, 100000, 250000, 500000, 1000000, 5000000, math.MaxUint64,
}

// LatencyHistogram is a fixed-bucket microsecond latency histogram for p50/p95/p99 estimation.
type LatencyHistogram struct {
	Buckets [12]uint64
}

func (h *LatencyHistogram) Record(us uint64) {
	for i, bound := range latencyUpperBoundsUs {
		if us <= bound {
			h.Buckets[i]++
			return
		}
	}
}

func (h *LatencyHistogram) PercentileUs(p float64) uint64 {
	var total uint64
	for _, c := range h.Buckets {
		total += c
	}
	if total == 0 {
		return 0
	}
	target := uint64(p * float64(total))
	if target == 0 {
		target = 1
	}
	var cum uint64
	for i, c := range h.Buckets {
		cum += c
		if cum >= target {
			if i == len(h.Buckets)-1 {
				return 5000000
			}
			return latencyUpperBoundsUs[i]
		}
	}
	return 5000000
}

// QueryStats aggregates query execution, byte/row volume, and latency statistics.
type QueryStats struct {
	QueryCount      uint64
	ErrorCount      uint64
	BytesIn         uint64
	BytesOut        uint64
	RowsReturned    uint64
	DataRowBytes    uint64
	TotalQueryUs    uint64
	MinQueryUs      uint64
	MaxQueryUs      uint64
	TotalPoolWaitUs uint64
	Hist            LatencyHistogram
	LastQueryAtMs   int64
}

func (q *QueryStats) Record(durationUs, poolWaitUs, inBytes, outBytes, rows, rowBytes uint64, hadErr bool, count uint64, tsMs int64) {
	inc := count
	if inc == 0 {
		inc = 1
	}
	q.QueryCount += inc
	if hadErr {
		q.ErrorCount++
	}
	q.BytesIn += inBytes
	q.BytesOut += outBytes
	q.RowsReturned += rows
	q.DataRowBytes += rowBytes
	q.TotalQueryUs += durationUs
	q.TotalPoolWaitUs += poolWaitUs
	if q.QueryCount == inc || durationUs < q.MinQueryUs {
		q.MinQueryUs = durationUs
	}
	if durationUs > q.MaxQueryUs {
		q.MaxQueryUs = durationUs
	}
	q.Hist.Record(durationUs)
	q.LastQueryAtMs = tsMs
}

func (q *QueryStats) AvgQueryUs() uint64 {
	if q.QueryCount == 0 {
		return 0
	}
	return q.TotalQueryUs / q.QueryCount
}

func (q *QueryStats) AvgPoolWaitUs() uint64 {
	if q.QueryCount == 0 {
		return 0
	}
	return q.TotalPoolWaitUs / q.QueryCount
}

func (q *QueryStats) appendJSONFields(b *strings.Builder) {
	fmt.Fprintf(b,
		`"query_count":%d,"error_count":%d,"bytes_in":%d,"bytes_out":%d,"rows_returned":%d,"data_row_bytes":%d,"total_query_us":%d,"avg_query_us":%d,"min_query_us":%d,"max_query_us":%d,"p50_query_us":%d,"p95_query_us":%d,"p99_query_us":%d,"total_pool_wait_us":%d,"avg_pool_wait_us":%d,"last_query_at_ms":%d`,
		q.QueryCount, q.ErrorCount, q.BytesIn, q.BytesOut, q.RowsReturned, q.DataRowBytes,
		q.TotalQueryUs, q.AvgQueryUs(), q.MinQueryUs, q.MaxQueryUs,
		q.Hist.PercentileUs(0.50), q.Hist.PercentileUs(0.95), q.Hist.PercentileUs(0.99),
		q.TotalPoolWaitUs, q.AvgPoolWaitUs(), q.LastQueryAtMs,
	)
}

// PoolTenantSnapshot captures a point-in-time view of a tenant pool's state.
type PoolTenantSnapshot struct {
	TenantID           string
	ActiveBackends     int
	MaxBackends        int
	IdleBackends       int
	PinnedClients      int64
	ActiveTransactions int64
}

type TenantStats struct {
	TenantID            string
	ActiveConnections   int64
	TotalConnections    uint64
	RejectedConnections uint64
	Queries             QueryStats
}

type UserStats struct {
	TenantID            string
	User                string
	ActiveConnections   int64
	TotalConnections    uint64
	RejectedConnections uint64
	Queries             QueryStats
}

// JSONEscape escapes a string for safe embedding in JSON or Prometheus label values.
func JSONEscape(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch c {
		case '"':
			b.WriteString(`\"`)
		case '\\':
			b.WriteString(`\\`)
		case '\b':
			b.WriteString(`\b`)
		case '\f':
			b.WriteString(`\f`)
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		case '\t':
			b.WriteString(`\t`)
		default:
			if c < 0x20 {
				fmt.Fprintf(&b, `\u%04x`, c)
			} else {
				b.WriteByte(c)
			}
		}
	}
	return b.String()
}

// MetricsRegistry is a thread-safe registry for connection limits, error/timeout counters,
// per-tenant/per-user query metrics, and JSON/Prometheus rendering.
type MetricsRegistry struct {
	maxGlobalConns int
	maxTenantConns int
	maxUserConns   int
	startedAt      time.Time

	authFailures   atomic.Uint64
	protocolErrors atomic.Uint64
	rpcErrors      atomic.Uint64
	timeouts       atomic.Uint64
	poolExhausted  atomic.Uint64

	mu                     sync.Mutex
	activeGlobalConns      int64
	totalGlobalConns       uint64
	rejectedGlobalConns    uint64
	globalQueries          QueryStats
	tenants                map[string]*TenantStats
	users                  map[string]*UserStats
	tenantLimitOverrides   map[string]int
}

func NewMetricsRegistry(maxGlobalConns, maxTenantConns, maxUserConns int) *MetricsRegistry {
	return &MetricsRegistry{
		maxGlobalConns:       maxGlobalConns,
		maxTenantConns:       maxTenantConns,
		maxUserConns:         maxUserConns,
		startedAt:            time.Now(),
		tenants:              make(map[string]*TenantStats),
		users:                make(map[string]*UserStats),
		tenantLimitOverrides: make(map[string]int),
	}
}

func (m *MetricsRegistry) TryAcquireGlobal() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.maxGlobalConns > 0 && m.activeGlobalConns >= int64(m.maxGlobalConns) {
		m.rejectedGlobalConns++
		return false
	}
	m.activeGlobalConns++
	m.totalGlobalConns++
	return true
}

func (m *MetricsRegistry) ReleaseGlobal() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.activeGlobalConns > 0 {
		m.activeGlobalConns--
	}
}

func (m *MetricsRegistry) RecordAuthFailure()   { m.authFailures.Add(1) }
func (m *MetricsRegistry) RecordProtocolError() { m.protocolErrors.Add(1) }
func (m *MetricsRegistry) RecordRPCError()      { m.rpcErrors.Add(1) }
func (m *MetricsRegistry) RecordTimeout()       { m.timeouts.Add(1) }
func (m *MetricsRegistry) RecordPoolExhausted() { m.poolExhausted.Add(1) }

func userKey(tenantID, user string) string {
	return tenantID + "\x00" + user
}

func (m *MetricsRegistry) effectiveTenantLimitLocked(tenantID string) int {
	if v, ok := m.tenantLimitOverrides[tenantID]; ok {
		return v
	}
	return m.maxTenantConns
}

func (m *MetricsRegistry) TryAcquireTenantUser(tenantID, user string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	t, ok := m.tenants[tenantID]
	if !ok {
		t = &TenantStats{TenantID: tenantID}
		m.tenants[tenantID] = t
	}

	ukey := userKey(tenantID, user)
	u, ok := m.users[ukey]
	if !ok {
		u = &UserStats{TenantID: tenantID, User: user}
		m.users[ukey] = u
	}

	tLimit := m.effectiveTenantLimitLocked(tenantID)
	if tLimit > 0 && t.ActiveConnections >= int64(tLimit) {
		t.RejectedConnections++
		u.RejectedConnections++
		m.rejectedGlobalConns++
		return fmt.Errorf("too many client connections for tenant %q (limit=%d)", tenantID, tLimit)
	}

	if m.maxUserConns > 0 && u.ActiveConnections >= int64(m.maxUserConns) {
		t.RejectedConnections++
		u.RejectedConnections++
		m.rejectedGlobalConns++
		return fmt.Errorf("too many client connections for user %q on tenant %q (limit=%d)", user, tenantID, m.maxUserConns)
	}

	t.ActiveConnections++
	t.TotalConnections++
	u.ActiveConnections++
	u.TotalConnections++
	return nil
}

func (m *MetricsRegistry) ReleaseTenantUser(tenantID, user string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if t, ok := m.tenants[tenantID]; ok && t.ActiveConnections > 0 {
		t.ActiveConnections--
	}
	if u, ok := m.users[userKey(tenantID, user)]; ok && u.ActiveConnections > 0 {
		u.ActiveConnections--
	}
}

func (m *MetricsRegistry) SetTenantClientLimit(tenantID string, maxConns int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if maxConns <= 0 {
		delete(m.tenantLimitOverrides, tenantID)
	} else {
		m.tenantLimitOverrides[tenantID] = maxConns
	}
}

func (m *MetricsRegistry) GetTenantClientLimit(tenantID string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.effectiveTenantLimitLocked(tenantID)
}

func (m *MetricsRegistry) RecordQuery(tenantID, user string, durationUs, poolWaitUs, bytesIn, bytesOut, rowsReturned, dataRowBytes uint64, hadErr bool, count uint64) {
	ts := time.Now().UnixMilli()
	m.mu.Lock()
	defer m.mu.Unlock()

	m.globalQueries.Record(durationUs, poolWaitUs, bytesIn, bytesOut, rowsReturned, dataRowBytes, hadErr, count, ts)

	t, ok := m.tenants[tenantID]
	if !ok {
		t = &TenantStats{TenantID: tenantID}
		m.tenants[tenantID] = t
	}
	t.Queries.Record(durationUs, poolWaitUs, bytesIn, bytesOut, rowsReturned, dataRowBytes, hadErr, count, ts)

	ukey := userKey(tenantID, user)
	u, ok := m.users[ukey]
	if !ok {
		u = &UserStats{TenantID: tenantID, User: user}
		m.users[ukey] = u
	}
	u.Queries.Record(durationUs, poolWaitUs, bytesIn, bytesOut, rowsReturned, dataRowBytes, hadErr, count, ts)
}

func (m *MetricsRegistry) ToJSON(poolSnaps []PoolTenantSnapshot, tenantFilter, userFilter string) string {
	pmap := make(map[string]PoolTenantSnapshot, len(poolSnaps))
	for _, ps := range poolSnaps {
		pmap[ps.TenantID] = ps
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	uptimeS := int64(time.Since(m.startedAt).Seconds())
	var b strings.Builder
	fmt.Fprintf(&b,
		`{"uptime_seconds":%d,"limits":{"max_client_conns":%d,"max_tenant_client_conns":%d,"max_user_client_conns":%d},"global":{"active_connections":%d,"total_connections":%d,"rejected_connections":%d,"auth_failures":%d,"protocol_errors":%d,"rpc_errors":%d,"timeouts":%d,"pool_exhausted":%d,`,
		uptimeS, m.maxGlobalConns, m.maxTenantConns, m.maxUserConns,
		m.activeGlobalConns, m.totalGlobalConns, m.rejectedGlobalConns,
		m.authFailures.Load(), m.protocolErrors.Load(), m.rpcErrors.Load(),
		m.timeouts.Load(), m.poolExhausted.Load(),
	)
	m.globalQueries.appendJSONFields(&b)
	b.WriteString(`},"tenants":[`)

	firstT := true
	for tid, t := range m.tenants {
		if tenantFilter != "" && tid != tenantFilter {
			continue
		}
		if !firstT {
			b.WriteByte(',')
		}
		firstT = false
		ps := pmap[tid]
		fmt.Fprintf(&b,
			`{"tenant_id":"%s","max_client_conns":%d,"active_connections":%d,"total_connections":%d,"rejected_connections":%d,"active_backends":%d,"max_backends":%d,"idle_backends":%d,"pinned_clients":%d,"active_transactions":%d,`,
			JSONEscape(tid), m.effectiveTenantLimitLocked(tid), t.ActiveConnections, t.TotalConnections, t.RejectedConnections,
			ps.ActiveBackends, ps.MaxBackends, ps.IdleBackends, ps.PinnedClients, ps.ActiveTransactions,
		)
		t.Queries.appendJSONFields(&b)
		b.WriteByte('}')
	}
	b.WriteString(`],"users":[`)

	firstU := true
	for _, u := range m.users {
		if tenantFilter != "" && u.TenantID != tenantFilter {
			continue
		}
		if userFilter != "" && u.User != userFilter {
			continue
		}
		if !firstU {
			b.WriteByte(',')
		}
		firstU = false
		fmt.Fprintf(&b,
			`{"tenant_id":"%s","user":"%s","active_connections":%d,"total_connections":%d,"rejected_connections":%d,`,
			JSONEscape(u.TenantID), JSONEscape(u.User), u.ActiveConnections, u.TotalConnections, u.RejectedConnections,
		)
		u.Queries.appendJSONFields(&b)
		b.WriteByte('}')
	}
	b.WriteString(`]}`)
	return b.String()
}

func emitUserCounter(b *strings.Builder, name, help string, users map[string]*UserStats, valFn func(*UserStats) string) {
	fmt.Fprintf(b, "# HELP %s %s\n# TYPE %s counter\n", name, help, name)
	for _, u := range users {
		fmt.Fprintf(b, `%s{tenant="%s",user="%s"} %s`+"\n",
			name, JSONEscape(u.TenantID), JSONEscape(u.User), valFn(u))
	}
}

func (m *MetricsRegistry) ToPrometheus(poolSnaps []PoolTenantSnapshot) string {
	m.mu.Lock()
	defer m.mu.Unlock()

	var b strings.Builder
	fmt.Fprintf(&b, "# HELP air_gateway_connections_active Active client connections\n# TYPE air_gateway_connections_active gauge\nair_gateway_connections_active %d\n", m.activeGlobalConns)
	fmt.Fprintf(&b, "# HELP air_gateway_connections_rejected_total Total rejected client connections\n# TYPE air_gateway_connections_rejected_total counter\nair_gateway_connections_rejected_total %d\n", m.rejectedGlobalConns)

	emitUserCounter(&b, "air_gateway_queries_total", "Total queries executed", m.users, func(u *UserStats) string {
		return strconv.FormatUint(u.Queries.QueryCount, 10)
	})
	emitUserCounter(&b, "air_gateway_query_errors_total", "Total failed queries", m.users, func(u *UserStats) string {
		return strconv.FormatUint(u.Queries.ErrorCount, 10)
	})
	emitUserCounter(&b, "air_gateway_query_duration_seconds_sum", "Total query execution time in seconds", m.users, func(u *UserStats) string {
		return fmt.Sprintf("%.6f", float64(u.Queries.TotalQueryUs)/1e6)
	})
	emitUserCounter(&b, "air_gateway_bytes_in_total", "Total query bytes received from clients", m.users, func(u *UserStats) string {
		return strconv.FormatUint(u.Queries.BytesIn, 10)
	})
	emitUserCounter(&b, "air_gateway_bytes_out_total", "Total response bytes sent to clients", m.users, func(u *UserStats) string {
		return strconv.FormatUint(u.Queries.BytesOut, 10)
	})
	emitUserCounter(&b, "air_gateway_rows_returned_total", "Total DataRow rows returned to clients", m.users, func(u *UserStats) string {
		return strconv.FormatUint(u.Queries.RowsReturned, 10)
	})

	b.WriteString("# HELP air_gateway_pool_backends_active Active pooled backend connections\n# TYPE air_gateway_pool_backends_active gauge\n")
	for _, ps := range poolSnaps {
		fmt.Fprintf(&b, `air_gateway_pool_backends_active{tenant="%s"} %d`+"\n", JSONEscape(ps.TenantID), ps.ActiveBackends)
	}

	fmt.Fprintf(&b, "# HELP air_gateway_auth_failures_total Total authentication failures\n# TYPE air_gateway_auth_failures_total counter\nair_gateway_auth_failures_total %d\n", m.authFailures.Load())
	fmt.Fprintf(&b, "# HELP air_gateway_protocol_errors_total Total PostgreSQL wire protocol errors\n# TYPE air_gateway_protocol_errors_total counter\nair_gateway_protocol_errors_total %d\n", m.protocolErrors.Load())
	fmt.Fprintf(&b, "# HELP air_gateway_rpc_errors_total Total Control Plane RPC failures\n# TYPE air_gateway_rpc_errors_total counter\nair_gateway_rpc_errors_total %d\n", m.rpcErrors.Load())
	fmt.Fprintf(&b, "# HELP air_gateway_timeouts_total Total gateway timeouts\n# TYPE air_gateway_timeouts_total counter\nair_gateway_timeouts_total %d\n", m.timeouts.Load())
	fmt.Fprintf(&b, "# HELP air_gateway_pool_exhausted_total Total backend pool saturation events\n# TYPE air_gateway_pool_exhausted_total counter\nair_gateway_pool_exhausted_total %d\n", m.poolExhausted.Load())

	return b.String()
}
