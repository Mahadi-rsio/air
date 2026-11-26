package gateway

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Mahadi-rsio/air/internal/prom"
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
	ReadBreakerOpen    bool
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

	authFailures       atomic.Uint64
	protocolErrors     atomic.Uint64
	rpcErrors          atomic.Uint64
	timeouts           atomic.Uint64
	poolExhausted      atomic.Uint64
	readEngineRequests atomic.Uint64
	readEngineInFlight atomic.Int64
	readEngineFallback atomic.Uint64
	readBreakerTrips   atomic.Uint64
	clientDisconnects  atomic.Uint64
	queryCancellations atomic.Uint64
	readEngineLatency  *prom.Histogram

	mu                   sync.Mutex
	activeGlobalConns    int64
	totalGlobalConns     uint64
	rejectedGlobalConns  uint64
	globalQueries        QueryStats
	tenants              map[string]*TenantStats
	users                map[string]*UserStats
	tenantLimitOverrides map[string]int

	writeGov  *WriteGovernor
	bufBudget *BufferBudget
	telemetry *TelemetryStore
}

func NewMetricsRegistry(maxGlobalConns, maxTenantConns, maxUserConns int) *MetricsRegistry {
	return &MetricsRegistry{
		maxGlobalConns:       maxGlobalConns,
		maxTenantConns:       maxTenantConns,
		maxUserConns:         maxUserConns,
		startedAt:            time.Now(),
		readEngineLatency:    prom.NewHistogram(nil),
		tenants:              make(map[string]*TenantStats),
		users:                make(map[string]*UserStats),
		tenantLimitOverrides: make(map[string]int),
	}
}

// SetWriteGovernorComponents attaches Phase 20 governor, buffer budget, and telemetry store for metrics export.
func (m *MetricsRegistry) SetWriteGovernorComponents(gov *WriteGovernor, buf *BufferBudget, tel *TelemetryStore) {
	m.mu.Lock()
	m.writeGov = gov
	m.bufBudget = buf
	m.telemetry = tel
	m.mu.Unlock()
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

// ActiveClients returns the current number of active client connections.
func (m *MetricsRegistry) ActiveClients() int64 {
	if m == nil {
		return 0
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.activeGlobalConns
}

func (m *MetricsRegistry) RecordAuthFailure()   { m.authFailures.Add(1) }
func (m *MetricsRegistry) RecordProtocolError() { m.protocolErrors.Add(1) }
func (m *MetricsRegistry) RecordRPCError()      { m.rpcErrors.Add(1) }
func (m *MetricsRegistry) RecordTimeout()       { m.timeouts.Add(1) }
func (m *MetricsRegistry) RecordPoolExhausted() { m.poolExhausted.Add(1) }

func (m *MetricsRegistry) IncReadEngineInFlight()   { m.readEngineInFlight.Add(1) }
func (m *MetricsRegistry) DecReadEngineInFlight()   { m.readEngineInFlight.Add(-1) }
func (m *MetricsRegistry) RecordReadEngineRequest(d time.Duration) {
	m.readEngineRequests.Add(1)
	if m.readEngineLatency != nil {
		m.readEngineLatency.Observe(d)
	}
}
func (m *MetricsRegistry) RecordReadEngineFallback() { m.readEngineFallback.Add(1) }
func (m *MetricsRegistry) RecordReadBreakerTrip()    { m.readBreakerTrips.Add(1) }
func (m *MetricsRegistry) RecordClientDisconnect()   { m.clientDisconnects.Add(1) }
func (m *MetricsRegistry) RecordQueryCancellation()  { m.queryCancellations.Add(1) }

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

	prom.WriteCounter(&b, "read_engine_requests_total", "Total read requests dispatched to Read Engine", m.readEngineRequests.Load())
	prom.WriteGauge(&b, "read_engine_in_flight", "Current in-flight Read Engine requests", m.readEngineInFlight.Load())
	if m.readEngineLatency != nil {
		m.readEngineLatency.WritePrometheus(&b, "read_engine_latency_seconds", "Read Engine request execution latency in seconds")
	}
	prom.WriteCounter(&b, "read_engine_fallbacks_total", "Total Read Engine requests that fell back to PostgreSQL primary", m.readEngineFallback.Load())
	var openBreakers int64
	for _, ps := range poolSnaps {
		if ps.ReadBreakerOpen {
			openBreakers++
		}
	}
	prom.WriteGauge(&b, "read_breaker_open", "Number of tenant pools with an open Read Engine circuit breaker", openBreakers)
	prom.WriteCounter(&b, "read_breaker_trips_total", "Total Read Engine circuit breaker trip events", m.readBreakerTrips.Load())
	b.WriteString("# HELP tenant_active_connections Active client connections per tenant\n# TYPE tenant_active_connections gauge\n")
	for _, t := range m.tenants {
		fmt.Fprintf(&b, `tenant_active_connections{tenant="%s"} %d`+"\n", JSONEscape(t.TenantID), t.ActiveConnections)
	}
	b.WriteString("# HELP tenant_connection_rejections_total Total rejected client connections per tenant\n# TYPE tenant_connection_rejections_total counter\n")
	for _, t := range m.tenants {
		fmt.Fprintf(&b, `tenant_connection_rejections_total{tenant="%s"} %d`+"\n", JSONEscape(t.TenantID), t.RejectedConnections)
	}
	prom.WriteCounter(&b, "client_disconnects_total", "Total client disconnects detected during query execution", m.clientDisconnects.Load())
	prom.WriteCounter(&b, "query_cancellations_total", "Total query cancellations propagated to backends", m.queryCancellations.Load())

	var totalActiveBackends, totalIdleBackends int
	for _, ps := range poolSnaps {
		totalActiveBackends += ps.ActiveBackends
		totalIdleBackends += ps.IdleBackends
	}
	fmt.Fprintf(&b, "# HELP air_backend_active Total active backend connections across pools\n# TYPE air_backend_active gauge\nair_backend_active %d\n", totalActiveBackends)
	fmt.Fprintf(&b, "# HELP air_backend_idle Total idle backend connections across pools\n# TYPE air_backend_idle gauge\nair_backend_idle %d\n", totalIdleBackends)

	if m.writeGov != nil {
		gs := m.writeGov.Snapshot()
		fmt.Fprintf(&b, "# HELP air_write_active Active write executions\n# TYPE air_write_active gauge\nair_write_active %d\n", gs.ActiveWrites)
		fmt.Fprintf(&b, "# HELP air_write_queue_depth Queued write requests\n# TYPE air_write_queue_depth gauge\nair_write_queue_depth %d\n", gs.QueueDepth)
		fmt.Fprintf(&b, "# HELP air_write_queue_bytes Queued write request bytes\n# TYPE air_write_queue_bytes gauge\nair_write_queue_bytes %d\n", gs.QueueBytes)
		fmt.Fprintf(&b, "# HELP air_write_queue_wait_seconds Write queue wait latency quantiles\n# TYPE air_write_queue_wait_seconds gauge\nair_write_queue_wait_seconds{quantile=\"0.5\"} %.6f\nair_write_queue_wait_seconds{quantile=\"0.95\"} %.6f\nair_write_queue_wait_seconds{quantile=\"0.99\"} %.6f\n",
			float64(gs.QueueWaitP50Us)/1e6, float64(gs.QueueWaitP95Us)/1e6, float64(gs.QueueWaitP99Us)/1e6)
		fmt.Fprintf(&b, "# HELP air_write_execution_seconds Write execution latency quantiles\n# TYPE air_write_execution_seconds gauge\nair_write_execution_seconds{quantile=\"0.5\"} %.6f\nair_write_execution_seconds{quantile=\"0.95\"} %.6f\nair_write_execution_seconds{quantile=\"0.99\"} %.6f\n",
			float64(gs.ExecP50Us)/1e6, float64(gs.ExecP95Us)/1e6, float64(gs.ExecP99Us)/1e6)
		fmt.Fprintf(&b, "# HELP air_write_rejected_total Total write requests rejected due to full queue\n# TYPE air_write_rejected_total counter\nair_write_rejected_total %d\n", gs.RejectedTotal)
		fmt.Fprintf(&b, "# HELP air_write_cancelled_total Total write requests cancelled while queued\n# TYPE air_write_cancelled_total counter\nair_write_cancelled_total %d\n", gs.CancelledTotal)
		fmt.Fprintf(&b, "# HELP air_write_timeout_total Total write requests timed out while queued\n# TYPE air_write_timeout_total counter\nair_write_timeout_total %d\n", gs.TimeoutTotal)
		fmt.Fprintf(&b, "# HELP air_write_errors_total Total write execution errors\n# TYPE air_write_errors_total counter\nair_write_errors_total %d\n", gs.ErrorsTotal)
		fmt.Fprintf(&b, "# HELP air_write_throughput Total completed governed writes\n# TYPE air_write_throughput counter\nair_write_throughput %d\n", gs.CompletedTotal)
		fmt.Fprintf(&b, "# HELP air_write_limit_global Effective global write concurrency limit\n# TYPE air_write_limit_global gauge\nair_write_limit_global %d\n", gs.EffectiveGlobalLimit)
		fmt.Fprintf(&b, "# HELP air_write_limit_tenant Default per-tenant write concurrency limit\n# TYPE air_write_limit_tenant gauge\nair_write_limit_tenant %d\n", gs.MaxPerTenant)
		fmt.Fprintf(&b, "# HELP air_write_controller_state Adaptive write governor state (0=static,1=healthy,2=pressured,3=critical,4=stale)\n# TYPE air_write_controller_state gauge\nair_write_controller_state %d\n", int(gs.ControllerState))
		fmt.Fprintf(&b, "# HELP air_transaction_open Currently open explicit transactions\n# TYPE air_transaction_open gauge\nair_transaction_open %d\n", gs.OpenTransactions)
		fmt.Fprintf(&b, "# HELP air_transaction_duration_seconds Explicit transaction duration quantiles\n# TYPE air_transaction_duration_seconds gauge\nair_transaction_duration_seconds{quantile=\"0.5\"} %.6f\nair_transaction_duration_seconds{quantile=\"0.95\"} %.6f\nair_transaction_duration_seconds{quantile=\"0.99\"} %.6f\n",
			float64(gs.TxDurP50Us)/1e6, float64(gs.TxDurP95Us)/1e6, float64(gs.TxDurP99Us)/1e6)
	}

	if m.bufBudget != nil {
		used := m.bufBudget.UsedBytes()
		maxB := m.bufBudget.GlobalMaxBytes()
		ratio := 0.0
		if maxB > 0 {
			ratio = float64(used) / float64(maxB)
		}
		fmt.Fprintf(&b, "# HELP air_gateway_buffered_bytes Currently reserved gateway protocol buffer bytes\n# TYPE air_gateway_buffered_bytes gauge\nair_gateway_buffered_bytes %d\n", used)
		fmt.Fprintf(&b, "# HELP air_gateway_buffer_budget_used Fraction of global gateway protocol buffer budget in use\n# TYPE air_gateway_buffer_budget_used gauge\nair_gateway_buffer_budget_used %.6f\n", ratio)
		fmt.Fprintf(&b, "# HELP air_gateway_buffer_rejections_total Total protocol buffer reservation rejections\n# TYPE air_gateway_buffer_rejections_total counter\nair_gateway_buffer_rejections_total %d\n", m.bufBudget.Rejections())
	}

	if m.telemetry != nil {
		tel, _ := m.telemetry.Latest(time.Now())
		fmt.Fprintf(&b, "# HELP air_cell_memory_current_bytes Latest cell cgroup memory.current bytes\n# TYPE air_cell_memory_current_bytes gauge\nair_cell_memory_current_bytes %d\n", tel.MemoryCurrentBytes)
		fmt.Fprintf(&b, "# HELP air_cell_memory_high_bytes Latest cell cgroup memory.high bytes\n# TYPE air_cell_memory_high_bytes gauge\nair_cell_memory_high_bytes %d\n", tel.MemoryHighBytes)
		fmt.Fprintf(&b, "# HELP air_cell_memory_max_bytes Latest cell cgroup memory.max bytes\n# TYPE air_cell_memory_max_bytes gauge\nair_cell_memory_max_bytes %d\n", tel.MemoryMaxBytes)
		fmt.Fprintf(&b, "# HELP air_cell_memory_pressure Latest cell memory PSI some avg10\n# TYPE air_cell_memory_pressure gauge\nair_cell_memory_pressure %.4f\n", tel.MemoryPSISomeAvg10)
		fmt.Fprintf(&b, "# HELP air_cell_memory_oom_events Latest cell cgroup OOM event count\n# TYPE air_cell_memory_oom_events counter\nair_cell_memory_oom_events %d\n", tel.MemoryOOMEvents)
	}

	return b.String()
}
