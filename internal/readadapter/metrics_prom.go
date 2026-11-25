package readadapter

import (
	"strings"

	"github.com/Mahadi-rsio/air/internal/prom"
)

// appendPhase25PrometheusMetrics appends Phase 25 Read Engine metrics in Prometheus text
// exposition format with bounded cardinality (no per-tenant labels).
func (s *SQLiteReadServer) appendPhase25PrometheusMetrics(b *strings.Builder) {

	// TenantScheduler metrics
	if s.scheduler != nil {
		ss := s.scheduler.Stats()
		prom.WriteGauge(b, "read_engine_scheduler_active_global", "Active global query executions in TenantScheduler", int64(ss.ActiveGlobal))
		prom.WriteGauge(b, "read_engine_scheduler_queued_global", "Queued global requests in TenantScheduler", int64(ss.QueuedGlobal))
		prom.WriteGauge(b, "read_engine_scheduler_max_global", "Configured max global concurrency in TenantScheduler", int64(ss.MaxGlobalConcurrency))
		prom.WriteGauge(b, "read_engine_scheduler_active_tenants", "Number of tenants with active queries", int64(ss.ActiveTenants))
		prom.WriteGauge(b, "read_engine_scheduler_tenant_active_max", "Maximum active queries for any single tenant", int64(ss.TenantActiveMax))
		prom.WriteGauge(b, "read_engine_scheduler_tenant_queued_max", "Maximum queued queries for any single tenant", int64(ss.TenantQueuedMax))
		prom.WriteCounter(b, "read_engine_scheduler_admitted_total", "Total queries admitted by TenantScheduler", ss.AdmittedTotal)
		prom.WriteCounter(b, "read_engine_scheduler_backpressure_total", "Total TenantScheduler backpressure rejections", ss.BackpressureTotal)
		prom.WriteCounter(b, "read_engine_scheduler_cancelled_waiters_total", "Total TenantScheduler queued waiters cancelled", ss.CancelledWaitTotal)
		s.scheduler.WaitHistogram().WritePrometheus(b, "read_engine_scheduler_wait_seconds", "TenantScheduler queue wait duration in seconds")
	}

	// Reader Pool metrics
	var availReaders int64
	s.mu.RLock()
	for _, ts := range s.tenants {
		if ts != nil && ts.readerPool != nil {
			availReaders += int64(len(ts.readerPool))
		}
	}
	s.mu.RUnlock()
	prom.WriteGauge(b, "read_engine_reader_pool_active", "Active SQLite reader slots across tenants", s.readerPoolActive.Load())
	prom.WriteGauge(b, "read_engine_reader_pool_available", "Available SQLite reader slots across tenants", availReaders)
	prom.WriteCounter(b, "read_engine_reader_pool_timeouts_total", "Total reader pool acquisition timeouts", s.readerPoolTimeouts.Load())
	s.readerPoolWaitHist.WritePrometheus(b, "read_engine_reader_pool_wait_seconds", "Reader pool acquisition wait duration in seconds")

	// Pageserver RPC metrics
	if s.nativeGRPC != nil {
		ps := s.nativeGRPC.Stats()
		prom.WriteGauge(b, "read_engine_pageserver_rpc_in_flight", "In-flight Pageserver gRPC requests", ps.InFlight)
		prom.WriteCounter(b, "read_engine_pageserver_rpc_requests_total", "Total Pageserver gRPC requests", ps.RequestsTotal)
		prom.WriteCounter(b, "read_engine_pageserver_rpc_errors_total", "Total Pageserver gRPC errors", ps.FailuresTotal)
		prom.WriteCounter(b, "read_engine_pageserver_rpc_timeouts_total", "Total Pageserver gRPC timeouts", ps.TimeoutsTotal)
		s.nativeGRPC.LatencyHistogram().WritePrometheus(b, "read_engine_pageserver_rpc_latency_seconds", "Pageserver gRPC request latency in seconds")
	} else if s.nativeSock != nil {
		ps := s.nativeSock.Stats()
		prom.WriteGauge(b, "read_engine_pageserver_rpc_in_flight", "In-flight Pageserver socket requests", ps.InFlight)
		prom.WriteCounter(b, "read_engine_pageserver_rpc_requests_total", "Total Pageserver socket requests", ps.RequestsTotal)
		prom.WriteCounter(b, "read_engine_pageserver_rpc_errors_total", "Total Pageserver socket errors", ps.FailuresTotal)
		prom.WriteCounter(b, "read_engine_pageserver_rpc_timeouts_total", "Total Pageserver socket timeouts", ps.TimeoutsTotal)
	}

	// Tiered Cache metrics
	if s.nativeCache != nil {
		cs := s.nativeCache.Stats()
		prom.WriteCounter(b, "read_engine_cache_l1_hits_total", "TieredCache L1 hits", cs.L1Hits)
		prom.WriteCounter(b, "read_engine_cache_l1_misses_total", "TieredCache L1 misses", cs.L1Misses)
		prom.WriteCounter(b, "read_engine_cache_l1_evictions_total", "TieredCache L1 evictions", cs.L1Evictions)
		prom.WriteGauge(b, "read_engine_cache_l1_bytes", "TieredCache L1 bytes used", cs.L1Bytes)
		prom.WriteCounter(b, "read_engine_cache_l2_hits_total", "TieredCache L2 hits", cs.L2Hits)
		prom.WriteCounter(b, "read_engine_cache_l2_misses_total", "TieredCache L2 misses", cs.L2Misses)
		prom.WriteCounter(b, "read_engine_cache_l2_evictions_total", "TieredCache L2 evictions", cs.L2Evictions)
		prom.WriteGauge(b, "read_engine_cache_l2_bytes", "TieredCache L2 bytes used", cs.L2Bytes)
		prom.WriteCounter(b, "read_engine_cache_l3_hits_total", "TieredCache L3 Pageserver fetch successes", cs.L3Hits)
		prom.WriteCounter(b, "read_engine_cache_l3_misses_total", "TieredCache L3 Pageserver fetch errors/misses", cs.L3Misses)
		prom.WriteCounter(b, "read_engine_cache_l3_fetches_total", "TieredCache L3 total fetches", cs.L3Fetches)
		prom.WriteCounter(b, "read_engine_cache_tenant_invalidations_total", "TieredCache full tenant invalidations", cs.TenantInvalidations)
		prom.WriteCounter(b, "read_engine_cache_reset_required_total", "TieredCache ResetRequired events from Pageserver", cs.ResetRequiredTotal)
		s.nativeCache.SyncLatencyHistogram().WritePrometheus(b, "read_engine_cache_sync_latency_seconds", "TieredCache synchronization latency in seconds")
	}

	prom.WriteGauge(b, "read_engine_active_queries", "Active SQLite read engine queries", s.activeQueries.Load())
	prom.WriteCounter(b, "read_engine_queries_allowed_total", "Total queries allowed", s.queriesAllowed.Load())
	prom.WriteCounter(b, "read_engine_queries_failed_total", "Total queries failed", s.queriesFailed.Load())
	prom.WriteCounter(b, "read_engine_queries_cancelled_total", "Total queries cancelled", s.queriesCancelled.Load())
}
