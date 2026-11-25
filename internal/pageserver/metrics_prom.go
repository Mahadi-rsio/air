package pageserver

import (
	"strings"

	"github.com/Mahadi-rsio/air/internal/prom"
)

// PrometheusMetrics renders Pageserver metrics in Prometheus text exposition format
// with bounded cardinality (no per-tenant labels).
func (p *Pageserver) PrometheusMetrics() string {
	var b strings.Builder

	// ReadLimiter metrics
	if p.store != nil && p.store.readLimiter != nil {
		rs := p.store.readLimiter.Stats()
		prom.WriteGauge(&b, "pageserver_read_limiter_active", "Active concurrent reads in ReadLimiter", int64(rs.Active))
		prom.WriteGauge(&b, "pageserver_read_limiter_pending", "Pending queued reads in ReadLimiter", int64(rs.Pending))
		prom.WriteCounter(&b, "pageserver_read_limiter_admitted_total", "Total reads admitted by ReadLimiter", uint64(rs.Admitted))
		prom.WriteCounter(&b, "pageserver_read_limiter_rejected_total", "Total reads rejected due to full ReadLimiter queue", uint64(rs.RejectedFull))
		prom.WriteCounter(&b, "pageserver_read_limiter_cancelled_total", "Total reads cancelled while waiting in ReadLimiter", uint64(rs.Cancelled))
		p.store.readLimiter.WaitHistogram().WritePrometheus(&b, "pageserver_read_limiter_wait_seconds", "ReadLimiter queue wait duration in seconds")
	}

	// GetPageAtLSN & Storage I/O & Layer Lookup metrics
	if p.store != nil {
		p.store.getPageLookupHist.WritePrometheus(&b, "pageserver_getpage_layer_lookup_seconds", "GetPageAtLSN layer lookup phase latency in seconds")
		p.store.readRangeHist.WritePrometheus(&b, "pageserver_getpage_storage_io_seconds", "ReadRange storage I/O latency in seconds")
		p.store.getPageRedoHist.WritePrometheus(&b, "pageserver_getpage_wal_redo_seconds", "GetPageAtLSN WAL redo reconstruction latency in seconds")
		p.store.getPageTotalHist.WritePrometheus(&b, "pageserver_getpage_total_seconds", "GetPageAtLSN total page reconstruction latency in seconds")
		p.store.layerLookupHist.WritePrometheus(&b, "pageserver_layer_lookup_seconds", "Per-layer LookupKeyInLayer duration in seconds")
		p.store.layerScansHist.WriteValueHistogram(&b, "pageserver_layer_scans_per_request", "Number of layers scanned per GetPageAtLSN request")
		prom.WriteGauge(&b, "pageserver_compactions_active", "Active L0->L1 compactions", p.store.compactionsActive.Load())
		prom.WriteCounter(&b, "pageserver_compactions_total", "Total L0->L1 compactions started", p.store.compactionsTotal.Load())

		if p.store.idxCache != nil {
			ih, im, ie := p.store.idxCache.Stats()
			prom.WriteCounter(&b, "pageserver_index_cache_hits_total", "Layer index cache hits", ih)
			prom.WriteCounter(&b, "pageserver_index_cache_misses_total", "Layer index cache misses", im)
			prom.WriteCounter(&b, "pageserver_index_cache_evictions_total", "Layer index cache evictions", ie)
		}
		if p.store.decodedChunks != nil {
			ds := p.store.decodedChunks.Stats()
			prom.WriteCounter(&b, "pageserver_decoded_chunk_cache_hits_total", "DecodedChunkCache hits", uint64(ds.Hits))
			prom.WriteCounter(&b, "pageserver_decoded_chunk_cache_misses_total", "DecodedChunkCache misses", uint64(ds.Misses))
			prom.WriteCounter(&b, "pageserver_decoded_chunk_cache_evictions_total", "DecodedChunkCache evictions", uint64(ds.Evictions))
			prom.WriteGauge(&b, "pageserver_decoded_chunk_cache_bytes", "DecodedChunkCache bytes used", ds.Bytes)
		}
	}

	// Layer Management (aggregate across timelines)
	timelines := p.snapshotTimelines()
	var totalLayers, maxLayersPerTimeline, l0Count, l1Count, imgCount int64
	var pendingFlushBytes int64
	for _, tl := range timelines {
		pendingFlushBytes += tl.PendingFlushBytes()
		tl.mu.RLock()
		nLayers := int64(len(tl.layers))
		totalLayers += nLayers
		if nLayers > maxLayersPerTimeline {
			maxLayersPerTimeline = nLayers
		}
		for _, lm := range tl.layers {
			switch lm.Kind {
			case LayerKindL0:
				l0Count++
			case LayerKindL1:
				l1Count++
			case LayerKindImage:
				imgCount++
			}
		}
		tl.mu.RUnlock()
	}
	prom.WriteGauge(&b, "pageserver_timelines_active", "Active timelines in Pageserver", int64(len(timelines)))
	prom.WriteGauge(&b, "pageserver_layers_active_total", "Total active immutable layers across all timelines", totalLayers)
	prom.WriteGauge(&b, "pageserver_layers_active_max_per_timeline", "Maximum active layers on any single timeline", maxLayersPerTimeline)
	prom.WriteGauge(&b, "pageserver_layers_l0", "Active L0 layers across all timelines", l0Count)
	prom.WriteGauge(&b, "pageserver_layers_l1", "Active L1 layers across all timelines", l1Count)
	prom.WriteGauge(&b, "pageserver_layers_img", "Active Image layers across all timelines", imgCount)

	// WAL Redo metrics
	if dr, ok := p.redoer.(*DefaultWALRedoer); ok && dr != nil {
		ws := dr.Stats()
		prom.WriteGauge(&b, "pageserver_walredo_active_workers", "Active WAL redo worker processes", ws.ActiveWorkers)
		prom.WriteGauge(&b, "pageserver_walredo_queue_depth", "Queued WAL redo jobs", int64(ws.QueueDepth))
		ws.QueueWaitHist.WritePrometheus(&b, "pageserver_walredo_queue_wait_seconds", "WAL redo queue wait duration in seconds")
		ws.ExecHist.WritePrometheus(&b, "pageserver_walredo_exec_seconds", "WAL redo execution duration in seconds")
		prom.WriteCounter(&b, "pageserver_walredo_timeouts_total", "Total WAL redo timeouts", ws.TimeoutsTotal)
		prom.WriteCounter(&b, "pageserver_walredo_failures_total", "Total WAL redo failures", ws.FailuresTotal)
	}

	// Memory metrics
	ms := p.MemoryStats()
	prom.WriteGauge(&b, "pageserver_memory_inmem_bytes", "Total InMemoryLayer bytes across timelines", ms.GlobalInMemBytes)
	prom.WriteGauge(&b, "pageserver_memory_pending_flush_bytes", "Total frozen InMemoryLayer bytes pending flush", pendingFlushBytes)
	prom.WriteCounter(&b, "pageserver_memory_pressure_events_total", "Total memory budget backpressure waits and rejections", uint64(ms.BackpressureWaits+ms.BackpressureRejects))

	return b.String()
}
