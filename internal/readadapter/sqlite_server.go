package readadapter

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Mahadi-rsio/air/internal/tenant"
	"github.com/jackc/pgx/v5"
)

// SQLiteServerConfig configures the shared SQLite read-only engine server.
type SQLiteServerConfig struct {
	ListenHost                    string
	PGPort                        int
	DataDir                       string
	PrimaryConnInfo               string
	CacheKB                       int
	Adapter                       *Adapter
	PageserverSocket        string
	PageserverGRPCAddr      string
	PageserverInternalToken string
	NativeBackend           StorageBackend
	NativeSnapProv                SnapshotProvider
	CacheDir                      string
	L1CacheBytes                  int64
	L2Disabled                    bool
	L2MaxBytes                    int64
	L2MaxEntries                  int
	L2MaxEntryBytes               int64
	L2MaxConcurrentIO             int
	L2BypassUnderPressure         bool
	L2DropPageCache               bool
	L2DirectIO                    bool
	MaxConcurrentQueriesPerTenant int
	ReadersPerTenant              int
	QueryTimeout                  time.Duration
	MaxResultRows                 int64
	GlobalMemoryLimitBytes        int64
	TenantMemoryLimitBytes        int64
	QueryMemoryLimitBytes         int64
	NoisyNeighbor                 NoisyNeighborConfig
}

type sqliteReaderSlot struct {
	conn              *SQLiteConn
	vtabIDs           map[string]uint64
	catalogLSN        uint64
	lastCatalogModLSN uint64
	generation        string
}

// SQLiteTenantStore holds the per-tenant SQLite database files and read/write connections.
type SQLiteTenantStore struct {
	TenantID          string
	Generation        string
	DBPath            string
	Writer            *SQLiteConn
	Reader            *SQLiteConn
	readerPool        chan *sqliteReaderSlot
	readers           []*sqliteReaderSlot
	SyncedLSN         atomic.Uint64
	NativeMode        bool
	vtabIDs           map[string]uint64
	catalogLSN        uint64
	lastCatalogModLSN uint64
	sem          chan struct{}
	queriesTotal atomic.Uint64
	extCols      atomic.Value // map[string]ColumnType
	mu           sync.Mutex
}

// ReadServerMetrics provides structured production diagnostics for SQLiteReadServer.
type ReadServerMetrics struct {
	QueriesTotal        uint64            `json:"queries_total"`
	QueriesSucceeded    uint64            `json:"queries_succeeded"`
	QueriesFailed       uint64            `json:"queries_failed"`
	QueriesCancelled    uint64            `json:"queries_cancelled"`
	WritesBlocked       uint64            `json:"writes_blocked"`
	ActiveQueries       int64             `json:"active_queries"`
	LatencyP50Ms        float64           `json:"latency_p50_ms"`
	LatencyP95Ms        float64           `json:"latency_p95_ms"`
	LatencyP99Ms        float64           `json:"latency_p99_ms"`
	Cache               CacheStats        `json:"cache"`
	Pageserver          PageserverSocketStats `json:"pageserver"`
	ActiveSnapshots     int               `json:"active_snapshots"`
	SnapshotAcqFailures uint64            `json:"snapshot_acq_failures"`
	GlobalLSN           uint64            `json:"global_lsn"`
	ActiveTenants       int               `json:"active_tenants"`
	PerTenantQueries    map[string]uint64 `json:"per_tenant_queries"`
	VTabPagesRead            uint64            `json:"vtab_pages_read"`
	VTabIndexPagesRead       uint64            `json:"vtab_index_pages_read"`
	VTabIndexScans           uint64            `json:"vtab_index_scans"`
	VTabTuplesDecoded        uint64            `json:"vtab_tuples_decoded"`
	VTabColsDecoded          uint64            `json:"vtab_cols_decoded"`
	VTabRowsMatched          uint64            `json:"vtab_rows_matched"`
	ToastFallbacks           uint64            `json:"toast_fallbacks"`
	MultiXactFallbacks       uint64            `json:"multixact_fallbacks"`
	UnsupportedTypeFallbacks uint64            `json:"unsupported_type_fallbacks"`
	TotalFallbacks           uint64             `json:"total_fallbacks"`
	FallbackReasons          map[string]uint64  `json:"fallback_reasons,omitempty"`
	PlanCache                PlanCacheStats     `json:"plan_cache"`
	Memory                   MemoryManagerStats `json:"memory"`
	Scheduler                SchedulerStats     `json:"scheduler"`
	Optimizer                OptimizerStats     `json:"optimizer"`
	Goroutines               int                `json:"goroutines"`
	GoHeapAllocBytes         uint64             `json:"go_heap_alloc_bytes"`
}

// SQLiteReadServer is a shared, multi-tenant SQLite read-only execution server
// that speaks the PostgreSQL v3 wire protocol (TCP and Unix domain socket) and
// enforces strict read-only execution via SQLITE_OPEN_READONLY, PRAGMA query_only=ON,
// and a C-level sqlite3_set_authorizer hook.
type SQLiteReadServer struct {
	cfg     SQLiteServerConfig
	rootDir string
	dbDir   string
	sockDir string

	nativeAdapter   *Adapter
	nativeCLOG      *CLOGResolver
	nativeSock      *PageserverSocketBackend
	nativeGRPC      *PageserverGRPCBackend
	nativeCache     *TieredCacheBackend
	statsCache      *StatsCache
	adaptiveTracker *AdaptiveTracker
	queryOptimizer  *QueryOptimizer
	planCache       *QueryPlanCache
	schemaVersions  *SchemaVersionRegistry
	memMgr          *MemoryManager
	scheduler       *TenantScheduler
	noisyNeighbor   *NoisyNeighborController

	mu      sync.RWMutex
	state   string
	tenants map[string]*SQLiteTenantStore

	sessionsMu sync.Mutex
	sessions   map[uint32]context.CancelFunc

	tcpLn  net.Listener
	unixLn net.Listener
	wg     sync.WaitGroup
	stopCh chan struct{}

	queriesAllowed      atomic.Uint64
	queriesFailed       atomic.Uint64
	queriesCancelled    atomic.Uint64
	activeQueries       atomic.Int64
	writesBlocked            atomic.Uint64
	snapshotAcqFailures      atomic.Uint64
	toastFallbacks           atomic.Uint64
	multixactFallbacks       atomic.Uint64
	unsupportedTypeFallbacks atomic.Uint64
	otherFallbacks           atomic.Uint64
	totalFallbacks           atomic.Uint64
	fallbackTelemetry        FallbackTelemetry
	globalLSN                atomic.Uint64
	connSeq                  atomic.Uint32

	latMu  sync.Mutex
	latBuf [1024]float64
	latIdx int
	latCnt int
}

// NewSQLiteReadServer creates a new shared SQLiteReadServer.
func NewSQLiteReadServer(cfg SQLiteServerConfig) (*SQLiteReadServer, error) {
	if cfg.ListenHost == "" {
		cfg.ListenHost = "127.0.0.1"
	}
	if cfg.CacheKB <= 0 {
		cfg.CacheKB = 32768 // 32 MiB page cache
	}
	if cfg.MaxConcurrentQueriesPerTenant <= 0 {
		cfg.MaxConcurrentQueriesPerTenant = 16
	}
	if cfg.ReadersPerTenant <= 0 {
		cfg.ReadersPerTenant = 4
	}
	if cfg.ReadersPerTenant > cfg.MaxConcurrentQueriesPerTenant {
		cfg.ReadersPerTenant = cfg.MaxConcurrentQueriesPerTenant
	}
	if cfg.QueryTimeout <= 0 {
		cfg.QueryTimeout = 30 * time.Second
	}
	root := filepath.Join(cfg.DataDir, "shared")
	dbDir := filepath.Join(root, "sqlite")
	sockDir := filepath.Join(root, "sock")
	if len(sockDir) > 80 {
		shortSock, err := os.MkdirTemp("/tmp", "air_sq_sock_")
		if err == nil {
			sockDir = shortSock
		}
	}
	if err := os.MkdirAll(dbDir, 0o700); err != nil {
		return nil, fmt.Errorf("mkdir sqlite dbDir: %w", err)
	}
	if err := os.MkdirAll(sockDir, 0o700); err != nil {
		return nil, fmt.Errorf("mkdir sockDir: %w", err)
	}
	s := &SQLiteReadServer{
		cfg:     cfg,
		rootDir: root,
		dbDir:   dbDir,
		sockDir: sockDir,
		state:   "STOPPED",
		tenants:  make(map[string]*SQLiteTenantStore),
		sessions: make(map[uint32]context.CancelFunc),
		stopCh:   make(chan struct{}),
	}
	if cfg.NativeBackend != nil || cfg.PageserverGRPCAddr != "" || cfg.PageserverSocket != "" {
		backend := cfg.NativeBackend
		snapProv := cfg.NativeSnapProv
		if backend == nil && cfg.PageserverGRPCAddr != "" {
			grpcBackend, gErr := NewPageserverGRPCBackend(PageserverGRPCConfig{
				Addr:          cfg.PageserverGRPCAddr,
				InternalToken: cfg.PageserverInternalToken,
			})
			if gErr != nil {
				return nil, fmt.Errorf("init pageserver gRPC backend: %w", gErr)
			}
			s.nativeGRPC = grpcBackend
			backend = grpcBackend
			if snapProv == nil {
				snapProv = grpcBackend
			}
		} else if backend == nil {
			sock := NewPageserverSocketBackend(cfg.PageserverSocket)
			s.nativeSock = sock
			backend = sock
			if snapProv == nil {
				snapProv = sock
			}
		}
		l1Bytes := cfg.L1CacheBytes
		if l1Bytes <= 0 {
			l1Bytes = 64 * 1024 * 1024
		}
		tiered, err := NewTieredCacheBackend(backend, CacheConfig{
			L1MaxBytes:            l1Bytes,
			L2Dir:                 cfg.CacheDir,
			L2Disabled:            cfg.L2Disabled,
			L2MaxBytes:            cfg.L2MaxBytes,
			L2MaxEntries:          cfg.L2MaxEntries,
			L2MaxEntryBytes:       cfg.L2MaxEntryBytes,
			L2MaxConcurrentIO:     cfg.L2MaxConcurrentIO,
			L2BypassUnderPressure: cfg.L2BypassUnderPressure,
			L2DropPageCache:       cfg.L2DropPageCache,
			L2DirectIO:            cfg.L2DirectIO,
		})
		if err != nil {
			return nil, fmt.Errorf("init native tiered cache: %w", err)
		}
		s.nativeCache = tiered
		s.nativeAdapter = NewAdapter(tiered, snapProv)
		s.nativeCLOG = NewCLOGResolver(tiered)
	}
	s.statsCache = NewStatsCache(0)
	s.adaptiveTracker = NewAdaptiveTracker(0)
	s.queryOptimizer = NewQueryOptimizer(DefaultCostModelConfig(), s.statsCache, s.adaptiveTracker, s.nativeCache)
	s.planCache = NewQueryPlanCache(0)
	s.schemaVersions = NewSchemaVersionRegistry()
	s.memMgr = NewMemoryManager(MemoryManagerConfig{
		GlobalLimitBytes:        cfg.GlobalMemoryLimitBytes,
		DefaultTenantLimitBytes: cfg.TenantMemoryLimitBytes,
		DefaultQueryLimitBytes:  cfg.QueryMemoryLimitBytes,
	})
	s.scheduler = NewTenantScheduler(SchedulerConfig{
		MemoryManager: s.memMgr,
	})
	s.noisyNeighbor = NewNoisyNeighborController(cfg.NoisyNeighbor)
	s.globalLSN.Store(0x1000000)
	return s, nil
}

// SockDir returns the Unix socket directory where .s.PGSQL.<port> is bound.
func (s *SQLiteReadServer) SockDir() string {
	return s.sockDir
}

// Port returns the active TCP port (useful when configured with port 0).
func (s *SQLiteReadServer) Port() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.cfg.PGPort
}

// State returns "ACTIVE" or "STOPPED".
func (s *SQLiteReadServer) State() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.state
}

// Stats returns total allowed read queries and blocked write attempts across all tenants.
func (s *SQLiteReadServer) Stats() (allowed uint64, blocked uint64) {
	return s.queriesAllowed.Load(), s.writesBlocked.Load()
}

func (s *SQLiteReadServer) recordLatency(d time.Duration) {
	ms := float64(d.Microseconds()) / 1000.0
	s.latMu.Lock()
	s.latBuf[s.latIdx] = ms
	s.latIdx = (s.latIdx + 1) % len(s.latBuf)
	if s.latCnt < len(s.latBuf) {
		s.latCnt++
	}
	s.latMu.Unlock()
}

// DetailedMetrics returns comprehensive query, cache, pageserver, consistency, and resource metrics.
func (s *SQLiteReadServer) DetailedMetrics() ReadServerMetrics {
	succeeded := s.queriesAllowed.Load()
	failed := s.queriesFailed.Load()
	cancelled := s.queriesCancelled.Load()

	s.latMu.Lock()
	n := s.latCnt
	samples := make([]float64, n)
	copy(samples, s.latBuf[:n])
	s.latMu.Unlock()

	var p50, p95, p99 float64
	if n > 0 {
		sort.Float64s(samples)
		p50 = samples[(n-1)*50/100]
		p95 = samples[(n-1)*95/100]
		p99 = samples[(n-1)*99/100]
	}

	s.mu.RLock()
	perTenant := make(map[string]uint64, len(s.tenants))
	for id, ts := range s.tenants {
		perTenant[id] = ts.queriesTotal.Load()
	}
	activeTenants := len(s.tenants)
	s.mu.RUnlock()

	var cacheStats CacheStats
	if s.nativeCache != nil {
		cacheStats = s.nativeCache.Stats()
	}
	var psStats PageserverSocketStats
	if s.nativeGRPC != nil {
		psStats = s.nativeGRPC.Stats()
	} else if s.nativeSock != nil {
		psStats = s.nativeSock.Stats()
	}
	var activeSnaps int
	if s.nativeAdapter != nil {
		activeSnaps = s.nativeAdapter.ActiveSnapshotCount()
	}
	pagesRead, tuplesDecoded, colsDecoded, rowsMatched := VTabStats()
	idxPagesRead, idxScans := VTabIndexStats()

	toastFB := s.toastFallbacks.Load()
	mxFB := s.multixactFallbacks.Load()
	utFB := s.unsupportedTypeFallbacks.Load()
	totalFB := s.totalFallbacks.Load()
	var fbReasons map[string]uint64
	if totalFB > 0 {
		fbReasons = s.fallbackTelemetry.ReasonCounts()
	}

	var planCacheStats PlanCacheStats
	if s.planCache != nil {
		planCacheStats = s.planCache.Stats()
	}
	var memStats MemoryManagerStats
	if s.memMgr != nil {
		memStats = s.memMgr.Stats()
	}
	var schedStats SchedulerStats
	if s.scheduler != nil {
		schedStats = s.scheduler.Stats()
	}
	var optStats OptimizerStats
	if s.queryOptimizer != nil {
		optStats = s.queryOptimizer.Stats()
	}
	var rms runtime.MemStats
	runtime.ReadMemStats(&rms)

	return ReadServerMetrics{
		QueriesTotal:             succeeded + failed + cancelled,
		QueriesSucceeded:         succeeded,
		QueriesFailed:            failed,
		QueriesCancelled:         cancelled,
		WritesBlocked:            s.writesBlocked.Load(),
		ActiveQueries:            s.activeQueries.Load(),
		LatencyP50Ms:             p50,
		LatencyP95Ms:             p95,
		LatencyP99Ms:             p99,
		Cache:                    cacheStats,
		Pageserver:               psStats,
		ActiveSnapshots:          activeSnaps,
		SnapshotAcqFailures:      s.snapshotAcqFailures.Load(),
		GlobalLSN:                s.globalLSN.Load(),
		ActiveTenants:            activeTenants,
		PerTenantQueries:         perTenant,
		VTabPagesRead:            pagesRead,
		VTabIndexPagesRead:       idxPagesRead,
		VTabIndexScans:           idxScans,
		VTabTuplesDecoded:        tuplesDecoded,
		VTabColsDecoded:          colsDecoded,
		VTabRowsMatched:          rowsMatched,
		ToastFallbacks:           toastFB,
		MultiXactFallbacks:       mxFB,
		UnsupportedTypeFallbacks: utFB,
		TotalFallbacks:           totalFB,
		FallbackReasons:          fbReasons,
		PlanCache:                planCacheStats,
		Memory:                   memStats,
		Scheduler:                schedStats,
		Optimizer:                optStats,
		Goroutines:               runtime.NumGoroutine(),
		GoHeapAllocBytes:         rms.HeapAlloc,
	}
}

// PrometheusMetrics renders low-cardinality Prometheus exposition metrics for AIR's
// read engine without unbounded per-tenant labels.
func (s *SQLiteReadServer) PrometheusMetrics() string {
	m := s.DetailedMetrics()
	var b strings.Builder
	fmt.Fprintf(&b, "# TYPE air_read_queries_total counter\n")
	fmt.Fprintf(&b, "air_read_queries_total{status=\"succeeded\"} %d\n", m.QueriesSucceeded)
	fmt.Fprintf(&b, "air_read_queries_total{status=\"failed\"} %d\n", m.QueriesFailed)
	fmt.Fprintf(&b, "air_read_queries_total{status=\"cancelled\"} %d\n", m.QueriesCancelled)
	fmt.Fprintf(&b, "air_read_active_queries %d\n", m.ActiveQueries)
	fmt.Fprintf(&b, "air_read_queued_queries %d\n", m.Scheduler.QueuedGlobal)
	fmt.Fprintf(&b, "air_read_latency_ms{quantile=\"0.5\"} %.3f\n", m.LatencyP50Ms)
	fmt.Fprintf(&b, "air_read_latency_ms{quantile=\"0.95\"} %.3f\n", m.LatencyP95Ms)
	fmt.Fprintf(&b, "air_read_latency_ms{quantile=\"0.99\"} %.3f\n", m.LatencyP99Ms)
	fmt.Fprintf(&b, "air_read_plan_cache_hits_total %d\n", m.PlanCache.Hits)
	fmt.Fprintf(&b, "air_read_plan_cache_misses_total %d\n", m.PlanCache.Misses)
	fmt.Fprintf(&b, "air_read_plan_cache_invalidations_total %d\n", m.PlanCache.Invalidations)
	fmt.Fprintf(&b, "air_read_optimizer_plans_total{strategy=\"seq\"} %d\n", m.Optimizer.SeqScansChosen)
	fmt.Fprintf(&b, "air_read_optimizer_plans_total{strategy=\"index\"} %d\n", m.Optimizer.IndexScansChosen)
	fmt.Fprintf(&b, "air_read_optimizer_plans_total{strategy=\"index_ordered\"} %d\n", m.Optimizer.OrderedIndexScansChosen)
	fmt.Fprintf(&b, "air_read_vtab_rows_matched_total %d\n", m.VTabRowsMatched)
	fmt.Fprintf(&b, "air_read_exec_memory_used_bytes %d\n", m.Memory.GlobalUsedBytes)
	fmt.Fprintf(&b, "air_read_sqlite_memory_used_bytes %d\n", m.Memory.SQLiteUsedBytes)
	fmt.Fprintf(&b, "air_read_go_heap_alloc_bytes %d\n", m.GoHeapAllocBytes)
	fmt.Fprintf(&b, "air_read_fallbacks_total %d\n", m.TotalFallbacks)
	fmt.Fprintf(&b, "air_native_read_queries_total %d\n", m.QueriesSucceeded)
	fmt.Fprintf(&b, "air_postgres_fallback_queries_total %d\n", m.TotalFallbacks)
	s.fallbackTelemetry.WritePrometheus(&b)
	var cs CacheStats
	if s.nativeCache != nil {
		cs = s.nativeCache.Stats()
	}
	fmt.Fprintf(&b, "air_cache_l1_hits_total %d\n", cs.L1Hits)
	fmt.Fprintf(&b, "air_cache_l2_hits_total %d\n", cs.L2Hits)
	fmt.Fprintf(&b, "air_cache_misses_total %d\n", cs.L1Misses)
	fmt.Fprintf(&b, "air_cache_evictions_total %d\n", cs.L1Evictions+cs.L2Evictions)
	fmt.Fprintf(&b, "air_l2_cache_bytes %d\n", cs.L2Bytes)
	fmt.Fprintf(&b, "air_l2_cache_entries %d\n", cs.L2Entries)
	fmt.Fprintf(&b, "air_tenant_memory_bytes %d\n", m.Memory.GlobalUsedBytes)
	fmt.Fprintf(&b, "air_cgroup_memory_bytes %d\n", readCgroupMemoryCurrent())
	return b.String()
}

func readCgroupMemoryCurrent() int64 {
	raw, err := os.ReadFile("/sys/fs/cgroup/memory.current")
	if err != nil {
		return 0
	}
	v, err := strconv.ParseInt(strings.TrimSpace(string(raw)), 10, 64)
	if err != nil {
		return 0
	}
	return v
}

// RankedFallbacks returns observed PostgreSQL fallbacks ranked by descending frequency.
func (s *SQLiteReadServer) RankedFallbacks() []FallbackRankEntry {
	return s.fallbackTelemetry.RankedFallbacks()
}

func (s *SQLiteReadServer) recordQueryError(err error) {
	s.recordQueryErrorWithSQL("", err)
}

func (s *SQLiteReadServer) recordQueryErrorWithSQL(sql string, err error) {
	if err == nil {
		return
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return
	}
	if errors.Is(err, ErrTOASTUnsupported) {
		s.toastFallbacks.Add(1)
		s.totalFallbacks.Add(1)
	} else if errors.Is(err, ErrMultiXactUnsupported) {
		s.multixactFallbacks.Add(1)
		s.totalFallbacks.Add(1)
	} else if errors.Is(err, ErrUnsupportedType) {
		s.unsupportedTypeFallbacks.Add(1)
		s.totalFallbacks.Add(1)
	} else {
		s.otherFallbacks.Add(1)
		s.totalFallbacks.Add(1)
	}
	shape := ClassifyQueryShape(sql)
	reason := ClassifyFallbackReason(sql, err)
	s.fallbackTelemetry.RecordFallback(shape, reason)
}

// Tenants returns the list of registered tenant IDs.
func (s *SQLiteReadServer) Tenants() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, 0, len(s.tenants))
	for id := range s.tenants {
		out = append(out, id)
	}
	return out
}

// SetGlobalLSN updates the default LSN reported when a tenant has no specific LSN recorded.
func (s *SQLiteReadServer) SetGlobalLSN(lsn uint64) {
	s.globalLSN.Store(lsn)
}

// Start binds the TCP and Unix domain socket listeners and begins serving PG v3 connections.
func (s *SQLiteReadServer) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state == "ACTIVE" {
		return nil
	}

	tcpAddr := fmt.Sprintf("%s:%d", s.cfg.ListenHost, s.cfg.PGPort)
	tcpLn, err := net.Listen("tcp", tcpAddr)
	if err != nil {
		return fmt.Errorf("sqlite read server listen tcp %s: %w", tcpAddr, err)
	}
	s.tcpLn = tcpLn
	s.cfg.PGPort = tcpLn.Addr().(*net.TCPAddr).Port

	sockPath := filepath.Join(s.sockDir, fmt.Sprintf(".s.PGSQL.%d", s.cfg.PGPort))
	_ = os.Remove(sockPath)
	unixLn, err := net.Listen("unix", sockPath)
	if err != nil {
		_ = tcpLn.Close()
		return fmt.Errorf("sqlite read server listen unix %s: %w", sockPath, err)
	}
	_ = os.Chmod(sockPath, 0o777)
	s.unixLn = unixLn

	// Pre-create default 'postgres' database so admin/health connections work immediately.
	if _, err := s.ensureTenantLocked("postgres"); err != nil {
		_ = tcpLn.Close()
		_ = unixLn.Close()
		return err
	}

	s.state = "ACTIVE"
	s.wg.Add(2)
	go s.acceptLoop(tcpLn)
	go s.acceptLoop(unixLn)
	return nil
}

// Stop closes listeners and all open tenant SQLite handles.
func (s *SQLiteReadServer) Stop() error {
	s.mu.Lock()
	if s.state != "ACTIVE" {
		s.mu.Unlock()
		return nil
	}
	s.state = "STOPPED"
	close(s.stopCh)
	if s.tcpLn != nil {
		_ = s.tcpLn.Close()
	}
	if s.unixLn != nil {
		_ = s.unixLn.Close()
	}
	tenants := make([]*SQLiteTenantStore, 0, len(s.tenants))
	for _, ts := range s.tenants {
		tenants = append(tenants, ts)
	}
	s.tenants = make(map[string]*SQLiteTenantStore)
	s.mu.Unlock()

	s.wg.Wait()
	for _, ts := range tenants {
		for _, vid := range ts.vtabIDs {
			UnregisterNativeTable(vid)
		}
		for _, slot := range ts.readers {
			for _, vid := range slot.vtabIDs {
				UnregisterNativeTable(vid)
			}
			if slot.conn != nil {
				_ = slot.conn.Close()
			}
		}
		if len(ts.readers) == 0 && ts.Reader != nil {
			_ = ts.Reader.Close()
		}
		if ts.Writer != nil {
			_ = ts.Writer.Close()
		}
	}
	if s.nativeSock != nil {
		_ = s.nativeSock.Close()
	}
	if s.nativeGRPC != nil {
		_ = s.nativeGRPC.Close()
	}
	return nil
}

// EnsureTenantDB initializes or returns the per-tenant SQLite database store.
func (s *SQLiteReadServer) EnsureTenantDB(tenantID string) error {
	if tenantID == "" {
		return ErrInvalidTenant
	}
	if tenantID != "postgres" {
		if err := tenant.ValidateID(tenantID); err != nil {
			return fmt.Errorf("%w: %v", ErrInvalidTenant, err)
		}
	}
	s.mu.Lock()
	ts, err := s.ensureTenantLocked(tenantID)
	s.mu.Unlock()
	if err != nil {
		return err
	}
	if s.cfg.PrimaryConnInfo != "" && tenantID != "postgres" {
		_ = s.SyncFromPrimary(context.Background(), tenantID, s.cfg.PrimaryConnInfo)
	}
	_ = ts
	return nil
}

// DeleteTenantDB closes and removes a tenant namespace from the shared SQLite read engine.
func (s *SQLiteReadServer) DeleteTenantDB(tenantID string) error {
	if tenantID == "" {
		return ErrInvalidTenant
	}
	s.mu.Lock()
	ts, ok := s.tenants[tenantID]
	if ok {
		delete(s.tenants, tenantID)
	}
	s.mu.Unlock()
	if !ok || ts == nil {
		return nil
	}
	for _, vid := range ts.vtabIDs {
		UnregisterNativeTable(vid)
	}
	for _, slot := range ts.readers {
		for _, vid := range slot.vtabIDs {
			UnregisterNativeTable(vid)
		}
		if slot.conn != nil {
			_ = slot.conn.Close()
		}
	}
	if len(ts.readers) == 0 && ts.Reader != nil {
		_ = ts.Reader.Close()
	}
	if ts.Writer != nil {
		_ = ts.Writer.Close()
	}
	if ts.DBPath != "" && ts.DBPath != ":memory:" {
		_ = os.Remove(ts.DBPath)
		_ = os.Remove(ts.DBPath + "-wal")
		_ = os.Remove(ts.DBPath + "-shm")
	}
	return nil
}

func (s *SQLiteReadServer) ensureTenantLocked(tenantID string) (*SQLiteTenantStore, error) {
	if ts, ok := s.tenants[tenantID]; ok {
		return ts, nil
	}
	if s.nativeAdapter != nil && tenantID != "postgres" {
		n := s.cfg.ReadersPerTenant
		if n <= 0 {
			n = 4
		}
		pool := make(chan *sqliteReaderSlot, n)
		slots := make([]*sqliteReaderSlot, 0, n)
		for i := 0; i < n; i++ {
			conn, err := OpenSQLiteConn(":memory:", false, s.cfg.CacheKB)
			if err != nil {
				for _, sl := range slots {
					_ = sl.conn.Close()
				}
				return nil, err
			}
			if err := conn.RegisterAirPGModule(); err != nil {
				_ = conn.Close()
				for _, sl := range slots {
					_ = sl.conn.Close()
				}
				return nil, err
			}
			conn.readOnly = true
			conn.SetReadOnlyAuthorizer(true)
			sl := &sqliteReaderSlot{
				conn:    conn,
				vtabIDs: make(map[string]uint64),
			}
			slots = append(slots, sl)
			pool <- sl
		}
		ts := &SQLiteTenantStore{
			TenantID:   tenantID,
			DBPath:     ":memory:",
			Reader:     slots[0].conn,
			readerPool: pool,
			readers:    slots,
			NativeMode: true,
			vtabIDs:    make(map[string]uint64),
			sem:        make(chan struct{}, s.cfg.MaxConcurrentQueriesPerTenant),
		}
		s.tenants[tenantID] = ts
		return ts, nil
	}
	dbPath := filepath.Join(s.dbDir, tenantID+".sqlite3")
	writer, err := OpenSQLiteConn(dbPath, false, s.cfg.CacheKB)
	if err != nil {
		return nil, err
	}
	initSQL := `
CREATE TABLE IF NOT EXISTS _air_meta (
	key TEXT PRIMARY KEY,
	val TEXT NOT NULL
);
INSERT OR IGNORE INTO _air_meta (key, val) VALUES ('lsn', '0/1000000');
`
	if err := writer.Exec(initSQL); err != nil {
		_ = writer.Close()
		return nil, fmt.Errorf("init _air_meta: %w", err)
	}
	reader, err := OpenSQLiteConn(dbPath, true, s.cfg.CacheKB)
	if err != nil {
		_ = writer.Close()
		return nil, err
	}
	ts := &SQLiteTenantStore{
		TenantID: tenantID,
		DBPath:   dbPath,
		Writer:   writer,
		Reader:   reader,
	}
	ts.SyncedLSN.Store(s.globalLSN.Load())
	s.tenants[tenantID] = ts
	return ts, nil
}

func (s *SQLiteReadServer) getOrCreateTenant(tenantID string) (*SQLiteTenantStore, error) {
	if tenantID == "" {
		tenantID = "postgres"
	}
	if tenantID != "postgres" {
		if err := tenant.ValidateID(tenantID); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrInvalidTenant, err)
		}
	}
	s.mu.RLock()
	ts, ok := s.tenants[tenantID]
	s.mu.RUnlock()
	if ok {
		return ts, nil
	}
	s.mu.Lock()
	ts, err := s.ensureTenantLocked(tenantID)
	s.mu.Unlock()
	return ts, err
}

// ExecInternal runs administrative DDL/DML on the tenant's privileged writer connection
// (used for seeding tests or syncing committed pages/tables from the Write Engine).
func (s *SQLiteReadServer) ExecInternal(tenantID, sql string, lsn uint64) error {
	ts, err := s.getOrCreateTenant(tenantID)
	if err != nil {
		return err
	}
	ts.mu.Lock()
	defer ts.mu.Unlock()
	if err := ts.Writer.Exec(sql); err != nil {
		return err
	}
	if lsn > 0 {
		ts.SyncedLSN.Store(lsn)
		lsnStr := FormatPGLSN(lsn)
		_ = ts.Writer.Exec(fmt.Sprintf("INSERT OR REPLACE INTO _air_meta (key, val) VALUES ('lsn', '%s');", lsnStr))
	}
	return nil
}

// MaterializeRelation decodes all visible PG17 heap tuples from an open readadapter.Adapter
// relation at snap into a SQLite table inside tenantID's SQLite database, updating its LSN.
func (s *SQLiteReadServer) MaterializeRelation(
	ctx context.Context,
	adapter *Adapter,
	rel *RelationHandle,
	snap *ReadSnapshot,
	resolver *CLOGResolver,
	tableName string,
	colNames []string,
	colTypes []ColumnType,
) (int, error) {
	if adapter == nil || rel == nil || snap == nil {
		return 0, fmt.Errorf("readadapter: adapter, relation, and snapshot are required")
	}
	if len(colNames) != len(colTypes) || len(colNames) == 0 {
		return 0, fmt.Errorf("readadapter: colNames and colTypes length mismatch")
	}
	meta, err := adapter.GetRelationMetadata(ctx, rel, snap)
	if err != nil {
		return 0, err
	}

	ts, err := s.getOrCreateTenant(snap.TenantID)
	if err != nil {
		return 0, err
	}
	ts.mu.Lock()
	defer ts.mu.Unlock()

	var colDefs []string
	for i, name := range colNames {
		sqType := "TEXT"
		switch colTypes[i] {
		case ColInt4:
			sqType = "INT4"
		case ColInt8:
			sqType = "INT8"
		case ColFloat8:
			sqType = "FLOAT8"
		case ColText:
			sqType = "TEXT"
		}
		colDefs = append(colDefs, fmt.Sprintf("%q %s", name, sqType))
	}

	var sb strings.Builder
	sb.WriteString("BEGIN;\n")
	sb.WriteString(fmt.Sprintf("DROP TABLE IF EXISTS %q;\n", tableName))
	sb.WriteString(fmt.Sprintf("CREATE TABLE %q (%s);\n", tableName, strings.Join(colDefs, ", ")))

	rowCount := 0
	if meta.Exists && meta.NBlocks > 0 {
		pages, err := adapter.ReadPages(ctx, rel, snap, 0, meta.NBlocks)
		if err != nil {
			return 0, err
		}
		for _, pg := range pages {
			hdr, err := ParsePageHeader(pg.Data)
			if err != nil {
				continue
			}
			lps := ParseLinePointers(pg.Data, hdr)
			for _, lp := range lps {
				if lp.Flags != LPNormal || lp.Length == 0 {
					continue
				}
				if int(lp.Offset)+int(lp.Length) > len(pg.Data) {
					continue
				}
				tup := pg.Data[lp.Offset : lp.Offset+lp.Length]
				thdr, err := ParseHeapTupleHeader(tup)
				if err != nil {
					continue
				}
				var vis bool
				if resolver != nil {
					vis, err = IsVisibleWithCLOG(ctx, thdr, snap, resolver)
					if err != nil {
						return 0, err
					}
				} else {
					vis = thdr.IsVisible(snap, nil)
				}
				if !vis {
					continue
				}
				vals, err := DecodeHeapColumns(tup, thdr, colTypes)
				if err != nil {
					continue
				}
				var litVals []string
				for _, v := range vals {
					if v == nil {
						litVals = append(litVals, "NULL")
					} else {
						switch tv := v.(type) {
						case int32:
							litVals = append(litVals, strconv.FormatInt(int64(tv), 10))
						case int64:
							litVals = append(litVals, strconv.FormatInt(tv, 10))
						case float64:
							litVals = append(litVals, strconv.FormatFloat(tv, 'g', -1, 64))
						case string:
							litVals = append(litVals, "'"+strings.ReplaceAll(tv, "'", "''")+"'")
						default:
							litVals = append(litVals, "'"+strings.ReplaceAll(fmt.Sprint(tv), "'", "''")+"'")
						}
					}
				}
				sb.WriteString(fmt.Sprintf("INSERT INTO %q VALUES (%s);\n", tableName, strings.Join(litVals, ", ")))
				rowCount++
			}
		}
	}

	if snap.LSN > 0 {
		lsnStr := FormatPGLSN(snap.LSN)
		sb.WriteString(fmt.Sprintf("INSERT OR REPLACE INTO _air_meta (key, val) VALUES ('lsn', '%s');\n", lsnStr))
	}
	sb.WriteString("COMMIT;")

	if err := ts.Writer.Exec(sb.String()); err != nil {
		_ = ts.Writer.Exec("ROLLBACK;")
		return 0, err
	}
	if snap.LSN > 0 {
		ts.SyncedLSN.Store(snap.LSN)
	}
	return rowCount, nil
}

// SyncFromPrimary connects to a PostgreSQL primary write engine for tenantID,
// reads public user tables and pg_current_wal_lsn(), and syncs them into the tenant's SQLite store.
func (s *SQLiteReadServer) SyncFromPrimary(ctx context.Context, tenantID, baseConnInfo string) error {
	ts, err := s.getOrCreateTenant(tenantID)
	if err != nil {
		return err
	}
	connCfg, err := pgx.ParseConfig(baseConnInfo)
	if err != nil {
		return err
	}
	if tenantID != "" && tenantID != "postgres" {
		connCfg.Database = tenantID
	}
	conn, err := pgx.ConnectConfig(ctx, connCfg)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	var lsnText string
	if err := conn.QueryRow(ctx, "SELECT pg_current_wal_lsn()::text").Scan(&lsnText); err != nil {
		return err
	}
	lsnVal := ParsePGLSN(lsnText)

	rows, err := conn.Query(ctx, `
		SELECT table_name
		FROM information_schema.tables
		WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
	`)
	if err != nil {
		return err
	}
	var tables []string
	for rows.Next() {
		var tname string
		if err := rows.Scan(&tname); err == nil {
			tables = append(tables, tname)
		}
	}
	rows.Close()

	var sb strings.Builder
	sb.WriteString("BEGIN;\n")

	for _, tname := range tables {
		colRows, err := conn.Query(ctx, `
			SELECT column_name, data_type
			FROM information_schema.columns
			WHERE table_schema = 'public' AND table_name = $1
			ORDER BY ordinal_position
		`, tname)
		if err != nil {
			continue
		}
		var colDefs []string
		for colRows.Next() {
			var cname, dtype string
			if err := colRows.Scan(&cname, &dtype); err == nil {
				colDefs = append(colDefs, fmt.Sprintf("%q %s", cname, pgTypeToSQLiteDecl(dtype)))
			}
		}
		colRows.Close()
		if len(colDefs) == 0 {
			continue
		}

		sb.WriteString(fmt.Sprintf("DROP TABLE IF EXISTS %q;\n", tname))
		sb.WriteString(fmt.Sprintf("CREATE TABLE %q (%s);\n", tname, strings.Join(colDefs, ", ")))

		dataRows, err := conn.Query(ctx, fmt.Sprintf("SELECT * FROM %q", tname))
		if err != nil {
			continue
		}
		for dataRows.Next() {
			vals, err := dataRows.Values()
			if err != nil {
				continue
			}
			var lits []string
			for _, v := range vals {
				if v == nil {
					lits = append(lits, "NULL")
				} else {
					switch tv := v.(type) {
					case bool:
						if tv {
							lits = append(lits, "1")
						} else {
							lits = append(lits, "0")
						}
					case int16, int32, int64, int, uint16, uint32, uint64:
						lits = append(lits, fmt.Sprintf("%d", tv))
					case float32, float64:
						lits = append(lits, fmt.Sprintf("%g", tv))
					case []byte:
						lits = append(lits, "'"+strings.ReplaceAll(string(tv), "'", "''")+"'")
					default:
						lits = append(lits, "'"+strings.ReplaceAll(fmt.Sprint(tv), "'", "''")+"'")
					}
				}
			}
			sb.WriteString(fmt.Sprintf("INSERT INTO %q VALUES (%s);\n", tname, strings.Join(lits, ", ")))
		}
		dataRows.Close()
	}

	sb.WriteString(fmt.Sprintf("INSERT OR REPLACE INTO _air_meta (key, val) VALUES ('lsn', '%s');\n", lsnText))
	sb.WriteString("COMMIT;")

	ts.mu.Lock()
	defer ts.mu.Unlock()
	if err := ts.Writer.Exec(sb.String()); err != nil {
		_ = ts.Writer.Exec("ROLLBACK;")
		return err
	}
	if lsnVal > 0 {
		ts.SyncedLSN.Store(lsnVal)
	}
	return nil
}

func pgTypeToSQLiteDecl(pgType string) string {
	u := strings.ToUpper(pgType)
	switch {
	case strings.Contains(u, "BIGINT") || u == "INT8":
		return "INT8"
	case strings.Contains(u, "INT"):
		return "INT4"
	case strings.Contains(u, "BOOL"):
		return "BOOLEAN"
	case strings.Contains(u, "DOUBLE") || strings.Contains(u, "REAL") || strings.Contains(u, "FLOAT"):
		return "FLOAT8"
	case strings.Contains(u, "BYTEA"):
		return "BYTEA"
	default:
		return "TEXT"
	}
}

// FormatPGLSN formats a 64-bit WAL LSN as PostgreSQL "X/YYYYYYYY" text.
func FormatPGLSN(lsn uint64) string {
	return fmt.Sprintf("%X/%08X", uint32(lsn>>32), uint32(lsn&0xFFFFFFFF))
}

// ParsePGLSN parses a PostgreSQL "X/YYYYYYYY" LSN string into a uint64.
func ParsePGLSN(s string) uint64 {
	parts := strings.Split(strings.TrimSpace(s), "/")
	if len(parts) != 2 {
		return 0
	}
	hi, err1 := strconv.ParseUint(parts[0], 16, 32)
	lo, err2 := strconv.ParseUint(parts[1], 16, 32)
	if err1 != nil || err2 != nil {
		return 0
	}
	return (hi << 32) | lo
}

func (s *SQLiteReadServer) acceptLoop(ln net.Listener) {
	defer s.wg.Done()
	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-s.stopCh:
				return
			default:
				continue
			}
		}
		s.wg.Add(1)
		go func(c net.Conn) {
			defer s.wg.Done()
			defer c.Close()
			s.serveConn(c)
		}(conn)
	}
}

type sqlitePreparedStmt struct {
	Name          string
	OrigSQL       string
	SQLiteSQL     string
	ParamCount    int
	ParamOIDs     []uint32
	SchemaVersion uint64
	Generation    string
	SearchPath    string
}

type sqlitePortal struct {
	Name      string
	Stmt      *sqlitePreparedStmt
	Args      []string
	Result    *SQLiteResult
	Evaluated bool
}

type sqliteClientSession struct {
	srv         *SQLiteReadServer
	ctx         context.Context
	conn        net.Conn
	tenantID    string
	user        string
	txStatus    byte
	sessionVars map[string]string
	stmts       map[string]*sqlitePreparedStmt
	portals     map[string]*sqlitePortal
}

func (sess *sqliteClientSession) sessionCtx() context.Context {
	if sess.ctx != nil {
		return sess.ctx
	}
	return context.Background()
}

func (s *SQLiteReadServer) serveConn(c net.Conn) {
	connCtx, connCancel := context.WithCancel(context.Background())
	defer connCancel()
	go func() {
		select {
		case <-s.stopCh:
			connCancel()
		case <-connCtx.Done():
		}
	}()

	params, ok := s.performHandshake(c)
	if !ok {
		return
	}
	dbName := params["database"]
	if dbName == "" {
		dbName = params["user"]
	}
	if dbName == "" {
		dbName = "postgres"
	}

	if _, err := s.getOrCreateTenant(dbName); err != nil {
		_ = writePGError(c, "FATAL", "3D000", fmt.Sprintf("database %q does not exist: %v", dbName, err))
		return
	}

	pid := 20000 + s.connSeq.Add(1)
	s.sessionsMu.Lock()
	s.sessions[pid] = connCancel
	s.sessionsMu.Unlock()
	defer func() {
		s.sessionsMu.Lock()
		delete(s.sessions, pid)
		s.sessionsMu.Unlock()
	}()
	var out bytes.Buffer
	writePGAuthOk(&out)
	writePGParamStatus(&out, "server_version", "17.12 (AIR SQLite Read Engine "+SQLiteVersion()+")")
	writePGParamStatus(&out, "client_encoding", "UTF8")
	writePGParamStatus(&out, "DateStyle", "ISO, MDY")
	writePGParamStatus(&out, "standard_conforming_strings", "on")
	writePGParamStatus(&out, "default_transaction_read_only", "on")
	writePGParamStatus(&out, "in_hot_standby", "on")
	writePGBackendKeyData(&out, pid, 0x51717E01)
	writePGReadyForQuery(&out, 'I')
	if _, err := c.Write(out.Bytes()); err != nil {
		return
	}

	sess := &sqliteClientSession{
		srv:      s,
		ctx:      connCtx,
		conn:     c,
		tenantID: dbName,
		user:     params["user"],
		txStatus: 'I',
		sessionVars: map[string]string{
			"default_transaction_read_only": "on",
			"transaction_read_only":         "on",
			"work_mem":                      "4MB",
			"search_path":                   "public",
			"client_encoding":               "UTF8",
			"datestyle":                     "ISO, MDY",
			"standard_conforming_strings":   "on",
			"air_ro.enabled":                "on",
		},
		stmts:   make(map[string]*sqlitePreparedStmt),
		portals: make(map[string]*sqlitePortal),
	}

	for {
		mtype, body, err := readPGMsg(c)
		if err != nil {
			return
		}
		switch mtype {
		case 'X': // Terminate
			return
		case 'Q': // SimpleQuery
			sql := readCString(body, 0)
			if !sess.handleSimpleQuery(sql) {
				return
			}
		case 'P': // Parse
			if !sess.handleParse(body) {
				return
			}
		case 'B': // Bind
			if !sess.handleBind(body) {
				return
			}
		case 'D': // Describe
			if !sess.handleDescribe(body) {
				return
			}
		case 'E': // Execute
			if !sess.handleExecute(body) {
				return
			}
		case 'C': // Close
			if !sess.handleClose(body) {
				return
			}
		case 'H': // Flush
			// All responses are written immediately to conn.
		case 'S': // Sync
			if sess.txStatus == 'E' {
				sess.txStatus = 'I'
			}
			var z bytes.Buffer
			writePGReadyForQuery(&z, sess.txStatus)
			if _, err := c.Write(z.Bytes()); err != nil {
				return
			}
		default:
			var errBuf bytes.Buffer
			writePGErrorBuf(&errBuf, "ERROR", "08P01", fmt.Sprintf("unsupported message type %q", mtype))
			writePGReadyForQuery(&errBuf, sess.txStatus)
			if _, err := c.Write(errBuf.Bytes()); err != nil {
				return
			}
		}
	}
}

func (s *SQLiteReadServer) performHandshake(c net.Conn) (map[string]string, bool) {
	for {
		var lenBuf [4]byte
		if _, err := io.ReadFull(c, lenBuf[:]); err != nil {
			return nil, false
		}
		msgLen := binary.BigEndian.Uint32(lenBuf[:])
		if msgLen < 8 || msgLen > 8192 {
			return nil, false
		}
		body := make([]byte, msgLen-4)
		if _, err := io.ReadFull(c, body); err != nil {
			return nil, false
		}
		code := binary.BigEndian.Uint32(body[:4])
		if code == 80877103 || code == 80877104 {
			// SSLRequest / GSSENCRequest -> decline with 'N'
			if _, err := c.Write([]byte{'N'}); err != nil {
				return nil, false
			}
			continue
		}
		if code == 80877102 {
			// CancelRequest: body has [4:8]=pid, [8:12]=secret
			if len(body) >= 12 {
				targetPID := binary.BigEndian.Uint32(body[4:8])
				s.sessionsMu.Lock()
				cancelFn := s.sessions[targetPID]
				s.sessionsMu.Unlock()
				if cancelFn != nil {
					cancelFn()
				}
			}
			return nil, false
		}
		if code != 196608 {
			return nil, false
		}
		params := make(map[string]string)
		rest := body[4:]
		i := 0
		for i < len(rest) && rest[i] != 0 {
			kEnd := bytes.IndexByte(rest[i:], 0)
			if kEnd < 0 {
				break
			}
			key := string(rest[i : i+kEnd])
			i += kEnd + 1
			if i >= len(rest) {
				break
			}
			vEnd := bytes.IndexByte(rest[i:], 0)
			if vEnd < 0 {
				break
			}
			val := string(rest[i : i+vEnd])
			i += vEnd + 1
			params[key] = val
		}
		return params, true
	}
}

func (sess *sqliteClientSession) handleSimpleQuery(rawSQL string) bool {
	var out bytes.Buffer
	trimmed := strings.TrimSpace(rawSQL)
	if trimmed == "" {
		writePGEmptyQuery(&out)
		writePGReadyForQuery(&out, sess.txStatus)
		_, err := sess.conn.Write(out.Bytes())
		return err == nil
	}

	stmts := splitSQLStatements(trimmed)
	if len(stmts) == 0 {
		writePGEmptyQuery(&out)
		writePGReadyForQuery(&out, sess.txStatus)
		_, err := sess.conn.Write(out.Bytes())
		return err == nil
	}

	for _, stmt := range stmts {
		if err := sess.execSingleStatement(stmt, nil, &out); err != nil {
			var roErr *SQLiteReadOnlyError
			sqlState := "42000"
			if errors.As(err, &roErr) {
				sqlState = "25006" // ERRCODE_READ_ONLY_SQL_TRANSACTION
			}
			writePGErrorBuf(&out, "ERROR", sqlState, err.Error())
			if sess.txStatus == 'T' {
				sess.txStatus = 'E'
			}
			break
		}
	}

	writePGReadyForQuery(&out, sess.txStatus)
	_, err := sess.conn.Write(out.Bytes())
	return err == nil
}

var (
	reDollarParam = regexp.MustCompile(`\$(\d+)`)
	reTypeCast    = regexp.MustCompile(`::[a-zA-Z_][a-zA-Z0-9_]*(\[\])?`)
	rePrepareSQL  = regexp.MustCompile(`(?is)^PREPARE\s+([a-zA-Z0-9_]+)\s*(?:\([^)]*\))?\s+AS\s+(.+)$`)
	reExecuteSQL  = regexp.MustCompile(`(?is)^EXECUTE\s+([a-zA-Z0-9_]+)\s*(?:\((.*)\))?$`)
	reDeallocSQL  = regexp.MustCompile(`(?is)^DEALLOCATE\s+(?:PREPARE\s+)?([a-zA-Z0-9_]+)$`)
	reCopyToSQL   = regexp.MustCompile(`(?is)^COPY\s+\((.+)\)\s+TO\s+STDOUT.*$`)
)

func translatePGToSQLite(sql string) (string, int) {
	maxParam := 0
	s := reDollarParam.ReplaceAllStringFunc(sql, func(m string) string {
		n, _ := strconv.Atoi(m[1:])
		if n > maxParam {
			maxParam = n
		}
		return "?" + m[1:]
	})
	s = reTypeCast.ReplaceAllString(s, "")
	s = rewriteDistinctOn(s)
	s = rewriteILikeAndRegex(s)
	return s, maxParam
}

func (s *SQLiteReadServer) refreshNativeCatalog(ctx context.Context, ts *SQLiteTenantStore) error {
	if !ts.NativeMode || s.nativeAdapter == nil {
		return nil
	}
	gen := s.nativeAdapter.backend.GetTenantGeneration(ts.TenantID)
	snap, err := s.nativeAdapter.GetSnapshot(ctx, ts.TenantID, gen)
	if err != nil {
		return err
	}
	defer func() { _ = s.nativeAdapter.ReleaseSnapshot(snap) }()
	return s.refreshNativeCatalogWithSnap(ctx, ts, snap)
}

func (s *SQLiteReadServer) getCatalogLSN(ctx context.Context, tenantID, generation string, targetLSN uint64) (uint64, bool) {
	if s.nativeCache != nil {
		return s.nativeCache.CatalogLSN(ctx, tenantID, generation, targetLSN)
	}
	return 0, false
}

func (s *SQLiteReadServer) refreshNativeCatalogWithSnap(ctx context.Context, ts *SQLiteTenantStore, snap *ReadSnapshot) error {
	if !ts.NativeMode || s.nativeAdapter == nil || snap == nil {
		return nil
	}

	if snap.LSN > 0 {
		ts.SyncedLSN.Store(snap.LSN)
	}

	ts.mu.Lock()
	defer ts.mu.Unlock()
	if snap.LSN <= ts.catalogLSN && len(ts.vtabIDs) > 0 && ts.Generation == snap.Generation {
		return nil
	}
	var curCatModLSN uint64
	if len(ts.vtabIDs) > 0 && ts.Generation == snap.Generation {
		if catLSN, ok := s.getCatalogLSN(ctx, ts.TenantID, snap.Generation, snap.LSN); ok {
			curCatModLSN = catLSN
			if catLSN <= ts.lastCatalogModLSN && snap.LSN >= ts.lastCatalogModLSN {
				ts.catalogLSN = snap.LSN
				return nil
			}
		}
	}

	tables, err := DiscoverCatalog(ctx, s.nativeAdapter, s.nativeCLOG, snap, ts.TenantID)
	if err != nil {
		return err
	}
	if s.statsCache != nil {
		s.statsCache.PutDiscoveredTables(ts.TenantID, snap.Generation, tables, snap.LSN)
	}
	ts.extCols.Store(BuildExtColMap(tables))
	s.reconcileAndInvalidatePlans(ts.TenantID, snap.Generation, tables)

	ts.Reader.InvalidatePreparedStmts()
	ts.Reader.SetReadOnlyAuthorizer(false)
	defer ts.Reader.SetReadOnlyAuthorizer(true)

	seen := make(map[string]struct{}, len(tables))
	for _, tbl := range tables {
		seen[tbl.Name] = struct{}{}
		if oldID, ok := ts.vtabIDs[tbl.Name]; ok {
			UnregisterNativeTable(oldID)
		}
		vid, err := ts.Reader.RegisterNativeTableWithOptimizer(ts.TenantID, snap.Generation, tbl, s.nativeAdapter, s.nativeCLOG, s.queryOptimizer, s.adaptiveTracker)
		if err != nil {
			continue
		}
		ts.vtabIDs[tbl.Name] = vid
	}
	for name, oldID := range ts.vtabIDs {
		if _, ok := seen[name]; !ok {
			_ = ts.Reader.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s;", quoteSQLiteIdent(name)))
			UnregisterNativeTable(oldID)
			delete(ts.vtabIDs, name)
		}
	}
	ts.Generation = snap.Generation
	ts.catalogLSN = snap.LSN
	if curCatModLSN == 0 {
		if catLSN, ok := s.getCatalogLSN(ctx, ts.TenantID, snap.Generation, snap.LSN); ok {
			curCatModLSN = catLSN
		}
	}
	if curCatModLSN > ts.lastCatalogModLSN {
		ts.lastCatalogModLSN = curCatModLSN
	}
	return nil
}

func (s *SQLiteReadServer) refreshNativeCatalogOnSlot(ctx context.Context, ts *SQLiteTenantStore, slot *sqliteReaderSlot, snap *ReadSnapshot) error {
	if !ts.NativeMode || s.nativeAdapter == nil || snap == nil || slot == nil {
		return nil
	}
	if snap.LSN > 0 {
		ts.SyncedLSN.Store(snap.LSN)
	}
	if snap.LSN <= slot.catalogLSN && len(slot.vtabIDs) > 0 && slot.generation == snap.Generation {
		return nil
	}
	var curCatModLSN uint64
	if len(slot.vtabIDs) > 0 && slot.generation == snap.Generation {
		if catLSN, ok := s.getCatalogLSN(ctx, ts.TenantID, snap.Generation, snap.LSN); ok {
			curCatModLSN = catLSN
			if catLSN <= slot.lastCatalogModLSN && snap.LSN >= slot.lastCatalogModLSN {
				slot.catalogLSN = snap.LSN
				return nil
			}
		}
	}

	tables, err := DiscoverCatalog(ctx, s.nativeAdapter, s.nativeCLOG, snap, ts.TenantID)
	if err != nil {
		return err
	}
	if s.statsCache != nil {
		s.statsCache.PutDiscoveredTables(ts.TenantID, snap.Generation, tables, snap.LSN)
	}
	ts.extCols.Store(BuildExtColMap(tables))
	s.reconcileAndInvalidatePlans(ts.TenantID, snap.Generation, tables)

	slot.conn.InvalidatePreparedStmts()
	slot.conn.SetReadOnlyAuthorizer(false)
	defer slot.conn.SetReadOnlyAuthorizer(true)

	seen := make(map[string]struct{}, len(tables))
	for _, tbl := range tables {
		seen[tbl.Name] = struct{}{}
		if oldID, ok := slot.vtabIDs[tbl.Name]; ok {
			UnregisterNativeTable(oldID)
		}
		vid, err := slot.conn.RegisterNativeTableWithOptimizer(ts.TenantID, snap.Generation, tbl, s.nativeAdapter, s.nativeCLOG, s.queryOptimizer, s.adaptiveTracker)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[air-read-engine] RegisterNativeTable(%s) failed: %v\n", tbl.Name, err)
			continue
		}
		slot.vtabIDs[tbl.Name] = vid
	}
	for name, oldID := range slot.vtabIDs {
		if _, ok := seen[name]; !ok {
			_ = slot.conn.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s;", quoteSQLiteIdent(name)))
			UnregisterNativeTable(oldID)
			delete(slot.vtabIDs, name)
		}
	}
	slot.generation = snap.Generation
	slot.catalogLSN = snap.LSN
	if curCatModLSN == 0 {
		if catLSN, ok := s.getCatalogLSN(ctx, ts.TenantID, snap.Generation, snap.LSN); ok {
			curCatModLSN = catLSN
		}
	}
	if curCatModLSN > slot.lastCatalogModLSN {
		slot.lastCatalogModLSN = curCatModLSN
	}
	return nil
}

func (s *SQLiteReadServer) executeReaderQuery(ctx context.Context, ts *SQLiteTenantStore, sqliteSQL string, args []string) (*SQLiteResult, error) {
	res, _, err := s.executeReaderQueryWithPlan(ctx, ts, sqliteSQL, args)
	return res, err
}

func (s *SQLiteReadServer) executeReaderQueryWithPlan(ctx context.Context, ts *SQLiteTenantStore, sqliteSQL string, args []string) (*SQLiteResult, *PlanDecision, error) {
	return s.executeReaderQueryWithPlanTyped(ctx, ts, sqliteSQL, args, nil)
}

func (s *SQLiteReadServer) executeReaderQueryWithPlanTyped(ctx context.Context, ts *SQLiteTenantStore, sqliteSQL string, args []string, paramOIDs []uint32) (*SQLiteResult, *PlanDecision, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.cfg.QueryTimeout)
		defer cancel()
	}
	if s.noisyNeighbor != nil {
		var hardCancel context.CancelFunc
		ctx, hardCancel = s.noisyNeighbor.ApplyHardTimeout(ctx)
		defer hardCancel()
		var admitErr error
		ctx, admitErr = s.noisyNeighbor.Admit(ctx, ts.TenantID, 0)
		if admitErr != nil {
			s.queriesFailed.Add(1)
			return nil, nil, admitErr
		}
	}

	if s.scheduler != nil {
		releaseSched, serr := s.scheduler.Acquire(ctx, ts.TenantID)
		if serr != nil {
			if errors.Is(serr, context.Canceled) || errors.Is(serr, context.DeadlineExceeded) {
				s.queriesCancelled.Add(1)
			} else {
				s.queriesFailed.Add(1)
			}
			return nil, nil, serr
		}
		defer releaseSched()
	}

	if ts.sem != nil {
		select {
		case ts.sem <- struct{}{}:
			defer func() { <-ts.sem }()
		case <-ctx.Done():
			s.queriesCancelled.Add(1)
			return nil, nil, ctx.Err()
		}
	}

	start := time.Now()
	s.activeQueries.Add(1)
	ts.queriesTotal.Add(1)
	if s.memMgr != nil {
		budget := s.memMgr.BeginQuery(ts.TenantID, 0)
		defer budget.Release()
		ctx = ContextWithMemoryBudget(ctx, budget)
	}
	if s.cfg.MaxResultRows > 0 {
		ctx = ContextWithResourceLimits(ctx, QueryResourceLimits{MaxResultRows: s.cfg.MaxResultRows})
	}
	defer func() {
		s.activeQueries.Add(-1)
		dur := time.Since(start)
		s.recordLatency(dur)
		s.fallbackTelemetry.RecordQuery(ClassifyQueryShape(sqliteSQL), dur)
		if s.noisyNeighbor != nil {
			s.noisyNeighbor.RecordExecution(ts.TenantID, dur)
		}
	}()

	var snap *ReadSnapshot
	if ts.NativeMode && s.nativeAdapter != nil {
		gen := s.nativeAdapter.backend.GetTenantGeneration(ts.TenantID)
		var err error
		snap, err = s.nativeAdapter.GetSnapshot(ctx, ts.TenantID, gen)
		if err != nil {
			s.snapshotAcqFailures.Add(1)
			s.queriesFailed.Add(1)
			return nil, nil, err
		}
		defer func() { _ = s.nativeAdapter.ReleaseSnapshot(snap) }()

		if ts.readerPool != nil {
			var slot *sqliteReaderSlot
			select {
			case slot = <-ts.readerPool:
				defer func() { ts.readerPool <- slot }()
			case <-ctx.Done():
				s.queriesCancelled.Add(1)
				return nil, nil, ctx.Err()
			}
			if rerr := s.refreshNativeCatalogOnSlot(ctx, ts, slot, snap); rerr != nil {
				s.queriesFailed.Add(1)
				return nil, nil, rerr
			}
			if extMap, _ := ts.extCols.Load().(map[string]ColumnType); true {
				sqliteSQL = rewriteNumericOps(sqliteSQL, extMap)
				if opErr := CheckUnsupportedExtOps(sqliteSQL, extMap); opErr != nil {
					s.queriesFailed.Add(1)
					s.recordQueryErrorWithSQL(sqliteSQL, opErr)
					return nil, nil, opErr
				}
			}
			var schemaVer uint64
			if s.schemaVersions != nil {
				schemaVer = s.schemaVersions.SchemaVersion(ts.TenantID, 0)
			}
			res, plan, err := slot.conn.QueryWithContextPlanTyped(ctx, snap, sqliteSQL, args, paramOIDs, schemaVer)
			if err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					s.queriesCancelled.Add(1)
				} else {
					s.queriesFailed.Add(1)
					s.recordQueryErrorWithSQL(sqliteSQL, err)
				}
				return nil, nil, err
			}
			return res, plan, nil
		}

		if rerr := s.refreshNativeCatalogWithSnap(ctx, ts, snap); rerr != nil {
			s.queriesFailed.Add(1)
			s.recordQueryErrorWithSQL(sqliteSQL, rerr)
			return nil, nil, rerr
		}
	}
	if extMap, _ := ts.extCols.Load().(map[string]ColumnType); true {
		sqliteSQL = rewriteNumericOps(sqliteSQL, extMap)
		if opErr := CheckUnsupportedExtOps(sqliteSQL, extMap); opErr != nil {
			s.queriesFailed.Add(1)
			s.recordQueryErrorWithSQL(sqliteSQL, opErr)
			return nil, nil, opErr
		}
	}

	var schemaVer uint64
	if s.schemaVersions != nil {
		schemaVer = s.schemaVersions.SchemaVersion(ts.TenantID, 0)
	}
	res, plan, err := ts.Reader.QueryWithContextPlanTyped(ctx, snap, sqliteSQL, args, paramOIDs, schemaVer)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			s.queriesCancelled.Add(1)
		} else {
			s.queriesFailed.Add(1)
			s.recordQueryErrorWithSQL(sqliteSQL, err)
		}
		return nil, nil, err
	}
	return res, plan, nil
}

func (sess *sqliteClientSession) currentTenantLSN() uint64 {
	ts, err := sess.srv.getOrCreateTenant(sess.tenantID)
	if err == nil && ts != nil {
		if ts.NativeMode && sess.srv.nativeAdapter != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			gen := sess.srv.nativeAdapter.backend.GetTenantGeneration(ts.TenantID)
			if snap, serr := sess.srv.nativeAdapter.GetSnapshot(ctx, ts.TenantID, gen); serr == nil {
				lsn := snap.LSN
				_ = sess.srv.nativeAdapter.ReleaseSnapshot(snap)
				if lsn > 0 {
					ts.SyncedLSN.Store(lsn)
					return lsn
				}
			}
		}
		if lsn := ts.SyncedLSN.Load(); lsn > 0 {
			return lsn
		}
	}
	return sess.srv.globalLSN.Load()
}

func (sess *sqliteClientSession) rejectWrite(reason string) error {
	sess.srv.writesBlocked.Add(1)
	return &SQLiteReadOnlyError{Message: reason}
}

func tokenizeSQLWords(sql string) []string {
	var words []string
	n := len(sql)
	i := 0
	for i < n {
		c := sql[i]
		if c == ' ' || c == '\t' || c == '\n' || c == '\r' {
			i++
			continue
		}
		if c == '-' && i+1 < n && sql[i+1] == '-' {
			i += 2
			for i < n && sql[i] != '\n' {
				i++
			}
			continue
		}
		if c == '/' && i+1 < n && sql[i+1] == '*' {
			i += 2
			depth := 1
			for i < n && depth > 0 {
				if sql[i] == '/' && i+1 < n && sql[i+1] == '*' {
					depth++
					i += 2
				} else if sql[i] == '*' && i+1 < n && sql[i+1] == '/' {
					depth--
					i += 2
				} else {
					i++
				}
			}
			continue
		}
		if (c == 'E' || c == 'e') && i+1 < n && sql[i+1] == '\'' {
			i += 2
			for i < n {
				if sql[i] == '\\' && i+1 < n {
					i += 2
					continue
				}
				if sql[i] == '\'' {
					if i+1 < n && sql[i+1] == '\'' {
						i += 2
						continue
					}
					i++
					break
				}
				i++
			}
			words = append(words, "'")
			continue
		}
		if c == '\'' {
			i++
			for i < n {
				if sql[i] == '\'' {
					if i+1 < n && sql[i+1] == '\'' {
						i += 2
						continue
					}
					i++
					break
				}
				i++
			}
			words = append(words, "'")
			continue
		}
		if c == '"' {
			i++
			var b strings.Builder
			for i < n {
				if sql[i] == '"' {
					if i+1 < n && sql[i+1] == '"' {
						b.WriteByte('"')
						i += 2
						continue
					}
					i++
					break
				}
				b.WriteByte(sql[i])
				i++
			}
			words = append(words, strings.ToUpper(b.String()))
			continue
		}
		if c == '$' {
			tagEnd := i + 1
			for tagEnd < n && ((sql[tagEnd] >= 'a' && sql[tagEnd] <= 'z') ||
				(sql[tagEnd] >= 'A' && sql[tagEnd] <= 'Z') ||
				(sql[tagEnd] >= '0' && sql[tagEnd] <= '9') ||
				sql[tagEnd] == '_') {
				tagEnd++
			}
			if tagEnd < n && sql[tagEnd] == '$' {
				tag := sql[i : tagEnd+1]
				if closeIdx := strings.Index(sql[tagEnd+1:], tag); closeIdx >= 0 {
					i = tagEnd + 1 + closeIdx + len(tag)
				} else {
					i = n
				}
				words = append(words, "$$")
				continue
			}
		}
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_' {
			start := i
			i++
			for i < n && ((sql[i] >= 'a' && sql[i] <= 'z') || (sql[i] >= 'A' && sql[i] <= 'Z') || (sql[i] >= '0' && sql[i] <= '9') || sql[i] == '_') {
				i++
			}
			words = append(words, strings.ToUpper(sql[start:i]))
			continue
		}
		if c == ';' {
			words = append(words, ";")
		}
		i++
	}
	return words
}

func (sess *sqliteClientSession) execSingleStatement(stmt string, args []string, out *bytes.Buffer) error {
	s := strings.TrimSpace(stmt)
	if s == "" {
		writePGEmptyQuery(out)
		return nil
	}
	fields := tokenizeSQLWords(s)
	if len(fields) == 0 {
		writePGEmptyQuery(out)
		return nil
	}
	head := fields[0]

	// 1. Transaction control & session utility commands
	switch head {
	case "BEGIN", "START":
		for i := 1; i+1 < len(fields); i++ {
			if fields[i] == "READ" && fields[i+1] == "WRITE" {
				return sess.rejectWrite("READ WRITE transactions are not permitted")
			}
		}
		sess.txStatus = 'T'
		writePGCommandComplete(out, "BEGIN")
		return nil

	case "COMMIT", "END":
		sess.txStatus = 'I'
		writePGCommandComplete(out, "COMMIT")
		return nil

	case "ROLLBACK":
		sess.txStatus = 'I'
		writePGCommandComplete(out, "ROLLBACK")
		return nil

	case "DISCARD":
		sess.txStatus = 'I'
		sess.stmts = make(map[string]*sqlitePreparedStmt)
		sess.portals = make(map[string]*sqlitePortal)
		writePGCommandComplete(out, "DISCARD ALL")
		return nil

	case "SET":
		return sess.handleSetStmt(s, out)

	case "RESET":
		return sess.handleResetStmt(s, out)

	case "SHOW":
		return sess.handleShowStmt(s, out)

	case "PREPARE":
		if m := rePrepareSQL.FindStringSubmatch(s); m != nil {
			name := strings.ToLower(m[1])
			inner := strings.TrimSpace(m[2])
			sq, pcnt := translatePGToSQLite(inner)
			sess.stmts[name] = &sqlitePreparedStmt{
				Name:       name,
				OrigSQL:    inner,
				SQLiteSQL:  sq,
				ParamCount: pcnt,
			}
			writePGCommandComplete(out, "PREPARE")
			return nil
		}

	case "EXECUTE":
		if m := reExecuteSQL.FindStringSubmatch(s); m != nil {
			name := strings.ToLower(m[1])
			ps, ok := sess.stmts[name]
			if !ok {
				return fmt.Errorf("prepared statement %q does not exist", name)
			}
			var execArgs []string
			if strings.TrimSpace(m[2]) != "" {
				for _, part := range strings.Split(m[2], ",") {
					p := strings.TrimSpace(part)
					p = strings.Trim(p, "'")
					execArgs = append(execArgs, p)
				}
			}
			return sess.execSingleStatement(ps.OrigSQL, execArgs, out)
		}

	case "DEALLOCATE":
		if m := reDeallocSQL.FindStringSubmatch(s); m != nil {
			name := strings.ToLower(m[1])
			if name == "all" {
				sess.stmts = make(map[string]*sqlitePreparedStmt)
			} else {
				if _, ok := sess.stmts[name]; !ok {
					return fmt.Errorf("prepared statement %q does not exist", name)
				}
				delete(sess.stmts, name)
			}
			writePGCommandComplete(out, "DEALLOCATE")
			return nil
		}

	case "COPY":
		if m := reCopyToSQL.FindStringSubmatch(s); m != nil {
			innerSQL := strings.TrimSpace(m[1])
			return sess.handleCopyToStdout(innerSQL, out)
		}
		return sess.rejectWrite("COPY FROM is not permitted")

	case "EXPLAIN":
		return sess.handleExplainStmt(s, args, out)

	case "INSERT", "UPDATE", "DELETE", "MERGE", "REPLACE":
		return sess.rejectWrite("DML statements (INSERT/UPDATE/DELETE/MERGE) are not permitted")

	case "CREATE", "ALTER", "DROP", "TRUNCATE", "GRANT", "REVOKE", "VACUUM", "CLUSTER", "REINDEX", "DO", "CALL", "LOCK", "ATTACH", "DETACH", "PRAGMA":
		return sess.rejectWrite(fmt.Sprintf("%s is not permitted on read-only SQLite engine", head))

	case "SELECT", "WITH", "VALUES", "TABLE":
		// Allowed read-only query heads; validated below.
	default:
		return sess.rejectWrite(fmt.Sprintf("unsupported statement %s on read-only SQLite engine", head))
	}

	// Check for modifying CTEs, SELECT INTO, multi-statement writes, or row locking (FOR UPDATE / FOR SHARE)
	for i := 0; i < len(fields); i++ {
		w := fields[i]
		switch w {
		case ";":
			if i+1 < len(fields) {
				return sess.rejectWrite("multi-statement execution is not permitted")
			}
		case "INSERT", "UPDATE", "DELETE", "MERGE", "REPLACE", "CREATE", "ALTER", "DROP", "TRUNCATE", "ATTACH", "DETACH", "PRAGMA":
			return sess.rejectWrite("data-modifying CTEs or DDL are not permitted")
		case "INTO":
			if head == "SELECT" {
				return sess.rejectWrite("SELECT INTO is not permitted")
			}
		case "FOR":
			if i+1 < len(fields) {
				nxt := fields[i+1]
				if nxt == "UPDATE" || nxt == "SHARE" || nxt == "NO" || nxt == "KEY" {
					return sess.rejectWrite("SELECT ... FOR UPDATE/SHARE row locking is not permitted")
				}
			}
		}
	}

	// 2. Intercept synthetic PostgreSQL LSN and status introspection queries
	upper := strings.ToUpper(s)
	if strings.Contains(upper, "PG_LAST_WAL_REPLAY_LSN()") || strings.Contains(upper, "PG_CURRENT_WAL_LSN()") {
		lsnStr := FormatPGLSN(sess.currentTenantLSN())
		sess.srv.queriesAllowed.Add(1)
		writeSingleTextCellResult(out, "lsn", lsnStr)
		return nil
	}
	if strings.Contains(upper, "AIR_RO_STATUS()") {
		sess.srv.queriesAllowed.Add(1)
		allowed, blocked := sess.srv.Stats()
		cols := []SQLiteColumn{
			{Name: "enabled", PGType: PGOIDBool, PGSize: 1},
			{Name: "queries_allowed", PGType: PGOIDInt8, PGSize: 8},
			{Name: "writes_blocked", PGType: PGOIDInt8, PGSize: 8},
			{Name: "allow_temp_tables", PGType: PGOIDBool, PGSize: 1},
		}
		rows := [][]SQLiteValue{{
			{Text: "t"},
			{Text: strconv.FormatUint(allowed, 10)},
			{Text: strconv.FormatUint(blocked, 10)},
			{Text: "f"},
		}}
		writeResultSet(out, cols, rows)
		return nil
	}
	if strings.Contains(upper, "SQLITE_VERSION()") {
		sess.srv.queriesAllowed.Add(1)
		writeSingleTextCellResult(out, "sqlite_version", SQLiteVersion())
		return nil
	}

	// 3. Execute against the tenant's read-only SQLite connection
	ts, err := sess.srv.getOrCreateTenant(sess.tenantID)
	if err != nil {
		return err
	}
	if !ts.NativeMode && sess.srv.cfg.PrimaryConnInfo != "" && sess.tenantID != "postgres" {
		// If configured with PrimaryConnInfo and not yet synced, sync on demand.
		_ = sess.srv.SyncFromPrimary(context.Background(), sess.tenantID, sess.srv.cfg.PrimaryConnInfo)
	}

	sqliteSQL, _, _ := sess.srv.translateCachedForSession(sess, ts, s, nil)
	res, err := sess.srv.executeReaderQuery(sess.sessionCtx(), ts, sqliteSQL, args)
	if err != nil {
		var roErr *SQLiteReadOnlyError
		if errors.As(err, &roErr) {
			sess.srv.writesBlocked.Add(1)
		}
		return err
	}
	sess.srv.queriesAllowed.Add(1)
	writeResultSet(out, res.Columns, res.Rows)
	return nil
}

func (sess *sqliteClientSession) handleSetStmt(s string, out *bytes.Buffer) error {
	// Parse SET <name> = <val> or SET <name> TO <val>
	rest := strings.TrimSpace(s[3:])
	rest = strings.TrimPrefix(strings.TrimSpace(rest), "SESSION ")
	rest = strings.TrimPrefix(strings.TrimSpace(rest), "LOCAL ")
	var k, v string
	if idx := strings.Index(rest, "="); idx >= 0 {
		k = strings.ToLower(strings.TrimSpace(rest[:idx]))
		v = strings.Trim(strings.TrimSpace(rest[idx+1:]), "'\"")
	} else {
		parts := strings.Fields(rest)
		if len(parts) >= 3 && strings.EqualFold(parts[1], "TO") {
			k = strings.ToLower(parts[0])
			v = strings.Trim(strings.Join(parts[2:], " "), "'\"")
		}
	}
	if k == "transaction_read_only" || k == "default_transaction_read_only" || k == "air_ro.enabled" {
		vl := strings.ToLower(v)
		if vl == "off" || vl == "false" || vl == "0" || vl == "no" {
			return sess.rejectWrite("cannot disable read-only transaction mode")
		}
	}
	if k != "" {
		sess.sessionVars[k] = v
	}
	writePGCommandComplete(out, "SET")
	return nil
}

func (sess *sqliteClientSession) handleResetStmt(s string, out *bytes.Buffer) error {
	fields := strings.Fields(s)
	if len(fields) >= 2 {
		k := strings.ToLower(fields[1])
		if k == "transaction_read_only" || k == "default_transaction_read_only" || k == "all" {
			// Keep read-only flags locked on; if explicitly resetting transaction_read_only, reject like air_ro.
			if k == "transaction_read_only" || k == "default_transaction_read_only" {
				return sess.rejectWrite("cannot reset read-only transaction mode")
			}
		}
		sess.sessionVars["work_mem"] = "4MB"
	}
	writePGCommandComplete(out, "RESET")
	return nil
}

func (sess *sqliteClientSession) handleShowStmt(s string, out *bytes.Buffer) error {
	fields := strings.Fields(s)
	k := ""
	if len(fields) >= 2 {
		k = strings.ToLower(fields[1])
	}
	val, ok := sess.sessionVars[k]
	if !ok {
		val = "on"
	}
	sess.srv.queriesAllowed.Add(1)
	writeSingleTextCellResult(out, k, val)
	return nil
}

func (sess *sqliteClientSession) handleCopyToStdout(innerSQL string, out *bytes.Buffer) error {
	ts, err := sess.srv.getOrCreateTenant(sess.tenantID)
	if err != nil {
		return err
	}
	sq, _ := translatePGToSQLite(innerSQL)
	res, err := sess.srv.executeReaderQuery(sess.sessionCtx(), ts, sq, nil)
	if err != nil {
		return err
	}
	sess.srv.queriesAllowed.Add(1)

	// CopyOutResponse ('H')
	var hbody bytes.Buffer
	hbody.WriteByte(0) // text format
	_ = binary.Write(&hbody, binary.BigEndian, uint16(len(res.Columns)))
	for range res.Columns {
		_ = binary.Write(&hbody, binary.BigEndian, uint16(0))
	}
	writePGMsg(out, 'H', hbody.Bytes())

	// CopyData ('d') per row
	for _, row := range res.Rows {
		var line strings.Builder
		for i, cell := range row {
			if i > 0 {
				line.WriteByte('\t')
			}
			if cell.IsNil {
				line.WriteString(`\N`)
			} else {
				line.WriteString(cell.Text)
			}
		}
		line.WriteByte('\n')
		writePGMsg(out, 'd', []byte(line.String()))
	}

	// CopyDone ('c') + CommandComplete
	writePGMsg(out, 'c', nil)
	writePGCommandComplete(out, fmt.Sprintf("COPY %d", len(res.Rows)))
	return nil
}

func (s *SQLiteReadServer) reconcileAndInvalidatePlans(tenantID, generation string, tables []DiscoveredTable) {
	if s.planCache != nil {
		s.planCache.EvictStaleGeneration(tenantID, generation)
	}
	if s.schemaVersions == nil {
		return
	}
	changedTables, changedIndexes, schemaChanged := s.schemaVersions.ReconcileCatalog(tenantID, 0, tables)
	if !schemaChanged || s.planCache == nil {
		return
	}
	for _, relOID := range changedTables {
		s.planCache.InvalidateTable(tenantID, relOID)
	}
	for _, idxOID := range changedIndexes {
		s.planCache.InvalidateIndex(tenantID, idxOID)
	}
}

func (s *SQLiteReadServer) translateCached(tenantID, generation string, catalogLSN uint64, rawPGSQL string) (string, int, bool) {
	if s.planCache == nil {
		sq, pcnt := translatePGToSQLite(rawPGSQL)
		return sq, pcnt, false
	}
	var statsVer uint64
	if s.statsCache != nil {
		statsVer = s.statsCache.TenantVersion(tenantID)
	}
	var schemaVer uint64
	if s.schemaVersions != nil {
		schemaVer = s.schemaVersions.SchemaVersion(tenantID, 0)
	}
	plan, hit := s.planCache.GetOrTranslateScoped(
		PlanScope{
			TenantID:      tenantID,
			Generation:    generation,
			CatalogLSN:    catalogLSN,
			SchemaVersion: schemaVer,
			StatsVersion:  statsVer,
			CapabilityVer: 1,
		},
		rawPGSQL,
		func(q string) (string, int, []uint32, []uint32) {
			sq, pcnt := translatePGToSQLite(q)
			tdeps, ideps := s.schemaVersions.ResolveQueryDeps(tenantID, 0, q)
			return sq, pcnt, tdeps, ideps
		},
	)
	if plan == nil {
		sq, pcnt := translatePGToSQLite(rawPGSQL)
		return sq, pcnt, false
	}
	return plan.SQLiteSQL, plan.ParamCount, hit
}

func (s *SQLiteReadServer) translateCachedForSession(sess *sqliteClientSession, ts *SQLiteTenantStore, rawPGSQL string, paramOIDs []uint32) (string, int, bool) {
	if s.planCache == nil {
		sq, pcnt := translatePGToSQLite(rawPGSQL)
		return sq, pcnt, false
	}
	var statsVer uint64
	if s.statsCache != nil {
		statsVer = s.statsCache.TenantVersion(sess.tenantID)
	}
	var schemaVer uint64
	if s.schemaVersions != nil {
		schemaVer = s.schemaVersions.SchemaVersion(sess.tenantID, 0)
	}
	searchPath := sess.sessionVars["search_path"]
	settingsHash := ExecSettingsHash(searchPath, sess.sessionVars["timezone"], sess.sessionVars["lc_collate"])
	ts.mu.Lock()
	catModLSN := ts.lastCatalogModLSN
	gen := ts.Generation
	ts.mu.Unlock()
	plan, hit := s.planCache.GetOrTranslateScoped(
		PlanScope{
			TenantID:      sess.tenantID,
			Generation:    gen,
			SearchPath:    searchPath,
			CatalogLSN:    catModLSN,
			SchemaVersion: schemaVer,
			StatsVersion:  statsVer,
			CapabilityVer: 1,
			ParamOIDs:     paramOIDs,
			SettingsHash:  settingsHash,
		},
		rawPGSQL,
		func(q string) (string, int, []uint32, []uint32) {
			sq, pcnt := translatePGToSQLite(q)
			tdeps, ideps := s.schemaVersions.ResolveQueryDeps(sess.tenantID, 0, q)
			return sq, pcnt, tdeps, ideps
		},
	)
	if plan == nil {
		sq, pcnt := translatePGToSQLite(rawPGSQL)
		return sq, pcnt, false
	}
	return plan.SQLiteSQL, plan.ParamCount, hit
}

func (sess *sqliteClientSession) handleExplainStmt(s string, args []string, out *bytes.Buffer) error {
	// Strip PostgreSQL EXPLAIN (...) options so SQLite's EXPLAIN QUERY PLAN can run cleanly.
	upper := strings.ToUpper(s)
	idx := strings.Index(upper, "SELECT")
	if idx < 0 {
		return sess.rejectWrite("EXPLAIN of non-SELECT statement is not permitted")
	}
	analyze := strings.Contains(upper[:idx], "ANALYZE")
	innerSelect := s[idx:]
	ts, err := sess.srv.getOrCreateTenant(sess.tenantID)
	if err != nil {
		return err
	}
	sq, _, cacheHit := sess.srv.translateCachedForSession(sess, ts, innerSelect, nil)
	res, planDecision, err := sess.srv.executeReaderQueryWithPlan(sess.sessionCtx(), ts, "EXPLAIN QUERY PLAN "+sq, args)
	if err != nil {
		return err
	}
	var actualRows int64
	var dur time.Duration
	if analyze {
		start := time.Now()
		execRes, execPlan, execErr := sess.srv.executeReaderQueryWithPlan(sess.sessionCtx(), ts, sq, args)
		dur = time.Since(start)
		if execErr != nil {
			return execErr
		}
		if execRes != nil {
			actualRows = int64(len(execRes.Rows))
		}
		if execPlan != nil {
			planDecision = execPlan
		}
	}
	sess.srv.queriesAllowed.Add(1)
	cols := []SQLiteColumn{{Name: "QUERY PLAN", PGType: PGOIDText, PGSize: -1}}
	var rows [][]SQLiteValue
	diag := ExplainDiagnostics{
		PlanCacheHit: cacheHit,
		HasAnalyze:   analyze,
		ActualRows:   actualRows,
		ExecTimeMs:   float64(dur.Microseconds()) / 1000.0,
	}
	if planDecision != nil {
		diag.Strategy = planDecision.Selected.Strategy
		diag.SelectedIndex = planDecision.Selected.IndexName
		diag.EstimatedCost = planDecision.Selected.EstimatedCost
		diag.EstimatedRows = planDecision.Selected.EstimatedRows
		diag.PagesRead = uint64(planDecision.Selected.EstimatedPages)
		diag.CacheTier = planDecision.Selected.ExpectedCacheTier
		diag.PredicatePushdown = planDecision.PredicatePushdown
		diag.ProjectionPushdown = planDecision.ProjectionPushdown
		diag.NeededCols = planDecision.NeededCols
		diag.TotalCols = planDecision.TotalCols
	}
	airLines := FormatAIRQueryPlan(diag)
	for _, line := range airLines {
		rows = append(rows, []SQLiteValue{{Text: line}})
	}
	for _, r := range res.Rows {
		detail := "SQLite Query Plan"
		if len(r) >= 4 {
			detail = r[3].Text
		} else if len(r) > 0 {
			detail = r[len(r)-1].Text
		}
		rows = append(rows, []SQLiteValue{{Text: "SQLite: " + detail}})
	}
	if len(rows) == 0 {
		rows = append(rows, []SQLiteValue{{Text: "SQLite Read Engine Plan"}})
	}
	writeResultSet(out, cols, rows)
	return nil
}

func (sess *sqliteClientSession) handleParse(body []byte) bool {
	off := 0
	stmtName := readCStringAdv(body, &off)
	query := readCStringAdv(body, &off)
	var oids []uint32
	if off+2 <= len(body) {
		n := int(binary.BigEndian.Uint16(body[off : off+2]))
		off += 2
		for i := 0; i < n && off+4 <= len(body); i++ {
			oids = append(oids, binary.BigEndian.Uint32(body[off:off+4]))
			off += 4
		}
	}
	var sq string
	var pcnt int
	var schemaVer uint64
	var gen string
	searchPath := sess.sessionVars["search_path"]
	if ts, err := sess.srv.getOrCreateTenant(sess.tenantID); err == nil {
		sq, pcnt, _ = sess.srv.translateCachedForSession(sess, ts, query, oids)
		if sess.srv.schemaVersions != nil {
			schemaVer = sess.srv.schemaVersions.SchemaVersion(sess.tenantID, 0)
		}
		gen = ts.Generation
	} else {
		sq, pcnt = translatePGToSQLite(query)
	}
	if len(oids) > pcnt {
		pcnt = len(oids)
	}
	sess.stmts[stmtName] = &sqlitePreparedStmt{
		Name:          stmtName,
		OrigSQL:       query,
		SQLiteSQL:     sq,
		ParamCount:    pcnt,
		ParamOIDs:     oids,
		SchemaVersion: schemaVer,
		Generation:    gen,
		SearchPath:    searchPath,
	}
	var out bytes.Buffer
	writePGMsg(&out, '1', nil) // ParseComplete
	_, err := sess.conn.Write(out.Bytes())
	return err == nil
}

func (sess *sqliteClientSession) handleBind(body []byte) bool {
	off := 0
	portalName := readCStringAdv(body, &off)
	stmtName := readCStringAdv(body, &off)
	ps, ok := sess.stmts[stmtName]
	if !ok {
		var out bytes.Buffer
		writePGErrorBuf(&out, "ERROR", "26000", fmt.Sprintf("prepared statement %q does not exist", stmtName))
		_, _ = sess.conn.Write(out.Bytes())
		return true
	}

	// Parameter format codes
	var fmtCodes []uint16
	if off+2 <= len(body) {
		nfmt := int(binary.BigEndian.Uint16(body[off : off+2]))
		off += 2
		for i := 0; i < nfmt && off+2 <= len(body); i++ {
			fmtCodes = append(fmtCodes, binary.BigEndian.Uint16(body[off:off+2]))
			off += 2
		}
	}

	// Parameter values
	var args []string
	if off+2 <= len(body) {
		nparams := int(binary.BigEndian.Uint16(body[off : off+2]))
		off += 2
		for i := 0; i < nparams && off+4 <= len(body); i++ {
			plen := int32(binary.BigEndian.Uint32(body[off : off+4]))
			off += 4
			if plen < 0 {
				args = append(args, "\x00NULL\x00")
				continue
			}
			if off+int(plen) > len(body) {
				break
			}
			rawVal := body[off : off+int(plen)]
			off += int(plen)

			isBinary := false
			if len(fmtCodes) == 1 {
				isBinary = fmtCodes[0] == 1
			} else if i < len(fmtCodes) {
				isBinary = fmtCodes[i] == 1
			}
			var oid uint32
			if i < len(ps.ParamOIDs) {
				oid = ps.ParamOIDs[i]
			}
			args = append(args, decodePGParam(rawVal, isBinary, oid))
		}
	}

	sess.portals[portalName] = &sqlitePortal{
		Name: portalName,
		Stmt: ps,
		Args: args,
	}

	var out bytes.Buffer
	writePGMsg(&out, '2', nil) // BindComplete
	_, err := sess.conn.Write(out.Bytes())
	return err == nil
}

func decodePGParam(raw []byte, isBinary bool, oid uint32) string {
	if !isBinary {
		return string(raw)
	}
	switch len(raw) {
	case 1:
		if oid == PGOIDBool {
			if raw[0] != 0 {
				return "1"
			}
			return "0"
		}
		return strconv.Itoa(int(int8(raw[0])))
	case 2:
		if oid == PGOIDInt4 || oid == 21 || oid == 0 {
			return strconv.Itoa(int(int16(binary.BigEndian.Uint16(raw))))
		}
	case 4:
		if oid == PGOIDInt4 || oid == PGOIDInt8 || oid == 0 {
			return strconv.FormatInt(int64(int32(binary.BigEndian.Uint32(raw))), 10)
		}
	case 8:
		if oid == PGOIDInt8 || oid == PGOIDInt4 || oid == 0 {
			return strconv.FormatInt(int64(binary.BigEndian.Uint64(raw)), 10)
		}
	}
	return string(raw)
}

func (sess *sqliteClientSession) handleDescribe(body []byte) bool {
	if len(body) == 0 {
		return true
	}
	kind := body[0]
	off := 1
	name := readCStringAdv(body, &off)
	var out bytes.Buffer

	if kind == 'S' {
		ps, ok := sess.stmts[name]
		if !ok {
			writePGErrorBuf(&out, "ERROR", "26000", fmt.Sprintf("prepared statement %q does not exist", name))
			_, err := sess.conn.Write(out.Bytes())
			return err == nil
		}
		// ParameterDescription ('t')
		var pbuf bytes.Buffer
		_ = binary.Write(&pbuf, binary.BigEndian, uint16(ps.ParamCount))
		for i := 0; i < ps.ParamCount; i++ {
			oid := uint32(0)
			if i < len(ps.ParamOIDs) && ps.ParamOIDs[i] != 0 {
				oid = ps.ParamOIDs[i]
			}
			_ = binary.Write(&pbuf, binary.BigEndian, oid)
		}
		writePGMsg(&out, 't', pbuf.Bytes())

		// Inspect columns via dummy query or emit NoData / RowDescription
		cols := sess.describeColumns(ps, nil)
		if len(cols) == 0 {
			writePGMsg(&out, 'n', nil) // NoData
		} else {
			writePGRowDescription(&out, cols)
		}
	} else if kind == 'P' {
		portal, ok := sess.portals[name]
		if !ok {
			writePGErrorBuf(&out, "ERROR", "34000", fmt.Sprintf("portal %q does not exist", name))
			_, err := sess.conn.Write(out.Bytes())
			return err == nil
		}
		cols := sess.describeColumns(portal.Stmt, portal.Args)
		if len(cols) == 0 {
			writePGMsg(&out, 'n', nil)
		} else {
			writePGRowDescription(&out, cols)
		}
	}
	_, err := sess.conn.Write(out.Bytes())
	return err == nil
}

func (sess *sqliteClientSession) describeColumns(ps *sqlitePreparedStmt, args []string) []SQLiteColumn {
	if ps == nil {
		return nil
	}
	trimmed := strings.TrimSpace(ps.OrigSQL)
	if trimmed == "" {
		return nil
	}
	upper := strings.ToUpper(trimmed)
	if strings.Contains(upper, "PG_LAST_WAL_REPLAY_LSN()") || strings.Contains(upper, "PG_CURRENT_WAL_LSN()") {
		return []SQLiteColumn{{Name: "lsn", PGType: PGOIDText, PGSize: -1}}
	}
	if strings.Contains(upper, "SQLITE_VERSION()") {
		return []SQLiteColumn{{Name: "sqlite_version", PGType: PGOIDText, PGSize: -1}}
	}
	if strings.HasPrefix(upper, "SHOW ") {
		fields := strings.Fields(trimmed)
		colName := "show"
		if len(fields) >= 2 {
			colName = strings.ToLower(fields[1])
		}
		return []SQLiteColumn{{Name: colName, PGType: PGOIDText, PGSize: -1}}
	}
	if !strings.HasPrefix(upper, "SELECT") && !strings.HasPrefix(upper, "WITH") && !strings.HasPrefix(upper, "VALUES") && !strings.HasPrefix(upper, "EXPLAIN") {
		return nil
	}
	_ = sess.revalidatePreparedStmt(ps)
	ts, err := sess.srv.getOrCreateTenant(sess.tenantID)
	if err != nil {
		return nil
	}
	cols, err := sess.srv.describeReaderColumns(sess.sessionCtx(), ts, ps.SQLiteSQL)
	if err != nil {
		return nil
	}
	return cols
}

func (s *SQLiteReadServer) describeReaderColumns(ctx context.Context, ts *SQLiteTenantStore, sqliteSQL string) ([]SQLiteColumn, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if ts.NativeMode && s.nativeAdapter != nil {
		gen := s.nativeAdapter.backend.GetTenantGeneration(ts.TenantID)
		snap, err := s.nativeAdapter.GetSnapshot(ctx, ts.TenantID, gen)
		if err != nil {
			return nil, err
		}
		defer func() { _ = s.nativeAdapter.ReleaseSnapshot(snap) }()

		if ts.readerPool != nil {
			select {
			case slot := <-ts.readerPool:
				defer func() { ts.readerPool <- slot }()
				if rerr := s.refreshNativeCatalogOnSlot(ctx, ts, slot, snap); rerr != nil {
					return nil, rerr
				}
				return slot.conn.DescribeColumns(sqliteSQL)
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		if rerr := s.refreshNativeCatalogWithSnap(ctx, ts, snap); rerr != nil {
			return nil, rerr
		}
	}
	return ts.Reader.DescribeColumns(sqliteSQL)
}

// revalidatePreparedStmt ensures a session-prepared statement is transparently
// re-translated and re-planned if the tenant's schema version, generation, or
// search_path changed since Parse.
func (sess *sqliteClientSession) revalidatePreparedStmt(ps *sqlitePreparedStmt) error {
	if ps == nil || sess == nil || sess.srv == nil {
		return nil
	}
	ts, err := sess.srv.getOrCreateTenant(sess.tenantID)
	if err != nil {
		return err
	}
	var curSchemaVer uint64
	if sess.srv.schemaVersions != nil {
		curSchemaVer = sess.srv.schemaVersions.SchemaVersion(sess.tenantID, 0)
	}
	curSearchPath := sess.sessionVars["search_path"]
	ts.mu.Lock()
	curGen := ts.Generation
	ts.mu.Unlock()
	if ps.SchemaVersion == curSchemaVer && ps.Generation == curGen && ps.SearchPath == curSearchPath {
		return nil
	}
	sq, pcnt, _ := sess.srv.translateCachedForSession(sess, ts, ps.OrigSQL, ps.ParamOIDs)
	if len(ps.ParamOIDs) > pcnt {
		pcnt = len(ps.ParamOIDs)
	}
	ps.SQLiteSQL = sq
	ps.ParamCount = pcnt
	ps.SchemaVersion = curSchemaVer
	ps.Generation = curGen
	ps.SearchPath = curSearchPath
	return nil
}

func (sess *sqliteClientSession) handleExecute(body []byte) bool {
	off := 0
	portalName := readCStringAdv(body, &off)
	var out bytes.Buffer
	portal, ok := sess.portals[portalName]
	if !ok {
		writePGErrorBuf(&out, "ERROR", "34000", fmt.Sprintf("portal %q does not exist", portalName))
		_, err := sess.conn.Write(out.Bytes())
		return err == nil
	}

	if err := sess.revalidatePreparedStmt(portal.Stmt); err != nil {
		writePGErrorBuf(&out, "ERROR", "42000", err.Error())
		_, werr := sess.conn.Write(out.Bytes())
		return werr == nil
	}

	// Fast path for prepared SELECT/WITH/VALUES statements: execute using pre-translated
	// SQLiteSQL + typed ParamOIDs + compiled sqlite3_stmt cache.
	if ts, terr := sess.srv.getOrCreateTenant(sess.tenantID); terr == nil && portal.Stmt.SQLiteSQL != "" {
		upper := strings.ToUpper(strings.TrimSpace(portal.Stmt.OrigSQL))
		if (strings.HasPrefix(upper, "SELECT") || strings.HasPrefix(upper, "WITH") || strings.HasPrefix(upper, "VALUES")) &&
			!strings.Contains(upper, "PG_LAST_WAL_REPLAY_LSN()") &&
			!strings.Contains(upper, "PG_CURRENT_WAL_LSN()") &&
			!strings.Contains(upper, "AIR_RO_STATUS()") &&
			!strings.Contains(upper, "SQLITE_VERSION()") {
			res, _, execErr := sess.srv.executeReaderQueryWithPlanTyped(sess.sessionCtx(), ts, portal.Stmt.SQLiteSQL, portal.Args, portal.Stmt.ParamOIDs)
			if execErr != nil {
				var roErr *SQLiteReadOnlyError
				sqlState := "42000"
				if errors.As(execErr, &roErr) {
					sess.srv.writesBlocked.Add(1)
					sqlState = "25006"
				}
				writePGErrorBuf(&out, "ERROR", sqlState, execErr.Error())
				if sess.txStatus == 'T' {
					sess.txStatus = 'E'
				}
			} else {
				sess.srv.queriesAllowed.Add(1)
				writeResultSet(&out, res.Columns, res.Rows)
			}
			_, werr := sess.conn.Write(out.Bytes())
			return werr == nil
		}
	}

	if err := sess.execSingleStatement(portal.Stmt.OrigSQL, portal.Args, &out); err != nil {
		var roErr *SQLiteReadOnlyError
		sqlState := "42000"
		if errors.As(err, &roErr) {
			sqlState = "25006"
		}
		writePGErrorBuf(&out, "ERROR", sqlState, err.Error())
		if sess.txStatus == 'T' {
			sess.txStatus = 'E'
		}
	}
	_, err := sess.conn.Write(out.Bytes())
	return err == nil
}

func (sess *sqliteClientSession) handleClose(body []byte) bool {
	if len(body) > 0 {
		kind := body[0]
		off := 1
		name := readCStringAdv(body, &off)
		if kind == 'S' {
			delete(sess.stmts, name)
		} else if kind == 'P' {
			delete(sess.portals, name)
		}
	}
	var out bytes.Buffer
	writePGMsg(&out, '3', nil) // CloseComplete
	_, err := sess.conn.Write(out.Bytes())
	return err == nil
}

// --- PostgreSQL v3 Wire Helpers ---

func readPGMsg(r io.Reader) (byte, []byte, error) {
	var hdr [5]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return 0, nil, err
	}
	mtype := hdr[0]
	mlen := binary.BigEndian.Uint32(hdr[1:5])
	if mlen < 4 || mlen > 32*1024*1024 {
		return 0, nil, fmt.Errorf("invalid pg packet length %d", mlen)
	}
	body := make([]byte, mlen-4)
	if len(body) > 0 {
		if _, err := io.ReadFull(r, body); err != nil {
			return 0, nil, err
		}
	}
	return mtype, body, nil
}

func readCString(b []byte, start int) string {
	if start >= len(b) {
		return ""
	}
	idx := bytes.IndexByte(b[start:], 0)
	if idx < 0 {
		return string(b[start:])
	}
	return string(b[start : start+idx])
}

func readCStringAdv(b []byte, off *int) string {
	if *off >= len(b) {
		return ""
	}
	idx := bytes.IndexByte(b[*off:], 0)
	if idx < 0 {
		s := string(b[*off:])
		*off = len(b)
		return s
	}
	s := string(b[*off : *off+idx])
	*off += idx + 1
	return s
}

func writePGMsg(buf *bytes.Buffer, mtype byte, body []byte) {
	buf.WriteByte(mtype)
	var lenBytes [4]byte
	binary.BigEndian.PutUint32(lenBytes[:], uint32(4+len(body)))
	buf.Write(lenBytes[:])
	buf.Write(body)
}

func writePGAuthOk(buf *bytes.Buffer) {
	var b [4]byte
	writePGMsg(buf, 'R', b[:])
}

func writePGParamStatus(buf *bytes.Buffer, k, v string) {
	var b bytes.Buffer
	b.WriteString(k)
	b.WriteByte(0)
	b.WriteString(v)
	b.WriteByte(0)
	writePGMsg(buf, 'S', b.Bytes())
}

func writePGBackendKeyData(buf *bytes.Buffer, pid, secret uint32) {
	var b [8]byte
	binary.BigEndian.PutUint32(b[0:4], pid)
	binary.BigEndian.PutUint32(b[4:8], secret)
	writePGMsg(buf, 'K', b[:])
}

func writePGReadyForQuery(buf *bytes.Buffer, status byte) {
	writePGMsg(buf, 'Z', []byte{status})
}

func writePGEmptyQuery(buf *bytes.Buffer) {
	writePGMsg(buf, 'I', nil)
}

func writePGCommandComplete(buf *bytes.Buffer, tag string) {
	var b bytes.Buffer
	b.WriteString(tag)
	b.WriteByte(0)
	writePGMsg(buf, 'C', b.Bytes())
}

func writePGErrorBuf(buf *bytes.Buffer, severity, sqlState, msg string) {
	var b bytes.Buffer
	b.WriteByte('S')
	b.WriteString(severity)
	b.WriteByte(0)
	b.WriteByte('C')
	b.WriteString(sqlState)
	b.WriteByte(0)
	b.WriteByte('M')
	b.WriteString(msg)
	b.WriteByte(0)
	b.WriteByte(0)
	writePGMsg(buf, 'E', b.Bytes())
}

func writePGError(w io.Writer, severity, sqlState, msg string) error {
	var buf bytes.Buffer
	writePGErrorBuf(&buf, severity, sqlState, msg)
	_, err := w.Write(buf.Bytes())
	return err
}

func writePGRowDescription(buf *bytes.Buffer, cols []SQLiteColumn) {
	var b bytes.Buffer
	_ = binary.Write(&b, binary.BigEndian, uint16(len(cols)))
	for _, col := range cols {
		b.WriteString(col.Name)
		b.WriteByte(0)
		_ = binary.Write(&b, binary.BigEndian, uint32(0)) // table OID
		_ = binary.Write(&b, binary.BigEndian, uint16(0)) // column attr num
		oid := col.PGType
		if oid == 0 {
			oid = PGOIDText
		}
		sz := col.PGSize
		if sz == 0 {
			sz = -1
		}
		_ = binary.Write(&b, binary.BigEndian, oid)
		_ = binary.Write(&b, binary.BigEndian, sz)
		_ = binary.Write(&b, binary.BigEndian, int32(-1)) // type modifier
		_ = binary.Write(&b, binary.BigEndian, uint16(0)) // text format code
	}
	writePGMsg(buf, 'T', b.Bytes())
}

var pgRowBufPool = sync.Pool{
	New: func() any {
		return new(bytes.Buffer)
	},
}

func writePGDataRow(buf *bytes.Buffer, row []SQLiteValue) {
	b := pgRowBufPool.Get().(*bytes.Buffer)
	b.Reset()
	var hdr [4]byte
	binary.BigEndian.PutUint16(hdr[:2], uint16(len(row)))
	b.Write(hdr[:2])
	for _, cell := range row {
		if cell.IsNil {
			binary.BigEndian.PutUint32(hdr[:], 0xffffffff)
			b.Write(hdr[:])
		} else {
			binary.BigEndian.PutUint32(hdr[:], uint32(len(cell.Text)))
			b.Write(hdr[:])
			b.WriteString(cell.Text)
		}
	}
	writePGMsg(buf, 'D', b.Bytes())
	if b.Cap() <= 64*1024 {
		b.Reset()
		pgRowBufPool.Put(b)
	}
}

func writeResultSet(buf *bytes.Buffer, cols []SQLiteColumn, rows [][]SQLiteValue) {
	if len(cols) > 0 {
		writePGRowDescription(buf, cols)
		for _, r := range rows {
			writePGDataRow(buf, r)
		}
	}
	writePGCommandComplete(buf, fmt.Sprintf("SELECT %d", len(rows)))
}

func writeSingleTextCellResult(buf *bytes.Buffer, colName, val string) {
	cols := []SQLiteColumn{{Name: colName, PGType: PGOIDText, PGSize: -1}}
	rows := [][]SQLiteValue{{{Text: val}}}
	writeResultSet(buf, cols, rows)
}

func splitSQLStatements(sql string) []string {
	var out []string
	var cur strings.Builder
	inSingle := false
	inDouble := false
	for i := 0; i < len(sql); i++ {
		c := sql[i]
		if c == '\'' && !inDouble {
			inSingle = !inSingle
			cur.WriteByte(c)
			continue
		}
		if c == '"' && !inSingle {
			inDouble = !inDouble
			cur.WriteByte(c)
			continue
		}
		if c == ';' && !inSingle && !inDouble {
			if s := strings.TrimSpace(cur.String()); s != "" {
				out = append(out, s)
			}
			cur.Reset()
			continue
		}
		cur.WriteByte(c)
	}
	if s := strings.TrimSpace(cur.String()); s != "" {
		out = append(out, s)
	}
	return out
}
