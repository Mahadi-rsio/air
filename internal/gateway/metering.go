package gateway

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	defaultMeteringMaxFileBytes  = int64(10 * 1024 * 1024)  // 10 MB
	defaultMeteringMaxSpoolBytes = int64(256 * 1024 * 1024) // 256 MB
	defaultMeteringFlushInterval = 2 * time.Minute
	activeMeteringLogName        = "query_metering.log"
	rotatedMeteringLogPrefix     = "query_metering."
	rotatedMeteringLogSuffix     = ".log"
	sealedBatchPrefix            = "batch."
	sealedBatchSuffix            = ".log"
	quarantineDirName            = "quarantine"
	legacyPendingDirName         = "legacy_pending"
	gatewayIDFileName            = ".gateway_id"
)

var (
	ErrMeteringSinkUnavailable = errors.New("metering sink unavailable")
	ErrMeteringSpoolFull       = errors.New("metering spool full")
	ErrBatchPayloadMismatch    = errors.New("metering batch receipt payload mismatch")
	ErrMeteringNotWritable     = errors.New("metering directory is not writable")
	ErrMeteringTempDirForbidden = errors.New("metering directory cannot use temporary directory when billing is enabled")
)

// MeteringEntry represents a single query metering record logged by the gateway.
type MeteringEntry struct {
	Date           time.Time
	DBID           string
	BandwidthBytes uint64
	QueryTimeUs    uint64
	QueryType      string
}

// MeteringSaver persists parsed metering entries to durable storage.
type MeteringSaver interface {
	SaveMeteringBatch(ctx context.Context, entries []MeteringEntry) error
}

// ReceiptMeteringSaver persists a sealed metering batch idempotently using an
// immutable batch ID and payload SHA-256 checksum in a single DB transaction.
type ReceiptMeteringSaver interface {
	MeteringSaver
	SaveMeteringBatchWithReceipt(ctx context.Context, batchID, gatewayID, payloadSHA256 string, entries []MeteringEntry) error
	Ping(ctx context.Context) error
}

// TenantStorageProvider returns the current storage usage in bytes for a tenant.
type TenantStorageProvider interface {
	GetTenantStorageBytes(ctx context.Context, tenantID string) (uint64, error)
}

// TenantUsageDelta represents aggregated metering increments for a single tenant.
type TenantUsageDelta struct {
	TenantID       string
	BandwidthBytes uint64
	ReadQueries    uint64
	WriteQueries   uint64
}

// IsWriteQueryType returns true if a normalized QueryType represents a mutating/write query.
func IsWriteQueryType(queryType string) bool {
	switch strings.ToUpper(strings.TrimSpace(queryType)) {
	case "INSERT", "UPDATE", "DELETE", "MERGE", "DDL", "COPY":
		return true
	default:
		return false
	}
}

// IsReadQueryType returns true if a normalized QueryType represents a read query.
func IsReadQueryType(queryType string) bool {
	switch strings.ToUpper(strings.TrimSpace(queryType)) {
	case "SELECT", "EXPLAIN", "SHOW", "EXECUTE", "EXTENDED", "OTHER":
		return true
	default:
		return false
	}
}

// AggregateMeteringEntries aggregates raw MeteringEntry records per tenant (DBID)
// into bandwidth, read query count, and write query count deltas, sorted by TenantID.
func AggregateMeteringEntries(entries []MeteringEntry) []TenantUsageDelta {
	if len(entries) == 0 {
		return nil
	}
	byTenant := make(map[string]*TenantUsageDelta)
	for _, e := range entries {
		tid := strings.TrimSpace(e.DBID)
		if tid == "" || tid == "unknown" {
			continue
		}
		d, ok := byTenant[tid]
		if !ok {
			d = &TenantUsageDelta{TenantID: tid}
			byTenant[tid] = d
		}
		d.BandwidthBytes += e.BandwidthBytes
		if IsWriteQueryType(e.QueryType) {
			d.WriteQueries++
		} else if IsReadQueryType(e.QueryType) {
			d.ReadQueries++
		}
	}
	if len(byTenant) == 0 {
		return nil
	}
	out := make([]TenantUsageDelta, 0, len(byTenant))
	for _, d := range byTenant {
		out = append(out, *d)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].TenantID < out[j].TenantID
	})
	return out
}

// PGMeteringSaver aggregates parsed metering batches per tenant and updates
// tenant_usage in the PostgreSQL metadata database.
type PGMeteringSaver struct {
	dsn             string
	mu              sync.Mutex
	conn            *pgx.Conn
	schemaEnsured   bool
	storageProvider TenantStorageProvider
}

// NewPGMeteringSaver creates a PostgreSQL-backed MeteringSaver for metaDSN.
func NewPGMeteringSaver(metaDSN string) *PGMeteringSaver {
	return &PGMeteringSaver{dsn: metaDSN}
}

// SetStorageProvider configures an optional TenantStorageProvider (e.g. PageserverGRPCClient)
// used to refresh each tenant's storage_bytes when flushing usage updates.
func (s *PGMeteringSaver) SetStorageProvider(p TenantStorageProvider) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.storageProvider = p
}

func (s *PGMeteringSaver) ensureConnLocked(ctx context.Context) (*pgx.Conn, error) {
	if s.dsn == "" {
		return nil, fmt.Errorf("metering metadata DSN is empty")
	}
	if s.conn != nil {
		if err := s.conn.Ping(ctx); err == nil {
			return s.conn, nil
		}
		_ = s.conn.Close(context.Background())
		s.conn = nil
		s.schemaEnsured = false
	}
	conn, err := pgx.Connect(ctx, s.dsn)
	if err != nil {
		return nil, err
	}
	s.conn = conn
	if !s.schemaEnsured {
		stmts := []string{
			`CREATE TABLE IF NOT EXISTS tenant_usage (
				tenant_id TEXT PRIMARY KEY,
				bandwidth_bytes BIGINT NOT NULL DEFAULT 0,
				read_queries BIGINT NOT NULL DEFAULT 0,
				write_queries BIGINT NOT NULL DEFAULT 0,
				storage_bytes BIGINT NOT NULL DEFAULT 0,
				updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
			)`,
			`CREATE TABLE IF NOT EXISTS metering_batch_receipts (
				batch_id TEXT PRIMARY KEY,
				gateway_id TEXT NOT NULL,
				payload_sha256 TEXT NOT NULL,
				entry_count BIGINT NOT NULL,
				tenant_count INT NOT NULL,
				committed_at TIMESTAMPTZ NOT NULL DEFAULT now()
			)`,
			`CREATE INDEX IF NOT EXISTS idx_metering_batch_receipts_gateway ON metering_batch_receipts(gateway_id, committed_at DESC)`,
		}
		for _, stmt := range stmts {
			if _, err := conn.Exec(ctx, stmt); err != nil {
				_ = conn.Close(context.Background())
				s.conn = nil
				return nil, fmt.Errorf("ensure tenant_usage schema failed: %w", err)
			}
		}
		s.schemaEnsured = true
	}
	return s.conn, nil
}

// Ping verifies that the metadata PostgreSQL sink is reachable and schema is ready.
func (s *PGMeteringSaver) Ping(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.ensureConnLocked(ctx)
	return err
}

// SaveMeteringBatch aggregates parsed MeteringEntry rows per tenant and updates
// bandwidth_bytes, read_queries, write_queries, and storage_bytes in tenant_usage.
func (s *PGMeteringSaver) SaveMeteringBatch(ctx context.Context, entries []MeteringEntry) error {
	if len(entries) == 0 {
		return nil
	}
	var b strings.Builder
	for _, e := range entries {
		b.WriteString(FormatMeteringLine(e))
	}
	sum := sha256.Sum256([]byte(b.String()))
	payloadHash := hex.EncodeToString(sum[:])
	batchID := "anon_" + payloadHash[:24]
	return s.SaveMeteringBatchWithReceipt(ctx, batchID, "unknown", payloadHash, entries)
}

// SaveMeteringBatchWithReceipt inserts an immutable batch receipt and applies
// tenant_usage deltas in one PostgreSQL transaction. Repeated batch IDs with
// identical payload_sha256 are idempotently ignored; mismatched payloads fail.
func (s *PGMeteringSaver) SaveMeteringBatchWithReceipt(ctx context.Context, batchID, gatewayID, payloadSHA256 string, entries []MeteringEntry) error {
	deltas := AggregateMeteringEntries(entries)
	if len(deltas) == 0 && batchID == "" {
		return nil
	}
	if strings.TrimSpace(batchID) == "" {
		return fmt.Errorf("metering batch_id is required")
	}
	if strings.TrimSpace(gatewayID) == "" {
		gatewayID = "unknown"
	}

	s.mu.Lock()
	storageProvider := s.storageProvider
	s.mu.Unlock()

	type tenantUpdate struct {
		delta      TenantUsageDelta
		storage    uint64
		hasStorage bool
	}
	updates := make([]tenantUpdate, len(deltas))
	for i, d := range deltas {
		u := tenantUpdate{delta: d}
		if storageProvider != nil {
			sz, err := storageProvider.GetTenantStorageBytes(ctx, d.TenantID)
			if err != nil {
				log.Printf("[air-gateway] metering storage lookup failed for %s: %v", d.TenantID, err)
			} else {
				u.storage = sz
				u.hasStorage = true
			}
		}
		updates[i] = u
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	conn, err := s.ensureConnLocked(ctx)
	if err != nil {
		return err
	}

	tx, err := conn.Begin(ctx)
	if err != nil {
		_ = conn.Close(context.Background())
		s.conn = nil
		return err
	}
	defer tx.Rollback(ctx)

	var insertedID string
	err = tx.QueryRow(ctx, `
		INSERT INTO metering_batch_receipts (batch_id, gateway_id, payload_sha256, entry_count, tenant_count)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (batch_id) DO NOTHING
		RETURNING batch_id
	`, batchID, gatewayID, payloadSHA256, int64(len(entries)), len(updates)).Scan(&insertedID)
	if err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("insert metering_batch_receipts (%s) failed: %w", batchID, err)
		}
		var existingHash string
		qErr := tx.QueryRow(ctx, `SELECT payload_sha256 FROM metering_batch_receipts WHERE batch_id = $1`, batchID).Scan(&existingHash)
		if qErr != nil {
			return fmt.Errorf("lookup existing metering_batch_receipts (%s) failed: %w", batchID, qErr)
		}
		if existingHash != payloadSHA256 {
			return fmt.Errorf("%w: batch_id=%s existing=%s got=%s", ErrBatchPayloadMismatch, batchID, existingHash, payloadSHA256)
		}
		// Identical batch already committed previously; do not double-count.
		return nil
	}

	const upsertSQL = `
		INSERT INTO tenant_usage (tenant_id, bandwidth_bytes, read_queries, write_queries, storage_bytes, updated_at)
		VALUES ($1, $2, $3, $4, $5, now())
		ON CONFLICT (tenant_id) DO UPDATE SET
			bandwidth_bytes = tenant_usage.bandwidth_bytes + EXCLUDED.bandwidth_bytes,
			read_queries    = tenant_usage.read_queries + EXCLUDED.read_queries,
			write_queries   = tenant_usage.write_queries + EXCLUDED.write_queries,
			storage_bytes   = CASE WHEN $6 THEN EXCLUDED.storage_bytes ELSE tenant_usage.storage_bytes END,
			updated_at      = now()
	`
	for _, u := range updates {
		if _, err := tx.Exec(
			ctx,
			upsertSQL,
			u.delta.TenantID,
			int64(u.delta.BandwidthBytes),
			int64(u.delta.ReadQueries),
			int64(u.delta.WriteQueries),
			int64(u.storage),
			u.hasStorage,
		); err != nil {
			return fmt.Errorf("upsert tenant_usage (%s) failed: %w", u.delta.TenantID, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit tenant_usage failed: %w", err)
	}
	return nil
}

// Close closes any open PostgreSQL connection held by the saver.
func (s *PGMeteringSaver) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn != nil {
		_ = s.conn.Close(context.Background())
		s.conn = nil
	}
}

// ClassifyQueryType extracts a normalized query type label (SELECT, INSERT, UPDATE, DELETE, DDL, TX, etc.)
// from a SQL string.
func ClassifyQueryType(sql string) string {
	stmts := tokenizeStatements(sql)
	best := ""
	bestPriority := -1

	record := func(qType string, priority int) {
		if priority > bestPriority {
			best = qType
			bestPriority = priority
		}
	}

	for _, toks := range stmts {
		if len(toks) == 0 {
			continue
		}
		head := toks[0].upper
		switch head {
		case "INSERT", "UPDATE", "DELETE", "MERGE":
			record(head, 4)
		case "CREATE", "ALTER", "DROP", "TRUNCATE", "GRANT", "REVOKE", "COMMENT", "VACUUM", "ANALYZE", "REINDEX", "CLUSTER", "REFRESH":
			record("DDL", 4)
		case "SELECT", "WITH", "VALUES", "TABLE":
			mutating := ""
			if head == "WITH" {
				for _, t := range toks[1:] {
					if t.upper == "INSERT" || t.upper == "UPDATE" || t.upper == "DELETE" {
						mutating = t.upper
						break
					}
				}
			}
			if mutating != "" {
				record(mutating, 4)
			} else {
				record("SELECT", 3)
			}
		case "COPY", "EXPLAIN", "PREPARE", "EXECUTE", "DEALLOCATE", "SHOW", "SET", "RESET", "LISTEN", "UNLISTEN", "NOTIFY":
			record(head, 2)
		case "BEGIN", "START", "COMMIT", "END", "ROLLBACK", "ABORT", "SAVEPOINT", "RELEASE":
			record("TX", 1)
		default:
			record("OTHER", 0)
		}
	}
	if best == "" {
		return "OTHER"
	}
	return best
}

// FormatMeteringLine formats a MeteringEntry as a single tab-separated log line:
// date  db_id  bandwidth  query_time  query_type
func FormatMeteringLine(e MeteringEntry) string {
	dbID := strings.TrimSpace(e.DBID)
	if dbID == "" {
		dbID = "unknown"
	}
	qType := strings.TrimSpace(e.QueryType)
	if qType == "" {
		qType = "OTHER"
	}
	ts := e.Date
	if ts.IsZero() {
		ts = time.Now().UTC()
	}
	return fmt.Sprintf("%s\t%s\t%d\t%d\t%s\n",
		ts.UTC().Format(time.RFC3339Nano),
		dbID,
		e.BandwidthBytes,
		e.QueryTimeUs,
		qType,
	)
}

// ParseMeteringLine parses a single metering log line into a MeteringEntry.
func ParseMeteringLine(line string) (MeteringEntry, error) {
	fields := strings.Fields(line)
	if len(fields) != 5 {
		return MeteringEntry{}, fmt.Errorf("expected 5 fields, got %d", len(fields))
	}
	ts, err := time.Parse(time.RFC3339Nano, fields[0])
	if err != nil {
		ts, err = time.Parse(time.RFC3339, fields[0])
		if err != nil {
			return MeteringEntry{}, fmt.Errorf("invalid timestamp %q: %w", fields[0], err)
		}
	}
	bw, err := strconv.ParseUint(fields[2], 10, 64)
	if err != nil {
		return MeteringEntry{}, fmt.Errorf("invalid bandwidth %q: %w", fields[2], err)
	}
	qt, err := strconv.ParseUint(fields[3], 10, 64)
	if err != nil {
		return MeteringEntry{}, fmt.Errorf("invalid query_time %q: %w", fields[3], err)
	}
	return MeteringEntry{
		Date:           ts.UTC(),
		DBID:           fields[1],
		BandwidthBytes: bw,
		QueryTimeUs:    qt,
		QueryType:      fields[4],
	}, nil
}

// MeteringLoggerOptions configures durable metering spool behavior.
type MeteringLoggerOptions struct {
	LogDir          string
	GatewayID       string
	BillingEnabled  bool
	MaxFileBytes    int64
	MaxSpoolBytes   int64
	FlushInterval   time.Duration
	SyncOnWrite     bool
	Saver           MeteringSaver
	RequireWritable bool
}

// MeteringStatus reports the current health and spool state of MeteringLogger.
type MeteringStatus struct {
	GatewayID          string    `json:"gateway_id"`
	LogDir             string    `json:"log_dir"`
	BillingEnabled     bool      `json:"billing_enabled"`
	Writable           bool      `json:"writable"`
	SinkConfigured     bool      `json:"sink_configured"`
	SinkHealthy        bool      `json:"sink_healthy"`
	PendingBatches     int       `json:"pending_batches"`
	PendingBytes       int64     `json:"pending_bytes"`
	ActiveBytes        int64     `json:"active_bytes"`
	QuarantinedCount   int       `json:"quarantined_count"`
	LegacyPendingCount int       `json:"legacy_pending_count"`
	SpoolBytes         int64     `json:"spool_bytes"`
	MaxSpoolBytes      int64     `json:"max_spool_bytes"`
	SpoolFull          bool      `json:"spool_full"`
	Blocked            bool      `json:"blocked"`
	AppendErrors       uint64    `json:"append_errors"`
	FlushErrors        uint64    `json:"flush_errors"`
	LastError          string    `json:"last_error,omitempty"`
	LastErrorAt        time.Time `json:"last_error_at,omitempty"`
	LastFlushedAt      time.Time `json:"last_flushed_at,omitempty"`
}

// MeteringLogger buffers query metering entries to a local durable spool and
// flushes sealed batches with immutable IDs to PostgreSQL, deleting each batch
// only after its commit or matching durable receipt is confirmed.
type MeteringLogger struct {
	logDir         string
	quarantineDir  string
	legacyDir      string
	activePath     string
	gatewayID      string
	billingEnabled bool
	maxFileBytes   int64
	maxSpoolBytes  int64
	flushInterval  time.Duration
	syncOnWrite    bool
	saver          MeteringSaver

	mu            sync.Mutex
	file          *os.File
	currentSize   int64
	rotateSeq     atomic.Uint64
	writable      bool
	spoolFull     atomic.Bool
	appendErrors  atomic.Uint64
	flushErrors   atomic.Uint64
	lastError     string
	lastErrorAt   time.Time
	lastFlushedAt time.Time

	running atomic.Bool
	cancel  context.CancelFunc
	flushCh chan string
	wg      sync.WaitGroup
	flushMu sync.Mutex
}

func isTempDirPath(dir string) bool {
	cleanDir := filepath.Clean(dir)
	tmp := filepath.Clean(os.TempDir())
	if cleanDir == tmp || strings.HasPrefix(cleanDir, tmp+string(filepath.Separator)) {
		return true
	}
	if cleanDir == "/tmp" || strings.HasPrefix(cleanDir, "/tmp/") {
		return true
	}
	return false
}

func probeDirWritable(dir string) error {
	f, err := os.CreateTemp(dir, ".air_write_probe_*")
	if err != nil {
		return fmt.Errorf("%w: %v", ErrMeteringNotWritable, err)
	}
	name := f.Name()
	if _, werr := f.WriteString("ok"); werr != nil {
		_ = f.Close()
		_ = os.Remove(name)
		return fmt.Errorf("%w: %v", ErrMeteringNotWritable, werr)
	}
	if serr := f.Sync(); serr != nil {
		_ = f.Close()
		_ = os.Remove(name)
		return fmt.Errorf("%w: %v", ErrMeteringNotWritable, serr)
	}
	if cerr := f.Close(); cerr != nil {
		_ = os.Remove(name)
		return fmt.Errorf("%w: %v", ErrMeteringNotWritable, cerr)
	}
	if rerr := os.Remove(name); rerr != nil {
		return fmt.Errorf("%w: %v", ErrMeteringNotWritable, rerr)
	}
	return nil
}

func sanitizeGatewayID(id string) string {
	id = strings.TrimSpace(id)
	if id == "" {
		return ""
	}
	var b strings.Builder
	for _, r := range id {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' {
			b.WriteRune(r)
		} else {
			b.WriteByte('_')
		}
	}
	return b.String()
}

func loadOrCreateGatewayID(logDir, configured string) (string, error) {
	idFile := filepath.Join(logDir, gatewayIDFileName)
	if b, err := os.ReadFile(idFile); err == nil {
		if existing := sanitizeGatewayID(string(b)); existing != "" {
			return existing, nil
		}
	}
	id := sanitizeGatewayID(configured)
	if id == "" {
		var buf [6]byte
		_, _ = rand.Read(buf[:])
		id = "gw_" + hex.EncodeToString(buf[:])
	}
	if err := os.WriteFile(idFile, []byte(id+"\n"), 0o644); err != nil {
		return id, err
	}
	return id, nil
}

// NewMeteringLogger creates a MeteringLogger writing to logDir.
func NewMeteringLogger(logDir string, maxFileBytes int64, flushInterval time.Duration, saver MeteringSaver) (*MeteringLogger, error) {
	return NewMeteringLoggerWithOptions(MeteringLoggerOptions{
		LogDir:        logDir,
		MaxFileBytes:  maxFileBytes,
		FlushInterval: flushInterval,
		Saver:         saver,
	})
}

// NewMeteringLoggerWithOptions creates a MeteringLogger with explicit durability and billing options.
func NewMeteringLoggerWithOptions(opts MeteringLoggerOptions) (*MeteringLogger, error) {
	logDir := strings.TrimSpace(opts.LogDir)
	if opts.BillingEnabled {
		if logDir == "" || isTempDirPath(logDir) {
			return nil, ErrMeteringTempDirForbidden
		}
		if opts.Saver == nil {
			return nil, ErrMeteringSinkUnavailable
		}
	}
	if logDir == "" {
		logDir = filepath.Join(os.TempDir(), "air-gateway-metering")
	}
	if opts.MaxFileBytes <= 0 {
		opts.MaxFileBytes = defaultMeteringMaxFileBytes
	}
	if opts.MaxSpoolBytes <= 0 {
		opts.MaxSpoolBytes = defaultMeteringMaxSpoolBytes
	}
	if opts.FlushInterval <= 0 {
		opts.FlushInterval = defaultMeteringFlushInterval
	}
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		return nil, fmt.Errorf("create metering log dir %q: %w", logDir, err)
	}
	if err := probeDirWritable(logDir); err != nil {
		return nil, err
	}
	qDir := filepath.Join(logDir, quarantineDirName)
	if err := os.MkdirAll(qDir, 0o755); err != nil {
		return nil, fmt.Errorf("create metering quarantine dir %q: %w", qDir, err)
	}
	lDir := filepath.Join(logDir, legacyPendingDirName)
	if err := os.MkdirAll(lDir, 0o755); err != nil {
		return nil, fmt.Errorf("create metering legacy dir %q: %w", lDir, err)
	}
	gwID, err := loadOrCreateGatewayID(logDir, opts.GatewayID)
	if err != nil {
		return nil, fmt.Errorf("persist gateway id in %q: %w", logDir, err)
	}

	return &MeteringLogger{
		logDir:         logDir,
		quarantineDir:  qDir,
		legacyDir:      lDir,
		activePath:     filepath.Join(logDir, activeMeteringLogName),
		gatewayID:      gwID,
		billingEnabled: opts.BillingEnabled,
		maxFileBytes:   opts.MaxFileBytes,
		maxSpoolBytes:  opts.MaxSpoolBytes,
		flushInterval:  opts.FlushInterval,
		syncOnWrite:    opts.SyncOnWrite,
		saver:          opts.Saver,
		writable:       true,
		flushCh:        make(chan string, 64),
	}, nil
}

// Start begins the background flush loop, preserves any legacy rotated files
// for reconciliation, and seals any leftover active log file from a prior run.
func (m *MeteringLogger) Start() {
	if m.running.Swap(true) {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	m.cancel = cancel

	// Preserve any legacy query_metering.<ts>.<seq>.log files without deleting or double-counting.
	if err := m.preserveLegacyFiles(); err != nil {
		m.recordError("preserve legacy metering files", err)
	}

	// Recover any leftover active log file from an earlier run by sealing it with an immutable batch ID.
	if info, err := os.Stat(m.activePath); err == nil && info.Size() > 0 {
		m.mu.Lock()
		if rerr := m.rotateLocked(false); rerr != nil {
			m.recordErrorLocked("recover active metering log", rerr)
		}
		m.mu.Unlock()
	}

	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		m.flushLoop(ctx)
	}()
}

// Stop stops the background flush loop, seals any remaining active log file,
// and performs a final synchronous flush.
func (m *MeteringLogger) Stop() {
	if !m.running.Swap(false) {
		return
	}
	if m.cancel != nil {
		m.cancel()
	}
	m.wg.Wait()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := m.FlushNow(ctx); err != nil {
		m.recordError("final metering flush on stop", err)
	}

	m.mu.Lock()
	if m.file != nil {
		_ = m.file.Sync()
		_ = m.file.Close()
		m.file = nil
	}
	m.mu.Unlock()

	if pgSaver, ok := m.saver.(*PGMeteringSaver); ok && pgSaver != nil {
		pgSaver.Close()
	}
}

func (m *MeteringLogger) recordError(op string, err error) {
	if err == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.recordErrorLocked(op, err)
}

func (m *MeteringLogger) recordErrorLocked(op string, err error) {
	if err == nil {
		return
	}
	msg := fmt.Sprintf("%s: %v", op, err)
	m.lastError = msg
	m.lastErrorAt = time.Now().UTC()
	log.Printf("[air-gateway] metering %s", msg)
}

func (m *MeteringLogger) computeSpoolBytesLocked() int64 {
	var total int64
	for _, dir := range []string{m.logDir, m.quarantineDir, m.legacyDir} {
		ents, err := os.ReadDir(dir)
		if err != nil {
			continue
		}
		for _, ent := range ents {
			if ent.IsDir() {
				continue
			}
			if info, err := ent.Info(); err == nil {
				total += info.Size()
			}
		}
	}
	return total
}

// IsSpoolBlocked returns true if the spool is full or has unrecoverable errors
// that prevent safe metering in billing-enabled mode.
func (m *MeteringLogger) IsSpoolBlocked() bool {
	if m == nil {
		return false
	}
	if m.spoolFull.Load() {
		return true
	}
	st := m.Status(context.Background())
	return st.Blocked
}

// LogQuery appends a query metering record to the active log file.
func (m *MeteringLogger) LogQuery(entry MeteringEntry) {
	_ = m.LogQueryErr(entry)
}

// LogQueryErr appends a query metering record and returns any durability or spool-limit error.
func (m *MeteringLogger) LogQueryErr(entry MeteringEntry) error {
	line := FormatMeteringLine(entry)

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.maxSpoolBytes > 0 {
		spoolBytes := m.computeSpoolBytesLocked()
		if spoolBytes+int64(len(line)) > m.maxSpoolBytes {
			m.spoolFull.Store(true)
			m.appendErrors.Add(1)
			m.recordErrorLocked("spool limit exceeded", ErrMeteringSpoolFull)
			return ErrMeteringSpoolFull
		}
	}

	if err := m.ensureOpenLocked(); err != nil {
		m.appendErrors.Add(1)
		m.recordErrorLocked("open active metering log", err)
		return err
	}
	n, err := m.file.WriteString(line)
	if err != nil {
		m.appendErrors.Add(1)
		m.recordErrorLocked("write metering log", err)
		return err
	}
	if m.syncOnWrite {
		if serr := m.file.Sync(); serr != nil {
			m.appendErrors.Add(1)
			m.recordErrorLocked("sync metering log", serr)
			return serr
		}
	}
	m.currentSize += int64(n)
	m.spoolFull.Store(false)
	if m.currentSize >= m.maxFileBytes {
		if rerr := m.rotateLocked(true); rerr != nil {
			m.appendErrors.Add(1)
			m.recordErrorLocked("rotate metering log", rerr)
			return rerr
		}
	}
	return nil
}

// FlushNow seals the active log file (if non-empty) and flushes all sealed
// batch files in logDir to the database, deleting each file only after receipt confirmation.
func (m *MeteringLogger) FlushNow(ctx context.Context) error {
	m.mu.Lock()
	var rotateErr error
	if m.currentSize > 0 || m.file != nil {
		rotateErr = m.rotateLocked(false)
	} else if info, err := os.Stat(m.activePath); err == nil && info.Size() > 0 {
		rotateErr = m.rotateLocked(false)
	}
	if rotateErr != nil {
		m.appendErrors.Add(1)
		m.recordErrorLocked("rotate on flush", rotateErr)
	}
	m.mu.Unlock()

	flushErr := m.flushPendingFiles(ctx)
	if rotateErr != nil {
		return rotateErr
	}
	return flushErr
}

// SealAndFlush seals any active metering data and flushes all pending batches.
func (m *MeteringLogger) SealAndFlush(ctx context.Context) error {
	return m.FlushNow(ctx)
}

func (m *MeteringLogger) ensureOpenLocked() error {
	if m.file != nil {
		return nil
	}
	f, err := os.OpenFile(m.activePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return err
	}
	m.file = f
	m.currentSize = info.Size()
	return nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func (m *MeteringLogger) nextBatchID() string {
	seq := m.rotateSeq.Add(1)
	var rnd [4]byte
	_, _ = rand.Read(rnd[:])
	return fmt.Sprintf("%s_%d_%d_%s", m.gatewayID, time.Now().UTC().UnixNano(), seq, hex.EncodeToString(rnd[:]))
}

func batchIDFromFilename(name string) (string, bool) {
	if strings.HasPrefix(name, sealedBatchPrefix) && strings.HasSuffix(name, sealedBatchSuffix) {
		id := strings.TrimSuffix(strings.TrimPrefix(name, sealedBatchPrefix), sealedBatchSuffix)
		if id != "" {
			return id, true
		}
	}
	return "", false
}

func (m *MeteringLogger) rotateLocked(notify bool) error {
	if m.file != nil {
		if err := m.file.Sync(); err != nil {
			return fmt.Errorf("sync active metering log: %w", err)
		}
		if err := m.file.Close(); err != nil {
			m.file = nil
			return fmt.Errorf("close active metering log: %w", err)
		}
		m.file = nil
	}
	info, err := os.Stat(m.activePath)
	if err != nil {
		if os.IsNotExist(err) {
			m.currentSize = 0
			return nil
		}
		return err
	}
	if info.Size() == 0 {
		_ = os.Remove(m.activePath)
		m.currentSize = 0
		return nil
	}

	batchID := m.nextBatchID()
	sealedPath := filepath.Join(m.logDir, sealedBatchPrefix+batchID+sealedBatchSuffix)
	if err := os.Rename(m.activePath, sealedPath); err != nil {
		return err
	}
	if err := syncDir(m.logDir); err != nil {
		return fmt.Errorf("sync metering dir after rotate: %w", err)
	}
	m.currentSize = 0

	if notify {
		select {
		case m.flushCh <- sealedPath:
		default:
		}
	}
	return nil
}

func (m *MeteringLogger) preserveLegacyFiles() error {
	entries, err := os.ReadDir(m.logDir)
	if err != nil {
		return err
	}
	for _, ent := range entries {
		if ent.IsDir() {
			continue
		}
		name := ent.Name()
		if strings.HasPrefix(name, rotatedMeteringLogPrefix) && strings.HasSuffix(name, rotatedMeteringLogSuffix) {
			src := filepath.Join(m.logDir, name)
			dst := filepath.Join(m.legacyDir, name)
			if err := os.Rename(src, dst); err != nil {
				return fmt.Errorf("preserve legacy file %s: %w", name, err)
			}
			_ = os.WriteFile(dst+".meta", []byte(fmt.Sprintf(`{"original_name":%q,"preserved_at":%q,"status":"unknown_prior_commit"}`+"\n", name, time.Now().UTC().Format(time.RFC3339))), 0o644)
		}
	}
	return nil
}

func (m *MeteringLogger) flushLoop(ctx context.Context) {
	if err := m.flushPendingFiles(ctx); err != nil {
		m.recordError("startup metering flush", err)
	}

	ticker := time.NewTicker(m.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case path := <-m.flushCh:
			if path != "" {
				if err := m.flushFile(ctx, path); err != nil {
					m.recordError("flush batch "+filepath.Base(path), err)
				}
			}
		case <-ticker.C:
			if err := m.FlushNow(ctx); err != nil {
				m.recordError("periodic metering flush", err)
			}
		}
	}
}

func (m *MeteringLogger) flushPendingFiles(ctx context.Context) error {
	entries, err := os.ReadDir(m.logDir)
	if err != nil {
		m.flushErrors.Add(1)
		m.recordError("read metering dir", err)
		return err
	}
	var sealed []string
	for _, ent := range entries {
		if ent.IsDir() {
			continue
		}
		if _, ok := batchIDFromFilename(ent.Name()); ok {
			sealed = append(sealed, filepath.Join(m.logDir, ent.Name()))
		}
	}
	sort.Strings(sealed)

	var firstErr error
	for _, path := range sealed {
		if err := m.flushFile(ctx, path); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (m *MeteringLogger) quarantineFile(path string, reason error) {
	base := filepath.Base(path)
	dst := filepath.Join(m.quarantineDir, base)
	if _, err := os.Stat(dst); err == nil {
		dst = filepath.Join(m.quarantineDir, fmt.Sprintf("%s.%d", base, time.Now().UTC().UnixNano()))
	}
	if err := os.Rename(path, dst); err != nil {
		m.recordError("quarantine rename "+base, err)
		return
	}
	_ = os.WriteFile(dst+".err", []byte(fmt.Sprintf("%s\nquarantined_at=%s\n", reason.Error(), time.Now().UTC().Format(time.RFC3339))), 0o644)
	_ = syncDir(m.quarantineDir)
}

func (m *MeteringLogger) flushFile(ctx context.Context, path string) error {
	m.flushMu.Lock()
	defer m.flushMu.Unlock()

	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		m.flushErrors.Add(1)
		return err
	}
	sum := sha256.Sum256(raw)
	payloadSHA256 := hex.EncodeToString(sum[:])

	batchID, ok := batchIDFromFilename(filepath.Base(path))
	if !ok {
		batchID = "batch_" + payloadSHA256[:24]
	}

	var parsed []MeteringEntry
	scanner := bufio.NewScanner(strings.NewReader(string(raw)))
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	lineNo := 0
	for scanner.Scan() {
		lineNo++
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		entry, perr := ParseMeteringLine(line)
		if perr != nil {
			qErr := fmt.Errorf("malformed metering line %d in %s: %w", lineNo, filepath.Base(path), perr)
			m.quarantineFile(path, qErr)
			m.flushErrors.Add(1)
			m.recordError("quarantine corrupt batch", qErr)
			return qErr
		}
		parsed = append(parsed, entry)
	}
	if scanErr := scanner.Err(); scanErr != nil {
		qErr := fmt.Errorf("scan metering log %s: %w", filepath.Base(path), scanErr)
		m.quarantineFile(path, qErr)
		m.flushErrors.Add(1)
		m.recordError("scan corrupt batch", qErr)
		return qErr
	}

	if len(parsed) > 0 {
		if m.saver == nil {
			m.flushErrors.Add(1)
			return ErrMeteringSinkUnavailable
		}
		var saveErr error
		if rs, ok := m.saver.(ReceiptMeteringSaver); ok {
			saveErr = rs.SaveMeteringBatchWithReceipt(ctx, batchID, m.gatewayID, payloadSHA256, parsed)
		} else {
			saveErr = m.saver.SaveMeteringBatch(ctx, parsed)
		}
		if saveErr != nil {
			if errors.Is(saveErr, ErrBatchPayloadMismatch) {
				m.quarantineFile(path, saveErr)
			}
			m.flushErrors.Add(1)
			return saveErr
		}
	}

	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		m.flushErrors.Add(1)
		return fmt.Errorf("delete metering log %s: %w", path, err)
	}
	m.mu.Lock()
	m.lastFlushedAt = time.Now().UTC()
	m.mu.Unlock()
	return nil
}

// Status returns the current durable metering spool and sink status.
func (m *MeteringLogger) Status(ctx context.Context) MeteringStatus {
	if m == nil {
		return MeteringStatus{}
	}
	m.mu.Lock()
	st := MeteringStatus{
		GatewayID:      m.gatewayID,
		LogDir:         m.logDir,
		BillingEnabled: m.billingEnabled,
		Writable:       m.writable,
		SinkConfigured: m.saver != nil,
		ActiveBytes:    m.currentSize,
		MaxSpoolBytes:  m.maxSpoolBytes,
		SpoolFull:      m.spoolFull.Load(),
		AppendErrors:   m.appendErrors.Load(),
		FlushErrors:    m.flushErrors.Load(),
		LastError:      m.lastError,
		LastErrorAt:    m.lastErrorAt,
		LastFlushedAt:  m.lastFlushedAt,
	}
	st.SpoolBytes = m.computeSpoolBytesLocked()
	m.mu.Unlock()

	if ents, err := os.ReadDir(m.logDir); err == nil {
		for _, ent := range ents {
			if ent.IsDir() {
				continue
			}
			if _, ok := batchIDFromFilename(ent.Name()); ok {
				st.PendingBatches++
				if info, ierr := ent.Info(); ierr == nil {
					st.PendingBytes += info.Size()
				}
			}
		}
	}
	if ents, err := os.ReadDir(m.quarantineDir); err == nil {
		for _, ent := range ents {
			if !ent.IsDir() && !strings.HasSuffix(ent.Name(), ".err") {
				st.QuarantinedCount++
			}
		}
	}
	if ents, err := os.ReadDir(m.legacyDir); err == nil {
		for _, ent := range ents {
			if !ent.IsDir() && !strings.HasSuffix(ent.Name(), ".meta") {
				st.LegacyPendingCount++
			}
		}
	}

	if m.saver != nil {
		if rs, ok := m.saver.(ReceiptMeteringSaver); ok {
			pctx, cancel := context.WithTimeout(ctx, 2*time.Second)
			st.SinkHealthy = rs.Ping(pctx) == nil
			cancel()
		} else {
			st.SinkHealthy = true
		}
	}

	if st.QuarantinedCount > 0 || st.LegacyPendingCount > 0 || st.SpoolFull || !st.Writable {
		st.Blocked = true
	}
	if st.BillingEnabled && (!st.SinkConfigured || !st.SinkHealthy) {
		st.Blocked = true
	}
	return st
}

// LegacyBatchReport describes a legacy metering file discovered during migration reconciliation.
type LegacyBatchReport struct {
	File          string             `json:"file"`
	LegacyBatchID string             `json:"legacy_batch_id"`
	PayloadSHA256 string             `json:"payload_sha256"`
	EntryCount    int                `json:"entry_count"`
	Status        string             `json:"status"`
	Deltas        []TenantUsageDelta `json:"deltas,omitempty"`
	Error         string             `json:"error,omitempty"`
}

// ReconcileLegacySpool inspects logDir and logDir/legacy_pending for legacy
// query_metering.*.log files, reports their deterministic batch IDs and usage
// deltas, and optionally applies uncommitted batches when apply=true.
func ReconcileLegacySpool(ctx context.Context, logDir, metaDSN string, apply bool) ([]LegacyBatchReport, error) {
	var files []string
	for _, dir := range []string{logDir, filepath.Join(logDir, legacyPendingDirName)} {
		ents, err := os.ReadDir(dir)
		if err != nil {
			continue
		}
		for _, ent := range ents {
			if ent.IsDir() {
				continue
			}
			name := ent.Name()
			if (strings.HasPrefix(name, rotatedMeteringLogPrefix) && strings.HasSuffix(name, rotatedMeteringLogSuffix)) || name == activeMeteringLogName {
				if info, ierr := ent.Info(); ierr == nil && info.Size() > 0 {
					files = append(files, filepath.Join(dir, name))
				}
			}
		}
	}
	sort.Strings(files)

	var saver *PGMeteringSaver
	if metaDSN != "" {
		saver = NewPGMeteringSaver(metaDSN)
		defer saver.Close()
	}

	reports := make([]LegacyBatchReport, 0, len(files))
	for _, path := range files {
		raw, err := os.ReadFile(path)
		if err != nil {
			reports = append(reports, LegacyBatchReport{File: path, Status: "read_error", Error: err.Error()})
			continue
		}
		sum := sha256.Sum256(raw)
		hashHex := hex.EncodeToString(sum[:])
		batchID := "legacy_" + hashHex[:24]

		var entries []MeteringEntry
		var parseErr error
		scanner := bufio.NewScanner(strings.NewReader(string(raw)))
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line == "" {
				continue
			}
			e, perr := ParseMeteringLine(line)
			if perr != nil {
				parseErr = perr
				break
			}
			entries = append(entries, e)
		}
		if parseErr != nil || scanner.Err() != nil {
			msg := ""
			if parseErr != nil {
				msg = parseErr.Error()
			} else {
				msg = scanner.Err().Error()
			}
			reports = append(reports, LegacyBatchReport{
				File:          path,
				LegacyBatchID: batchID,
				PayloadSHA256: hashHex,
				Status:        "corrupt",
				Error:         msg,
			})
			continue
		}
		rep := LegacyBatchReport{
			File:          path,
			LegacyBatchID: batchID,
			PayloadSHA256: hashHex,
			EntryCount:    len(entries),
			Status:        "unknown_prior_commit",
			Deltas:        AggregateMeteringEntries(entries),
		}
		if apply && saver != nil && len(entries) > 0 {
			if err := saver.SaveMeteringBatchWithReceipt(ctx, batchID, "legacy_reconcile", hashHex, entries); err != nil {
				rep.Status = "apply_error"
				rep.Error = err.Error()
			} else {
				rep.Status = "reconciled"
				_ = os.Remove(path)
				_ = os.Remove(path + ".meta")
			}
		}
		reports = append(reports, rep)
	}
	return reports, nil
}

// MarshalJSONReport formats LegacyBatchReport slice as indented JSON.
func MarshalLegacyReport(reports []LegacyBatchReport) string {
	b, _ := json.MarshalIndent(reports, "", "  ")
	return string(b)
}
