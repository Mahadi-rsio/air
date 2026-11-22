package gateway

import (
	"bufio"
	"context"
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
	defaultMeteringMaxFileBytes  = int64(10 * 1024 * 1024) // 10 MB
	defaultMeteringFlushInterval = 2 * time.Minute
	activeMeteringLogName        = "query_metering.log"
	rotatedMeteringLogPrefix     = "query_metering."
	rotatedMeteringLogSuffix     = ".log"
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

// PGMeteringSaver persists metering batches to the PostgreSQL metadata database.
type PGMeteringSaver struct {
	dsn           string
	mu            sync.Mutex
	conn          *pgx.Conn
	schemaEnsured bool
}

// NewPGMeteringSaver creates a PostgreSQL-backed MeteringSaver for metaDSN.
func NewPGMeteringSaver(metaDSN string) *PGMeteringSaver {
	return &PGMeteringSaver{dsn: metaDSN}
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
			`CREATE TABLE IF NOT EXISTS query_metering (
				id BIGSERIAL PRIMARY KEY,
				recorded_at TIMESTAMPTZ NOT NULL,
				db_id TEXT NOT NULL,
				bandwidth_bytes BIGINT NOT NULL,
				query_time_us BIGINT NOT NULL,
				query_type TEXT NOT NULL,
				created_at TIMESTAMPTZ NOT NULL DEFAULT now()
			)`,
			`CREATE INDEX IF NOT EXISTS idx_query_metering_db_recorded ON query_metering(db_id, recorded_at DESC)`,
			`CREATE INDEX IF NOT EXISTS idx_query_metering_recorded ON query_metering(recorded_at DESC)`,
		}
		for _, stmt := range stmts {
			if _, err := conn.Exec(ctx, stmt); err != nil {
				_ = conn.Close(context.Background())
				s.conn = nil
				return nil, fmt.Errorf("ensure query_metering schema failed: %w", err)
			}
		}
		s.schemaEnsured = true
	}
	return s.conn, nil
}

// SaveMeteringBatch inserts a batch of MeteringEntry rows using PostgreSQL COPY.
func (s *PGMeteringSaver) SaveMeteringBatch(ctx context.Context, entries []MeteringEntry) error {
	if len(entries) == 0 {
		return nil
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

	rows := make([][]any, 0, len(entries))
	for _, e := range entries {
		rows = append(rows, []any{
			e.Date.UTC(),
			e.DBID,
			int64(e.BandwidthBytes),
			int64(e.QueryTimeUs),
			e.QueryType,
		})
	}

	_, err = tx.CopyFrom(
		ctx,
		pgx.Identifier{"query_metering"},
		[]string{"recorded_at", "db_id", "bandwidth_bytes", "query_time_us", "query_type"},
		pgx.CopyFromRows(rows),
	)
	if err != nil {
		return fmt.Errorf("copy query_metering failed: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit query_metering failed: %w", err)
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

// MeteringLogger buffers query metering entries to a log file and flushes them
// to the database whenever the file reaches maxFileBytes (default 10MB) or
// flushInterval (default 2 minutes) completes, deleting the log file afterwards.
type MeteringLogger struct {
	logDir        string
	activePath    string
	maxFileBytes  int64
	flushInterval time.Duration
	saver         MeteringSaver

	mu          sync.Mutex
	file        *os.File
	currentSize int64
	rotateSeq   atomic.Uint64

	running atomic.Bool
	cancel  context.CancelFunc
	flushCh chan string
	wg      sync.WaitGroup
	flushMu sync.Mutex
}

// NewMeteringLogger creates a MeteringLogger writing to logDir.
func NewMeteringLogger(logDir string, maxFileBytes int64, flushInterval time.Duration, saver MeteringSaver) (*MeteringLogger, error) {
	if strings.TrimSpace(logDir) == "" {
		logDir = filepath.Join(os.TempDir(), "air-gateway-metering")
	}
	if maxFileBytes <= 0 {
		maxFileBytes = defaultMeteringMaxFileBytes
	}
	if flushInterval <= 0 {
		flushInterval = defaultMeteringFlushInterval
	}
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		return nil, fmt.Errorf("create metering log dir %q: %w", logDir, err)
	}
	return &MeteringLogger{
		logDir:        logDir,
		activePath:    filepath.Join(logDir, activeMeteringLogName),
		maxFileBytes:  maxFileBytes,
		flushInterval: flushInterval,
		saver:         saver,
		flushCh:       make(chan string, 64),
	}, nil
}

// Start begins the background 2-minute interval flush loop and processes any
// leftover rotated log files from a prior run.
func (m *MeteringLogger) Start() {
	if m.running.Swap(true) {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	m.cancel = cancel

	// Recover any leftover active or rotated log files from an earlier run.
	if info, err := os.Stat(m.activePath); err == nil && info.Size() > 0 {
		m.mu.Lock()
		_ = m.rotateLocked(false)
		m.mu.Unlock()
	}

	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		m.flushLoop(ctx)
	}()
}

// Stop stops the background flush loop, rotates any remaining active log file,
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
	_ = m.FlushNow(ctx)

	m.mu.Lock()
	if m.file != nil {
		_ = m.file.Close()
		m.file = nil
	}
	m.mu.Unlock()

	if pgSaver, ok := m.saver.(*PGMeteringSaver); ok && pgSaver != nil {
		pgSaver.Close()
	}
}

// LogQuery appends a query metering record to the active log file and triggers
// rotation + flush if the file size reaches maxFileBytes.
func (m *MeteringLogger) LogQuery(entry MeteringEntry) {
	line := FormatMeteringLine(entry)

	m.mu.Lock()
	if err := m.ensureOpenLocked(); err != nil {
		m.mu.Unlock()
		log.Printf("[air-gateway] metering log open error: %v", err)
		return
	}
	n, err := m.file.WriteString(line)
	if err != nil {
		m.mu.Unlock()
		log.Printf("[air-gateway] metering log write error: %v", err)
		return
	}
	m.currentSize += int64(n)
	if m.currentSize >= m.maxFileBytes {
		_ = m.rotateLocked(true)
	}
	m.mu.Unlock()
}

// FlushNow rotates the active log file (if non-empty) and flushes all rotated
// log files in logDir to the database, deleting each file after persistence.
func (m *MeteringLogger) FlushNow(ctx context.Context) error {
	m.mu.Lock()
	if m.currentSize > 0 || m.file != nil {
		_ = m.rotateLocked(false)
	} else if info, err := os.Stat(m.activePath); err == nil && info.Size() > 0 {
		_ = m.rotateLocked(false)
	}
	m.mu.Unlock()

	return m.flushPendingFiles(ctx)
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

func (m *MeteringLogger) rotateLocked(notify bool) error {
	if m.file != nil {
		_ = m.file.Sync()
		_ = m.file.Close()
		m.file = nil
	}
	info, err := os.Stat(m.activePath)
	if err != nil || info.Size() == 0 {
		if err == nil && info.Size() == 0 {
			_ = os.Remove(m.activePath)
		}
		m.currentSize = 0
		return nil
	}

	seq := m.rotateSeq.Add(1)
	rotatedPath := filepath.Join(
		m.logDir,
		fmt.Sprintf("%s%d.%d%s", rotatedMeteringLogPrefix, time.Now().UTC().UnixNano(), seq, rotatedMeteringLogSuffix),
	)
	if err := os.Rename(m.activePath, rotatedPath); err != nil {
		return err
	}
	m.currentSize = 0

	if notify {
		select {
		case m.flushCh <- rotatedPath:
		default:
		}
	}
	return nil
}

func (m *MeteringLogger) flushLoop(ctx context.Context) {
	// Flush any files left over on startup.
	_ = m.flushPendingFiles(ctx)

	ticker := time.NewTicker(m.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case path := <-m.flushCh:
			if path != "" {
				if err := m.flushFile(ctx, path); err != nil {
					log.Printf("[air-gateway] metering flush error (%s): %v", path, err)
				}
			}
		case <-ticker.C:
			if err := m.FlushNow(ctx); err != nil {
				log.Printf("[air-gateway] periodic metering flush error: %v", err)
			}
		}
	}
}

func (m *MeteringLogger) flushPendingFiles(ctx context.Context) error {
	entries, err := os.ReadDir(m.logDir)
	if err != nil {
		return err
	}
	var rotated []string
	for _, ent := range entries {
		if ent.IsDir() {
			continue
		}
		name := ent.Name()
		if strings.HasPrefix(name, rotatedMeteringLogPrefix) && strings.HasSuffix(name, rotatedMeteringLogSuffix) {
			rotated = append(rotated, filepath.Join(m.logDir, name))
		}
	}
	sort.Strings(rotated)

	var firstErr error
	for _, path := range rotated {
		if err := m.flushFile(ctx, path); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (m *MeteringLogger) flushFile(ctx context.Context, path string) error {
	m.flushMu.Lock()
	defer m.flushMu.Unlock()

	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	var parsed []MeteringEntry
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		entry, perr := ParseMeteringLine(line)
		if perr != nil {
			log.Printf("[air-gateway] skipping malformed metering log line in %s: %v", path, perr)
			continue
		}
		parsed = append(parsed, entry)
	}
	scanErr := scanner.Err()
	_ = f.Close()
	if scanErr != nil {
		return fmt.Errorf("scan metering log %s: %w", path, scanErr)
	}

	if len(parsed) > 0 && m.saver != nil {
		if err := m.saver.SaveMeteringBatch(ctx, parsed); err != nil {
			return err
		}
	}

	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("delete metering log %s: %w", path, err)
	}
	return nil
}
