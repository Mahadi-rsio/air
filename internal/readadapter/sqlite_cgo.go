package readadapter

/*
#cgo LDFLAGS: -lsqlite3
#include <sqlite3.h>
#include <stdlib.h>
#include <string.h>

static int air_sqlite_ro_authorizer(void *user_data, int action, const char *arg1, const char *arg2, const char *db_name, const char *trigger_name) {
	int *strict = (int *)user_data;
	if (!strict || *strict == 0) {
		return SQLITE_OK;
	}
	switch (action) {
		case SQLITE_SELECT:
		case SQLITE_READ:
		case SQLITE_FUNCTION:
#ifdef SQLITE_RECURSIVE
		case SQLITE_RECURSIVE:
#endif
			return SQLITE_OK;
		case SQLITE_TRANSACTION:
			if (arg1 != NULL && (
				strcasecmp(arg1, "BEGIN") == 0 ||
				strcasecmp(arg1, "COMMIT") == 0 ||
				strcasecmp(arg1, "ROLLBACK") == 0)) {
				return SQLITE_OK;
			}
			return SQLITE_DENY;
		case SQLITE_PRAGMA:
			if (arg1 != NULL && (
				strcasecmp(arg1, "table_info") == 0 ||
				strcasecmp(arg1, "index_list") == 0 ||
				strcasecmp(arg1, "index_info") == 0 ||
				strcasecmp(arg1, "database_list") == 0 ||
				strcasecmp(arg1, "compile_options") == 0 ||
				strcasecmp(arg1, "query_only") == 0 ||
				strcasecmp(arg1, "cache_size") == 0)) {
				if (arg2 == NULL || strcasecmp(arg1, "table_info") == 0 || strcasecmp(arg1, "index_list") == 0 || strcasecmp(arg1, "index_info") == 0) {
					return SQLITE_OK;
				}
			}
			return SQLITE_DENY;
		default:
			return SQLITE_DENY;
	}
}

static void air_install_ro_authorizer(sqlite3 *db, int *flag_ptr) {
	sqlite3_set_authorizer(db, air_sqlite_ro_authorizer, (void *)flag_ptr);
}

static int air_bind_text(sqlite3_stmt *stmt, int idx, const char *val, int n) {
	return sqlite3_bind_text(stmt, idx, val, n, SQLITE_TRANSIENT);
}

static int air_bind_blob(sqlite3_stmt *stmt, int idx, const void *val, int n) {
	return sqlite3_bind_blob(stmt, idx, val, n, SQLITE_TRANSIENT);
}

static int air_progress_cb(void *arg) {
	int *cancelled = (int *)arg;
	if (cancelled != NULL && __atomic_load_n(cancelled, __ATOMIC_RELAXED) != 0) {
		return 1;
	}
	return 0;
}

static void air_set_cancelled(int *cancelled) {
	if (cancelled != NULL) {
		__atomic_store_n(cancelled, 1, __ATOMIC_RELAXED);
	}
}

static void air_reset_cancelled(int *cancelled) {
	if (cancelled != NULL) {
		__atomic_store_n(cancelled, 0, __ATOMIC_RELAXED);
	}
}

static void air_install_progress_handler(sqlite3 *db, int *cancelled) {
	if (cancelled != NULL) {
		sqlite3_progress_handler(db, 100, air_progress_cb, (void *)cancelled);
	} else {
		sqlite3_progress_handler(db, 0, NULL, NULL);
	}
}
*/
import "C"

import (
	"container/list"
	"context"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"unsafe"
)

// SQLiteValue represents a typed value returned or bound in SQLite.
type SQLiteValue struct {
	Type  int // C.SQLITE_INTEGER, C.SQLITE_FLOAT, C.SQLITE_TEXT, C.SQLITE_BLOB, C.SQLITE_NULL
	Int64 int64
	Float float64
	Text  string
	Blob  []byte
	IsNil bool
}

// SQLiteColumn describes a result column from a prepared SQLite query.
type SQLiteColumn struct {
	Name     string
	DeclType string
	PGType   uint32
	PGSize   int16
}

// SQLiteResult holds the result set and command tag of a SQLite execution.
type SQLiteResult struct {
	Columns     []SQLiteColumn
	Rows        [][]SQLiteValue
	RowsChanged int64
	IsSelect    bool
}

const maxCachedSQLiteStmts = 64

type stmtCacheKey struct {
	SQL       string
	SchemaVer uint64
}

type cachedSQLiteStmt struct {
	key  stmtCacheKey
	stmt *C.sqlite3_stmt
	cols []SQLiteColumn
}

// SQLiteConn wraps a single native *C.sqlite3 connection with optional read-only authorizer enforcement.
type SQLiteConn struct {
	mu         sync.Mutex
	cancelMu   sync.Mutex
	db         *C.sqlite3
	roFlag     *C.int
	cancelFlag *C.int
	path       string
	readOnly   bool

	stmtLRU        *list.List
	stmtCache      map[stmtCacheKey]*list.Element
	stmtCacheHits  atomic.Uint64
	stmtCacheMiss  atomic.Uint64
}

// OpenSQLiteConn opens a SQLite database at path. If readOnly is true, the database
// is opened with SQLITE_OPEN_READONLY and a strict C authorizer is installed.
func OpenSQLiteConn(path string, readOnly bool, cacheKB int) (*SQLiteConn, error) {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))

	flags := C.int(C.SQLITE_OPEN_NOMUTEX | C.SQLITE_OPEN_URI)
	if readOnly {
		flags |= C.SQLITE_OPEN_READONLY
	} else {
		flags |= C.SQLITE_OPEN_READWRITE | C.SQLITE_OPEN_CREATE
	}

	var db *C.sqlite3
	rc := C.sqlite3_open_v2(cpath, &db, flags, nil)
	if rc != C.SQLITE_OK {
		var msg string
		if db != nil {
			msg = C.GoString(C.sqlite3_errmsg(db))
			C.sqlite3_close(db)
		}
		return nil, fmt.Errorf("sqlite3_open_v2(%s): rc=%d %s", path, int(rc), msg)
	}

	C.sqlite3_busy_timeout(db, 5000)

	roPtr := (*C.int)(C.malloc(C.size_t(unsafe.Sizeof(C.int(0)))))
	*roPtr = 0
	cancelPtr := (*C.int)(C.malloc(C.size_t(unsafe.Sizeof(C.int(0)))))
	*cancelPtr = 0

	conn := &SQLiteConn{
		db:         db,
		roFlag:     roPtr,
		cancelFlag: cancelPtr,
		path:       path,
		readOnly:   readOnly,
		stmtLRU:    list.New(),
		stmtCache:  make(map[stmtCacheKey]*list.Element),
	}

	if !readOnly {
		_ = conn.execRaw("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA temp_store=MEMORY;")
	}
	if cacheKB > 0 {
		_ = conn.execRaw(fmt.Sprintf("PRAGMA cache_size=-%d;", cacheKB))
	}
	if readOnly {
		_ = conn.execRaw("PRAGMA query_only=ON; PRAGMA temp_store=MEMORY;")
		*roPtr = 1
		C.air_install_ro_authorizer(db, roPtr)
	}
	if err := conn.registerPGCollations(); err != nil {
		_ = conn.Close()
		return nil, err
	}
	if err := conn.registerPGFuncs(); err != nil {
		_ = conn.Close()
		return nil, err
	}

	return conn, nil
}

// SetReadOnlyAuthorizer toggles the C-level read-only authorizer on this connection.
func (c *SQLiteConn) SetReadOnlyAuthorizer(enabled bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.db == nil || c.roFlag == nil {
		return
	}
	if enabled {
		*c.roFlag = 1
		C.air_install_ro_authorizer(c.db, c.roFlag)
	} else {
		*c.roFlag = 0
		C.sqlite3_set_authorizer(c.db, nil, nil)
	}
}

// InvalidatePreparedStmts finalizes all cached compiled sqlite3_stmt handles on this connection.
func (c *SQLiteConn) InvalidatePreparedStmts() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.clearStmtCacheLocked()
}

func (c *SQLiteConn) clearStmtCacheLocked() {
	if c.stmtLRU == nil {
		return
	}
	for elem := c.stmtLRU.Front(); elem != nil; elem = elem.Next() {
		ent := elem.Value.(*cachedSQLiteStmt)
		if ent.stmt != nil {
			C.sqlite3_finalize(ent.stmt)
			ent.stmt = nil
		}
	}
	c.stmtLRU.Init()
	clear(c.stmtCache)
}

// StmtCacheStats returns compiled sqlite3_stmt cache hit/miss counts.
func (c *SQLiteConn) StmtCacheStats() (hits, misses uint64) {
	if c == nil {
		return 0, 0
	}
	return c.stmtCacheHits.Load(), c.stmtCacheMiss.Load()
}

// Close closes the underlying SQLite connection and frees C resources.
func (c *SQLiteConn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cancelMu.Lock()
	defer c.cancelMu.Unlock()
	c.clearStmtCacheLocked()
	if c.db != nil {
		C.air_install_progress_handler(c.db, nil)
		C.sqlite3_set_authorizer(c.db, nil, nil)
		C.sqlite3_close_v2(c.db)
		c.db = nil
	}
	if c.roFlag != nil {
		C.free(unsafe.Pointer(c.roFlag))
		c.roFlag = nil
	}
	if c.cancelFlag != nil {
		C.free(unsafe.Pointer(c.cancelFlag))
		c.cancelFlag = nil
	}
	return nil
}

func (c *SQLiteConn) execRaw(sql string) error {
	csql := C.CString(sql)
	defer C.free(unsafe.Pointer(csql))
	var errmsg *C.char
	rc := C.sqlite3_exec(c.db, csql, nil, nil, &errmsg)
	if rc != C.SQLITE_OK {
		msg := ""
		if errmsg != nil {
			msg = C.GoString(errmsg)
			C.sqlite3_free(unsafe.Pointer(errmsg))
		}
		return fmt.Errorf("sqlite3_exec: %s (rc=%d)", msg, int(rc))
	}
	return nil
}

// Exec runs one or more non-returning SQL statements (used internally for sync/setup).
func (c *SQLiteConn) Exec(sql string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.db == nil {
		return fmt.Errorf("sqlite connection closed")
	}
	return c.execRaw(sql)
}

// PostgreSQL OID constants for wire protocol RowDescription.
const (
	PGOIDBool    uint32 = 16
	PGOIDBytea   uint32 = 17
	PGOIDInt8    uint32 = 20
	PGOIDInt4    uint32 = 23
	PGOIDText    uint32 = 25
	PGOIDFloat8  uint32 = 701
	PGOIDNumeric uint32 = 1700
)

func declTypeToPG(decl string) (uint32, int16) {
	u := ""
	for i := 0; i < len(decl); i++ {
		ch := decl[i]
		if ch >= 'a' && ch <= 'z' {
			ch -= 32
		}
		u += string(ch)
	}
	switch {
	case u == "BOOL" || u == "BOOLEAN":
		return PGOIDBool, 1
	case u == "INT" || u == "INT4" || u == "INTEGER" || u == "SMALLINT" || u == "INT2":
		return PGOIDInt4, 4
	case u == "BIGINT" || u == "INT8":
		return PGOIDInt8, 8
	case u == "REAL" || u == "FLOAT" || u == "FLOAT8" || u == "DOUBLE" || u == "DOUBLE PRECISION":
		return PGOIDFloat8, 8
	case u == "BLOB" || u == "BYTEA":
		return PGOIDBytea, -1
	case u == "TEXT_NUMERIC" || strings.Contains(u, "TEXT_NUMERIC"):
		return PGOIDNumeric, -1
	case u == "TEXT_INET" || strings.Contains(u, "TEXT_INET"):
		return PGOIDInet, -1
	case u == "TEXT_CIDR" || strings.Contains(u, "TEXT_CIDR"):
		return PGOIDCIDR, -1
	case u == "TEXT_INT2ARRAY":
		return PGOIDInt2Array, -1
	case u == "TEXT_INT4ARRAY":
		return PGOIDInt4Array, -1
	case u == "TEXT_INT8ARRAY":
		return PGOIDInt8Array, -1
	case u == "TEXT_TEXTARRAY":
		return PGOIDTextArray, -1
	case u == "TEXT_BOOLARRAY":
		return PGOIDBoolArray, -1
	case u == "TEXT_FLOAT4ARRAY":
		return PGOIDFloat4Array, -1
	case u == "TEXT_FLOAT8ARRAY":
		return PGOIDFloat8Array, -1
	default:
		return PGOIDText, -1
	}
}

// Query executes a SQL statement with optional positional parameters and returns all rows.
func (c *SQLiteConn) Query(sql string, args []string) (*SQLiteResult, error) {
	return c.QueryWithContext(context.Background(), nil, sql, args)
}

// QueryWithContext executes a SQL statement under ctx and an optional shared ReadSnapshot.
func (c *SQLiteConn) QueryWithContext(ctx context.Context, snap *ReadSnapshot, sql string, args []string) (*SQLiteResult, error) {
	res, _, err := c.queryWithContextInternal(ctx, snap, sql, args, nil, 0, false)
	return res, err
}

// QueryWithContextPlan executes a SQL statement under ctx and an optional shared ReadSnapshot,
// also returning the last AIR virtual-table PlanDecision recorded during execution (if any).
func (c *SQLiteConn) QueryWithContextPlan(ctx context.Context, snap *ReadSnapshot, sql string, args []string) (*SQLiteResult, *PlanDecision, error) {
	return c.queryWithContextInternal(ctx, snap, sql, args, nil, 0, false)
}

// QueryWithContextPlanTyped executes a SQL statement using the bounded compiled-statement
// cache (keyed by sql + schemaVer) and typed PostgreSQL->SQLite parameter binding.
func (c *SQLiteConn) QueryWithContextPlanTyped(ctx context.Context, snap *ReadSnapshot, sql string, args []string, paramOIDs []uint32, schemaVer uint64) (*SQLiteResult, *PlanDecision, error) {
	return c.queryWithContextInternal(ctx, snap, sql, args, paramOIDs, schemaVer, true)
}

func bindSQLiteParamTyped(db *C.sqlite3, stmt *C.sqlite3_stmt, idx C.int, arg string, oid uint32) error {
	if arg == "\x00NULL\x00" {
		C.sqlite3_bind_null(stmt, idx)
		return nil
	}
	switch oid {
	case PGOIDInt4, PGOIDInt8, 21: // int4, int8, int2
		if iv, err := strconv.ParseInt(arg, 10, 64); err == nil {
			if rc := C.sqlite3_bind_int64(stmt, idx, C.sqlite3_int64(iv)); rc != C.SQLITE_OK {
				return fmt.Errorf("bind int64 param %d: %s", int(idx), C.GoString(C.sqlite3_errmsg(db)))
			}
			return nil
		}
	case PGOIDFloat8, 700: // float8, float4
		if fv, err := strconv.ParseFloat(arg, 64); err == nil {
			if rc := C.sqlite3_bind_double(stmt, idx, C.double(fv)); rc != C.SQLITE_OK {
				return fmt.Errorf("bind double param %d: %s", int(idx), C.GoString(C.sqlite3_errmsg(db)))
			}
			return nil
		}
	case PGOIDBool:
		var bv int64
		switch arg {
		case "1", "t", "T", "true", "TRUE":
			bv = 1
		case "0", "f", "F", "false", "FALSE":
			bv = 0
		default:
			goto bindText
		}
		if rc := C.sqlite3_bind_int64(stmt, idx, C.sqlite3_int64(bv)); rc != C.SQLITE_OK {
			return fmt.Errorf("bind bool param %d: %s", int(idx), C.GoString(C.sqlite3_errmsg(db)))
		}
		return nil
	}
bindText:
	carg := C.CString(arg)
	brc := C.air_bind_text(stmt, idx, carg, C.int(len(arg)))
	C.free(unsafe.Pointer(carg))
	if brc != C.SQLITE_OK {
		return fmt.Errorf("bind param %d: %s", int(idx), C.GoString(C.sqlite3_errmsg(db)))
	}
	return nil
}

func (c *SQLiteConn) queryWithContextInternal(ctx context.Context, snap *ReadSnapshot, sql string, args []string, paramOIDs []uint32, schemaVer uint64, useStmtCache bool) (*SQLiteResult, *PlanDecision, error) {
	return c.queryStreamInternal(ctx, snap, sql, args, paramOIDs, schemaVer, useStmtCache, 0, nil, nil)
}

// QueryStreamWithContext executes sql and streams result rows in bounded batches
// of batchSize rows to onBatch, calling onMeta once before the first batch.
func (c *SQLiteConn) QueryStreamWithContext(
	ctx context.Context,
	snap *ReadSnapshot,
	sql string,
	args []string,
	paramOIDs []uint32,
	schemaVer uint64,
	batchSize int,
	onMeta func([]SQLiteColumn) error,
	onBatch func([][]SQLiteValue) error,
) (*SQLiteResult, *PlanDecision, error) {
	if batchSize <= 0 {
		batchSize = 128
	}
	return c.queryStreamInternal(ctx, snap, sql, args, paramOIDs, schemaVer, true, batchSize, onMeta, onBatch)
}

func (c *SQLiteConn) queryStreamInternal(
	ctx context.Context,
	snap *ReadSnapshot,
	sql string,
	args []string,
	paramOIDs []uint32,
	schemaVer uint64,
	useStmtCache bool,
	batchSize int,
	onMeta func([]SQLiteColumn) error,
	onBatch func([][]SQLiteValue) error,
) (*SQLiteResult, *PlanDecision, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.db == nil {
		return nil, nil, fmt.Errorf("sqlite connection closed")
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}

	if ctx.Done() != nil && c.cancelFlag != nil {
		C.air_reset_cancelled(c.cancelFlag)
		C.air_install_progress_handler(c.db, c.cancelFlag)
		stopAfter := context.AfterFunc(ctx, func() {
			c.cancelMu.Lock()
			if c.db != nil && c.cancelFlag != nil {
				C.air_set_cancelled(c.cancelFlag)
				C.sqlite3_interrupt(c.db)
			}
			c.cancelMu.Unlock()
		})
		defer func() {
			stopAfter()
			C.air_install_progress_handler(c.db, nil)
		}()
	}

	qctx := &queryExecContext{
		ctx:  ctx,
		snap: snap,
	}
	setActiveQueryContext(unsafe.Pointer(c.db), qctx)
	defer clearActiveQueryContext(unsafe.Pointer(c.db))

	var stmt *C.sqlite3_stmt
	var cachedCols []SQLiteColumn
	fromCache := false
	key := stmtCacheKey{SQL: sql, SchemaVer: schemaVer}
	if useStmtCache && c.stmtCache != nil {
		if elem, ok := c.stmtCache[key]; ok {
			ent := elem.Value.(*cachedSQLiteStmt)
			c.stmtLRU.MoveToFront(elem)
			stmt = ent.stmt
			cachedCols = ent.cols
			fromCache = true
			c.stmtCacheHits.Add(1)
			C.sqlite3_reset(stmt)
			C.sqlite3_clear_bindings(stmt)
		}
	}

	if stmt == nil {
		if useStmtCache {
			c.stmtCacheMiss.Add(1)
		}
		csql := C.CString(sql)
		var tail *C.char
		rc := C.sqlite3_prepare_v2(c.db, csql, -1, &stmt, &tail)
		C.free(unsafe.Pointer(csql))
		if rc != C.SQLITE_OK {
			msg := C.GoString(C.sqlite3_errmsg(c.db))
			if rc == C.SQLITE_AUTH {
				return nil, nil, &SQLiteReadOnlyError{Message: msg}
			}
			return nil, nil, fmt.Errorf("%s", msg)
		}
		if stmt == nil {
			// Empty statement or comment-only query.
			return &SQLiteResult{}, nil, nil
		}
		if C.sqlite3_stmt_readonly(stmt) == 0 && c.readOnly {
			C.sqlite3_finalize(stmt)
			return nil, nil, &SQLiteReadOnlyError{Message: "statement is not read-only"}
		}
		if useStmtCache && c.stmtCache != nil && (tail == nil || *tail == 0) {
			colCount := int(C.sqlite3_column_count(stmt))
			cols := make([]SQLiteColumn, colCount)
			for i := 0; i < colCount; i++ {
				cname := C.GoString(C.sqlite3_column_name(stmt, C.int(i)))
				var decl string
				if cdecl := C.sqlite3_column_decltype(stmt, C.int(i)); cdecl != nil {
					decl = C.GoString(cdecl)
				}
				oid, sz := declTypeToPG(decl)
				cols[i] = SQLiteColumn{
					Name:     cname,
					DeclType: decl,
					PGType:   oid,
					PGSize:   sz,
				}
			}
			cachedCols = cols
			for c.stmtLRU.Len() >= maxCachedSQLiteStmts {
				back := c.stmtLRU.Back()
				if back == nil {
					break
				}
				old := back.Value.(*cachedSQLiteStmt)
				c.stmtLRU.Remove(back)
				delete(c.stmtCache, old.key)
				if old.stmt != nil {
					C.sqlite3_finalize(old.stmt)
				}
			}
			ent := &cachedSQLiteStmt{key: key, stmt: stmt, cols: cols}
			c.stmtCache[key] = c.stmtLRU.PushFront(ent)
			fromCache = true
		}
	}

	if fromCache {
		defer func() {
			C.sqlite3_reset(stmt)
			C.sqlite3_clear_bindings(stmt)
		}()
	} else {
		defer C.sqlite3_finalize(stmt)
	}

	for i, arg := range args {
		var oid uint32
		if i < len(paramOIDs) {
			oid = paramOIDs[i]
		}
		if berr := bindSQLiteParamTyped(c.db, stmt, C.int(i+1), arg, oid); berr != nil {
			return nil, nil, berr
		}
	}

	var cols []SQLiteColumn
	colCount := int(C.sqlite3_column_count(stmt))
	if cachedCols != nil {
		cols = make([]SQLiteColumn, len(cachedCols))
		copy(cols, cachedCols)
	} else {
		cols = make([]SQLiteColumn, colCount)
		for i := 0; i < colCount; i++ {
			cname := C.GoString(C.sqlite3_column_name(stmt, C.int(i)))
			var decl string
			if cdecl := C.sqlite3_column_decltype(stmt, C.int(i)); cdecl != nil {
				decl = C.GoString(cdecl)
			}
			oid, sz := declTypeToPG(decl)
			cols[i] = SQLiteColumn{
				Name:     cname,
				DeclType: decl,
				PGType:   oid,
				PGSize:   sz,
			}
		}
	}

	res := &SQLiteResult{
		Columns:  cols,
		IsSelect: colCount > 0,
	}

	budget := MemoryBudgetFromContext(ctx)
	limits, hasLimits := ResourceLimitsFromContext(ctx)
	const valStructSize = int64(unsafe.Sizeof(SQLiteValue{}))
	// Arena-style slab allocation for row slices reduces per-row heap allocations from O(N) to O(log N).
	var rowArena []SQLiteValue
	arenaOff := 0
	allocRow := func(n int) []SQLiteValue {
		if n == 0 {
			return nil
		}
		if arenaOff+n > len(rowArena) {
			slabRows := 16
			if len(res.Rows) >= 64 {
				slabRows = 128
			}
			rowArena = make([]SQLiteValue, slabRows*n)
			arenaOff = 0
		}
		s := rowArena[arenaOff : arenaOff+n : arenaOff+n]
		arenaOff += n
		return s
	}

	var totalRows int64
	var batchBytes int64
	metaSent := false
	streaming := onBatch != nil && batchSize > 0
	if streaming {
		res.Rows = make([][]SQLiteValue, 0, batchSize)
	}
	defer func() {
		if budget != nil && batchBytes > 0 {
			budget.ReleaseBytes(batchBytes)
			batchBytes = 0
		}
		if streaming {
			res.Rows = nil
		}
	}()

	flushBatch := func() error {
		if !streaming {
			return nil
		}
		if !metaSent {
			metaSent = true
			if onMeta != nil {
				if err := onMeta(res.Columns); err != nil {
					return err
				}
			}
		}
		if len(res.Rows) > 0 {
			if err := onBatch(res.Rows); err != nil {
				return err
			}
			res.Rows = res.Rows[:0]
			arenaOff = 0
		}
		if budget != nil && batchBytes > 0 {
			budget.ReleaseBytes(batchBytes)
			batchBytes = 0
		}
		return nil
	}

	for {
		if streaming {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return nil, nil, ctxErr
			}
		}
		stepRC := C.sqlite3_step(stmt)
		if stepRC == C.SQLITE_ROW {
			if hasLimits && limits.MaxResultRows > 0 && totalRows >= limits.MaxResultRows {
				return nil, nil, fmt.Errorf("%w: max rows %d exceeded", ErrResultLimitExceeded, limits.MaxResultRows)
			}
			row := allocRow(colCount)
			var rowBytes int64 = int64(colCount)*valStructSize + 24
			for i := 0; i < colCount; i++ {
				ct := int(C.sqlite3_column_type(stmt, C.int(i)))
				switch ct {
				case C.SQLITE_NULL:
					row[i] = SQLiteValue{Type: ct, IsNil: true}
				case C.SQLITE_INTEGER:
					iv := int64(C.sqlite3_column_int64(stmt, C.int(i)))
					txt := strconv.FormatInt(iv, 10)
					row[i] = SQLiteValue{
						Type:  ct,
						Int64: iv,
						Text:  txt,
					}
					rowBytes += int64(len(txt))
					if res.Columns[i].DeclType == "" {
						res.Columns[i].PGType = PGOIDInt8
						res.Columns[i].PGSize = 8
					}
				case C.SQLITE_FLOAT:
					fv := float64(C.sqlite3_column_double(stmt, C.int(i)))
					txt := strconv.FormatFloat(fv, 'g', -1, 64)
					row[i] = SQLiteValue{
						Type:  ct,
						Float: fv,
						Text:  txt,
					}
					rowBytes += int64(len(txt))
					if res.Columns[i].DeclType == "" {
						res.Columns[i].PGType = PGOIDFloat8
						res.Columns[i].PGSize = 8
					}
				case C.SQLITE_BLOB:
					n := C.sqlite3_column_bytes(stmt, C.int(i))
					ptr := C.sqlite3_column_blob(stmt, C.int(i))
					var b []byte
					var txt string
					if n > 0 && ptr != nil {
						b = C.GoBytes(ptr, n)
						txt = "\\x" + hex.EncodeToString(b)
					} else {
						b = []byte{}
						txt = "\\x"
					}
					row[i] = SQLiteValue{
						Type: ct,
						Blob: b,
						Text: txt,
					}
					rowBytes += int64(len(b)) + int64(len(txt))
					if res.Columns[i].DeclType == "" {
						res.Columns[i].PGType = PGOIDBytea
						res.Columns[i].PGSize = -1
					}
				default: // SQLITE_TEXT
					n := C.sqlite3_column_bytes(stmt, C.int(i))
					ptr := C.sqlite3_column_text(stmt, C.int(i))
					var s string
					if n > 0 && ptr != nil {
						s = C.GoStringN((*C.char)(unsafe.Pointer(ptr)), n)
					}
					row[i] = SQLiteValue{
						Type: C.SQLITE_TEXT,
						Text: s,
					}
					rowBytes += int64(len(s))
				}
			}
			if budget != nil {
				if berr := budget.Reserve(rowBytes); berr != nil {
					return nil, nil, berr
				}
				batchBytes += rowBytes
			}
			totalRows++
			res.Rows = append(res.Rows, row)
			if streaming && len(res.Rows) >= batchSize {
				if ferr := flushBatch(); ferr != nil {
					return nil, nil, ferr
				}
			}
		} else if stepRC == C.SQLITE_DONE {
			res.RowsChanged = int64(C.sqlite3_changes(c.db))
			if streaming {
				if ferr := flushBatch(); ferr != nil {
					return nil, nil, ferr
				}
				res.RowsChanged = totalRows
			}
			break
		} else if stepRC == C.SQLITE_AUTH {
			return nil, nil, &SQLiteReadOnlyError{Message: C.GoString(C.sqlite3_errmsg(c.db))}
		} else {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return nil, nil, ctxErr
			}
			if qctx.lastErr != nil {
				return nil, nil, qctx.lastErr
			}
			return nil, nil, fmt.Errorf("%s", C.GoString(C.sqlite3_errmsg(c.db)))
		}
	}

	return res, qctx.lastPlan, nil
}

// DescribeColumns prepares a SQL statement without stepping it and returns its result column metadata.
func (c *SQLiteConn) DescribeColumns(sql string) ([]SQLiteColumn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.db == nil {
		return nil, fmt.Errorf("sqlite connection closed")
	}
	csql := C.CString(sql)
	defer C.free(unsafe.Pointer(csql))

	var stmt *C.sqlite3_stmt
	rc := C.sqlite3_prepare_v2(c.db, csql, -1, &stmt, nil)
	if rc != C.SQLITE_OK {
		return nil, fmt.Errorf("%s", C.GoString(C.sqlite3_errmsg(c.db)))
	}
	if stmt == nil {
		return nil, nil
	}
	defer C.sqlite3_finalize(stmt)

	colCount := int(C.sqlite3_column_count(stmt))
	cols := make([]SQLiteColumn, colCount)
	for i := 0; i < colCount; i++ {
		cname := C.GoString(C.sqlite3_column_name(stmt, C.int(i)))
		var decl string
		if cdecl := C.sqlite3_column_decltype(stmt, C.int(i)); cdecl != nil {
			decl = C.GoString(cdecl)
		}
		oid, sz := declTypeToPG(decl)
		cols[i] = SQLiteColumn{
			Name:     cname,
			DeclType: decl,
			PGType:   oid,
			PGSize:   sz,
		}
	}
	return cols, nil
}

// SQLiteReadOnlyError is returned when SQLite's authorizer or read-only mode rejects a write operation.
type SQLiteReadOnlyError struct {
	Message string
}

func (e *SQLiteReadOnlyError) Error() string {
	return fmt.Sprintf("AIR SQLite read-only engine rejected write operation: %s", e.Message)
}

// SQLiteVersion returns the linked libsqlite3 version string.
func SQLiteVersion() string {
	return C.GoString(C.sqlite3_libversion())
}

// SQLiteMemoryUsed returns the current C heap bytes allocated by SQLite.
func SQLiteMemoryUsed() int64 {
	return int64(C.sqlite3_memory_used())
}

// SQLiteMemoryHighwater returns the peak C heap bytes allocated by SQLite.
func SQLiteMemoryHighwater() int64 {
	return int64(C.sqlite3_memory_highwater(0))
}
