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
	"context"
	"encoding/hex"
	"fmt"
	"sync"
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

// SQLiteConn wraps a single native *C.sqlite3 connection with optional read-only authorizer enforcement.
type SQLiteConn struct {
	mu       sync.Mutex
	db       *C.sqlite3
	roFlag   *C.int
	path     string
	readOnly bool
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

	conn := &SQLiteConn{
		db:       db,
		roFlag:   roPtr,
		path:     path,
		readOnly: readOnly,
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

// Close closes the underlying SQLite connection and frees C resources.
func (c *SQLiteConn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.db != nil {
		C.sqlite3_set_authorizer(c.db, nil, nil)
		C.sqlite3_close_v2(c.db)
		c.db = nil
	}
	if c.roFlag != nil {
		C.free(unsafe.Pointer(c.roFlag))
		c.roFlag = nil
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
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.db == nil {
		return nil, fmt.Errorf("sqlite connection closed")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if ctx.Done() != nil {
		cancelPtr := (*C.int)(C.malloc(C.size_t(unsafe.Sizeof(C.int(0)))))
		*cancelPtr = 0
		stopWatch := make(chan struct{})
		doneCh := make(chan struct{})
		C.air_install_progress_handler(c.db, cancelPtr)
		go func() {
			defer close(doneCh)
			select {
			case <-ctx.Done():
				C.air_set_cancelled(cancelPtr)
				C.sqlite3_interrupt(c.db)
			case <-stopWatch:
			}
		}()
		defer func() {
			close(stopWatch)
			<-doneCh
			C.air_install_progress_handler(c.db, nil)
			C.free(unsafe.Pointer(cancelPtr))
		}()
	}

	qctx := &queryExecContext{
		ctx:  ctx,
		snap: snap,
	}
	setActiveQueryContext(unsafe.Pointer(c.db), qctx)
	defer clearActiveQueryContext(unsafe.Pointer(c.db))

	csql := C.CString(sql)
	defer C.free(unsafe.Pointer(csql))

	var stmt *C.sqlite3_stmt
	rc := C.sqlite3_prepare_v2(c.db, csql, -1, &stmt, nil)
	if rc != C.SQLITE_OK {
		msg := C.GoString(C.sqlite3_errmsg(c.db))
		if rc == C.SQLITE_AUTH {
			return nil, &SQLiteReadOnlyError{Message: msg}
		}
		return nil, fmt.Errorf("%s", msg)
	}
	if stmt == nil {
		// Empty statement or comment-only query.
		return &SQLiteResult{}, nil
	}
	defer C.sqlite3_finalize(stmt)

	if C.sqlite3_stmt_readonly(stmt) == 0 && c.readOnly {
		return nil, &SQLiteReadOnlyError{Message: "statement is not read-only"}
	}

	for i, arg := range args {
		idx := C.int(i + 1)
		if arg == "\x00NULL\x00" {
			C.sqlite3_bind_null(stmt, idx)
			continue
		}
		carg := C.CString(arg)
		brc := C.air_bind_text(stmt, idx, carg, C.int(len(arg)))
		C.free(unsafe.Pointer(carg))
		if brc != C.SQLITE_OK {
			return nil, fmt.Errorf("bind param %d: %s", i+1, C.GoString(C.sqlite3_errmsg(c.db)))
		}
	}

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

	res := &SQLiteResult{
		Columns:  cols,
		IsSelect: colCount > 0,
	}

	for {
		stepRC := C.sqlite3_step(stmt)
		if stepRC == C.SQLITE_ROW {
			row := make([]SQLiteValue, colCount)
			for i := 0; i < colCount; i++ {
				ct := int(C.sqlite3_column_type(stmt, C.int(i)))
				switch ct {
				case C.SQLITE_NULL:
					row[i] = SQLiteValue{Type: ct, IsNil: true}
				case C.SQLITE_INTEGER:
					iv := int64(C.sqlite3_column_int64(stmt, C.int(i)))
					row[i] = SQLiteValue{
						Type:  ct,
						Int64: iv,
						Text:  fmt.Sprintf("%d", iv),
					}
					if res.Columns[i].DeclType == "" {
						res.Columns[i].PGType = PGOIDInt8
						res.Columns[i].PGSize = 8
					}
				case C.SQLITE_FLOAT:
					fv := float64(C.sqlite3_column_double(stmt, C.int(i)))
					row[i] = SQLiteValue{
						Type:  ct,
						Float: fv,
						Text:  fmt.Sprintf("%g", fv),
					}
					if res.Columns[i].DeclType == "" {
						res.Columns[i].PGType = PGOIDFloat8
						res.Columns[i].PGSize = 8
					}
				case C.SQLITE_BLOB:
					n := C.sqlite3_column_bytes(stmt, C.int(i))
					ptr := C.sqlite3_column_blob(stmt, C.int(i))
					b := []byte{}
					if n > 0 && ptr != nil {
						b = C.GoBytes(ptr, n)
					}
					row[i] = SQLiteValue{
						Type: ct,
						Blob: b,
						Text: "\\x" + hex.EncodeToString(b),
					}
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
				}
			}
			res.Rows = append(res.Rows, row)
		} else if stepRC == C.SQLITE_DONE {
			res.RowsChanged = int64(C.sqlite3_changes(c.db))
			break
		} else if stepRC == C.SQLITE_AUTH {
			return nil, &SQLiteReadOnlyError{Message: C.GoString(C.sqlite3_errmsg(c.db))}
		} else {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return nil, ctxErr
			}
			if qctx.lastErr != nil {
				return nil, qctx.lastErr
			}
			return nil, fmt.Errorf("%s", C.GoString(C.sqlite3_errmsg(c.db)))
		}
	}

	return res, nil
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
