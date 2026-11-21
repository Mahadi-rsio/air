package readadapter

/*
#cgo LDFLAGS: -lsqlite3
#include <sqlite3.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
	sqlite3_vtab base;
	unsigned long long go_id;
} air_pg_vtab;

typedef struct {
	sqlite3_vtab_cursor base;
	unsigned long long go_id;
} air_pg_cursor;

extern int goAirPGConnect(sqlite3 *db, int argc, char **argv, unsigned long long *out_id, char **out_ddl);
extern int goAirPGOpen(unsigned long long vtab_id, unsigned long long *out_cursor_id);
extern int goAirPGClose(unsigned long long cursor_id);
extern int goAirPGFilter(unsigned long long cursor_id);
extern int goAirPGNext(unsigned long long cursor_id);
extern int goAirPGEof(unsigned long long cursor_id);
extern int goAirPGColumn(unsigned long long cursor_id, sqlite3_context *ctx, int col_idx);
extern sqlite3_int64 goAirPGRowid(unsigned long long cursor_id);

static int air_pg_connect(
	sqlite3 *db,
	void *pAux,
	int argc,
	const char *const *argv,
	sqlite3_vtab **ppVtab,
	char **pzErr
) {
	(void)pAux;
	unsigned long long go_id = 0;
	char *ddl = NULL;
	int rc = goAirPGConnect(db, argc, (char **)argv, &go_id, &ddl);
	if (rc != SQLITE_OK) {
		if (ddl != NULL) {
			*pzErr = sqlite3_mprintf("%s", ddl);
			free(ddl);
		}
		return rc;
	}
	rc = sqlite3_declare_vtab(db, ddl);
	free(ddl);
	if (rc != SQLITE_OK) {
		return rc;
	}
	air_pg_vtab *v = (air_pg_vtab *)sqlite3_malloc(sizeof(air_pg_vtab));
	if (v == NULL) {
		return SQLITE_NOMEM;
	}
	memset(v, 0, sizeof(air_pg_vtab));
	v->go_id = go_id;
	*ppVtab = &v->base;
	return SQLITE_OK;
}

static int air_pg_best_index(sqlite3_vtab *pVtab, sqlite3_index_info *pIdxInfo) {
	(void)pVtab;
	pIdxInfo->estimatedCost = 1000.0;
	pIdxInfo->estimatedRows = 1000;
	return SQLITE_OK;
}

static int air_pg_disconnect(sqlite3_vtab *pVtab) {
	sqlite3_free(pVtab);
	return SQLITE_OK;
}

static int air_pg_open(sqlite3_vtab *pVtab, sqlite3_vtab_cursor **ppCursor) {
	air_pg_vtab *v = (air_pg_vtab *)pVtab;
	unsigned long long cid = 0;
	int rc = goAirPGOpen(v->go_id, &cid);
	if (rc != SQLITE_OK) {
		return rc;
	}
	air_pg_cursor *c = (air_pg_cursor *)sqlite3_malloc(sizeof(air_pg_cursor));
	if (c == NULL) {
		goAirPGClose(cid);
		return SQLITE_NOMEM;
	}
	memset(c, 0, sizeof(air_pg_cursor));
	c->go_id = cid;
	*ppCursor = &c->base;
	return SQLITE_OK;
}

static int air_pg_close(sqlite3_vtab_cursor *pCur) {
	air_pg_cursor *c = (air_pg_cursor *)pCur;
	goAirPGClose(c->go_id);
	sqlite3_free(c);
	return SQLITE_OK;
}

static int air_pg_filter(
	sqlite3_vtab_cursor *pCur,
	int idxNum,
	const char *idxStr,
	int argc,
	sqlite3_value **argv
) {
	(void)idxNum;
	(void)idxStr;
	(void)argc;
	(void)argv;
	air_pg_cursor *c = (air_pg_cursor *)pCur;
	return goAirPGFilter(c->go_id);
}

static int air_pg_next(sqlite3_vtab_cursor *pCur) {
	air_pg_cursor *c = (air_pg_cursor *)pCur;
	return goAirPGNext(c->go_id);
}

static int air_pg_eof(sqlite3_vtab_cursor *pCur) {
	air_pg_cursor *c = (air_pg_cursor *)pCur;
	return goAirPGEof(c->go_id);
}

static int air_pg_column(sqlite3_vtab_cursor *pCur, sqlite3_context *ctx, int i) {
	air_pg_cursor *c = (air_pg_cursor *)pCur;
	return goAirPGColumn(c->go_id, ctx, i);
}

static int air_pg_rowid(sqlite3_vtab_cursor *pCur, sqlite3_int64 *pRowid) {
	air_pg_cursor *c = (air_pg_cursor *)pCur;
	*pRowid = goAirPGRowid(c->go_id);
	return SQLITE_OK;
}

static sqlite3_module air_pg_module = {
	0,                  // iVersion
	air_pg_connect,     // xCreate
	air_pg_connect,     // xConnect
	air_pg_best_index,  // xBestIndex
	air_pg_disconnect,  // xDisconnect
	air_pg_disconnect,  // xDestroy
	air_pg_open,        // xOpen
	air_pg_close,       // xClose
	air_pg_filter,      // xFilter
	air_pg_next,        // xNext
	air_pg_eof,         // xEof
	air_pg_column,      // xColumn
	air_pg_rowid,       // xRowid
	NULL,               // xUpdate (read-only!)
	NULL,               // xBegin
	NULL,               // xSync
	NULL,               // xCommit
	NULL,               // xRollback
	NULL,               // xFindFunction
	NULL,               // xRename
	NULL,               // xSavepoint
	NULL,               // xRelease
	NULL,               // xRollbackTo
	NULL,               // xShadowName
};

static int air_register_pg_vtab_module(sqlite3 *db) {
	return sqlite3_create_module_v2(db, "air_pg", &air_pg_module, NULL, NULL);
}

static void air_result_text_copy(sqlite3_context *ctx, const char *s, int n) {
	sqlite3_result_text(ctx, s, n, SQLITE_TRANSIENT);
}

static void air_result_blob_copy(sqlite3_context *ctx, const void *p, int n) {
	sqlite3_result_blob(ctx, p, n, SQLITE_TRANSIENT);
}
*/
import "C"

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"unsafe"
)

type pgVTabSpec struct {
	ID         uint64
	TenantID   string
	Generation string
	Table      DiscoveredTable
	AllCols    []ColumnType // includes dropped columns for heap offset alignment
	VisibleIdx []int        // maps SQLite column index -> index in AllCols
	Adapter    *Adapter
	CLOG       *CLOGResolver
}

type pgVTabCursor struct {
	spec        *pgVTabSpec
	snap        *ReadSnapshot
	rel         *RelationHandle
	nblocks     uint32
	nextBlock   uint32
	pageRows    [][]any
	pageRowIdx  int
	rowid       int64
	eof         bool
}

var (
	vtabNextID   atomic.Uint64
	cursorNextID atomic.Uint64

	vtabMu   sync.RWMutex
	vtabMap  = make(map[uint64]*pgVTabSpec)

	cursorMu  sync.RWMutex
	cursorMap = make(map[uint64]*pgVTabCursor)
)

// RegisterAirPGModule registers the read-only "air_pg" SQLite virtual table module
// on this SQLiteConn.
func (c *SQLiteConn) RegisterAirPGModule() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.db == nil {
		return fmt.Errorf("sqlite connection closed")
	}
	rc := C.air_register_pg_vtab_module(c.db)
	if rc != C.SQLITE_OK {
		return fmt.Errorf("sqlite3_create_module_v2(air_pg): rc=%d %s", int(rc), C.GoString(C.sqlite3_errmsg(c.db)))
	}
	return nil
}

// RegisterNativeTable registers a discovered PostgreSQL table as a read-only
// SQLite virtual table backed by AIR's Pageserver via adapter and clog.
// Returns the registered vtab spec ID (call UnregisterNativeTable on cleanup).
func (c *SQLiteConn) RegisterNativeTable(tenantID, generation string, tbl DiscoveredTable, adapter *Adapter, clog *CLOGResolver) (uint64, error) {
	allCols := make([]ColumnType, len(tbl.Columns))
	var visibleIdx []int
	for i, col := range tbl.Columns {
		allCols[i] = col.ColType
		if !col.IsDropped {
			visibleIdx = append(visibleIdx, i)
		}
	}
	if len(visibleIdx) == 0 {
		return 0, fmt.Errorf("readadapter: table %q has no visible columns", tbl.Name)
	}

	id := vtabNextID.Add(1)
	spec := &pgVTabSpec{
		ID:         id,
		TenantID:   tenantID,
		Generation: generation,
		Table:      tbl,
		AllCols:    allCols,
		VisibleIdx: visibleIdx,
		Adapter:    adapter,
		CLOG:       clog,
	}
	vtabMu.Lock()
	vtabMap[id] = spec
	vtabMu.Unlock()

	quotedName := quoteSQLiteIdent(tbl.Name)
	ddl := fmt.Sprintf("DROP TABLE IF EXISTS %s; CREATE VIRTUAL TABLE %s USING air_pg(%d);", quotedName, quotedName, id)
	if err := c.Exec(ddl); err != nil {
		UnregisterNativeTable(id)
		return 0, fmt.Errorf("register native virtual table %s: %w", tbl.Name, err)
	}
	return id, nil
}

func quoteSQLiteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// UnregisterNativeTable removes a previously registered virtual table spec.
func UnregisterNativeTable(id uint64) {
	vtabMu.Lock()
	delete(vtabMap, id)
	vtabMu.Unlock()
}

//export goAirPGConnect
func goAirPGConnect(_ *C.sqlite3, argc C.int, argv **C.char, outID *C.ulonglong, outDDL **C.char) C.int {
	if int(argc) < 4 {
		*outDDL = C.CString("air_pg requires vtab spec id argument")
		return C.SQLITE_ERROR
	}
	argSlice := unsafe.Slice(argv, int(argc))
	idStr := strings.TrimSpace(C.GoString(argSlice[3]))
	id, err := strconv.ParseUint(idStr, 10, 64)
	if err != nil {
		*outDDL = C.CString("invalid air_pg spec id")
		return C.SQLITE_ERROR
	}

	vtabMu.RLock()
	spec, ok := vtabMap[id]
	vtabMu.RUnlock()
	if !ok {
		*outDDL = C.CString("unknown air_pg spec id")
		return C.SQLITE_ERROR
	}

	var cols []string
	for _, idx := range spec.VisibleIdx {
		col := spec.Table.Columns[idx]
		cols = append(cols, fmt.Sprintf("%s %s", quoteSQLiteIdent(col.Name), col.SQLiteType))
	}
	ddl := fmt.Sprintf("CREATE TABLE x(%s)", strings.Join(cols, ", "))
	*outID = C.ulonglong(id)
	*outDDL = C.CString(ddl)
	return C.SQLITE_OK
}

//export goAirPGOpen
func goAirPGOpen(vtabID C.ulonglong, outCursorID *C.ulonglong) C.int {
	vtabMu.RLock()
	spec, ok := vtabMap[uint64(vtabID)]
	vtabMu.RUnlock()
	if !ok {
		return C.SQLITE_ERROR
	}
	cid := cursorNextID.Add(1)
	cur := &pgVTabCursor{
		spec: spec,
		eof:  true,
	}
	cursorMu.Lock()
	cursorMap[cid] = cur
	cursorMu.Unlock()
	*outCursorID = C.ulonglong(cid)
	return C.SQLITE_OK
}

func (cur *pgVTabCursor) resetResources() {
	if cur.rel != nil {
		cur.spec.Adapter.CloseRelation(cur.rel)
		cur.rel = nil
	}
	if cur.snap != nil {
		cur.spec.Adapter.ReleaseSnapshot(cur.snap)
		cur.snap = nil
	}
	cur.pageRows = nil
	cur.pageRowIdx = 0
}

//export goAirPGClose
func goAirPGClose(cursorID C.ulonglong) C.int {
	cursorMu.Lock()
	cur, ok := cursorMap[uint64(cursorID)]
	delete(cursorMap, uint64(cursorID))
	cursorMu.Unlock()
	if ok && cur != nil {
		cur.resetResources()
	}
	return C.SQLITE_OK
}

func (cur *pgVTabCursor) loadNextBatch(ctx context.Context) error {
	cur.pageRows = nil
	cur.pageRowIdx = 0

	const batchBlocks uint32 = 16
	for cur.nextBlock < cur.nblocks {
		n := batchBlocks
		if cur.nextBlock+n > cur.nblocks {
			n = cur.nblocks - cur.nextBlock
		}
		pages, err := cur.spec.Adapter.ReadPages(ctx, cur.rel, cur.snap, cur.nextBlock, n)
		if err != nil {
			return err
		}
		cur.nextBlock += n

		for _, pd := range pages {
			phdr, err := ParsePageHeader(pd.Data)
			if err != nil {
				continue
			}
			lps := ParseLinePointers(pd.Data, phdr)
			for _, lp := range lps {
				if lp.Flags != LPNormal || lp.Length < PGSizeOfHeapTupleHdr {
					continue
				}
				start := int(lp.Offset)
				end := start + int(lp.Length)
				if start < PGPageHeaderSize || end > len(pd.Data) {
					continue
				}
				tup := pd.Data[start:end]
				thdr, err := ParseHeapTupleHeader(tup)
				if err != nil {
					continue
				}
				vis, err := IsVisibleWithCLOG(ctx, thdr, cur.snap, cur.spec.CLOG)
				if err != nil || !vis {
					continue
				}
				vals, err := DecodeHeapColumns(tup, thdr, cur.spec.AllCols)
				if err != nil {
					continue
				}
				cur.pageRows = append(cur.pageRows, vals)
			}
		}
		if len(cur.pageRows) > 0 {
			return nil
		}
	}
	cur.eof = true
	return nil
}

//export goAirPGFilter
func goAirPGFilter(cursorID C.ulonglong) C.int {
	cursorMu.RLock()
	cur, ok := cursorMap[uint64(cursorID)]
	cursorMu.RUnlock()
	if !ok || cur == nil {
		return C.SQLITE_ERROR
	}

	cur.resetResources()
	cur.eof = false
	cur.rowid = 0
	cur.nextBlock = 0

	ctx := context.Background()
	snap, err := cur.spec.Adapter.GetSnapshot(ctx, cur.spec.TenantID, cur.spec.Generation)
	if err != nil {
		cur.eof = true
		return C.SQLITE_ERROR
	}
	cur.snap = snap

	rel, err := cur.spec.Adapter.OpenRelation(ctx, RelationSpec{
		TenantID:   cur.spec.TenantID,
		Generation: cur.spec.Generation,
		SpcOID:     cur.spec.Table.SpcOID,
		DbOID:      cur.spec.Table.DbOID,
		RelNumber:  cur.spec.Table.RelNumber,
		ForkNum:    0,
	})
	if err != nil {
		cur.resetResources()
		cur.eof = true
		return C.SQLITE_ERROR
	}
	cur.rel = rel

	meta, err := cur.spec.Adapter.GetRelationMetadata(ctx, rel, snap)
	if err != nil {
		cur.resetResources()
		cur.eof = true
		return C.SQLITE_ERROR
	}
	if !meta.Exists || meta.NBlocks == 0 {
		cur.eof = true
		return C.SQLITE_OK
	}
	cur.nblocks = meta.NBlocks

	if err := cur.loadNextBatch(ctx); err != nil {
		cur.eof = true
		return C.SQLITE_ERROR
	}
	if !cur.eof {
		cur.rowid = 1
	}
	return C.SQLITE_OK
}

//export goAirPGNext
func goAirPGNext(cursorID C.ulonglong) C.int {
	cursorMu.RLock()
	cur, ok := cursorMap[uint64(cursorID)]
	cursorMu.RUnlock()
	if !ok || cur == nil {
		return C.SQLITE_ERROR
	}
	if cur.eof {
		return C.SQLITE_OK
	}

	cur.pageRowIdx++
	if cur.pageRowIdx < len(cur.pageRows) {
		cur.rowid++
		return C.SQLITE_OK
	}

	if err := cur.loadNextBatch(context.Background()); err != nil {
		cur.eof = true
		return C.SQLITE_ERROR
	}
	if !cur.eof {
		cur.rowid++
	}
	return C.SQLITE_OK
}

//export goAirPGEof
func goAirPGEof(cursorID C.ulonglong) C.int {
	cursorMu.RLock()
	cur, ok := cursorMap[uint64(cursorID)]
	cursorMu.RUnlock()
	if !ok || cur == nil || cur.eof {
		return 1
	}
	return 0
}

//export goAirPGColumn
func goAirPGColumn(cursorID C.ulonglong, ctx *C.sqlite3_context, colIdx C.int) C.int {
	cursorMu.RLock()
	cur, ok := cursorMap[uint64(cursorID)]
	cursorMu.RUnlock()
	if !ok || cur == nil || cur.eof || cur.pageRowIdx >= len(cur.pageRows) {
		C.sqlite3_result_null(ctx)
		return C.SQLITE_OK
	}
	idx := int(colIdx)
	if idx < 0 || idx >= len(cur.spec.VisibleIdx) {
		C.sqlite3_result_null(ctx)
		return C.SQLITE_OK
	}
	val := cur.pageRows[cur.pageRowIdx][cur.spec.VisibleIdx[idx]]
	if val == nil {
		C.sqlite3_result_null(ctx)
		return C.SQLITE_OK
	}
	switch v := val.(type) {
	case bool:
		if v {
			C.sqlite3_result_int64(ctx, 1)
		} else {
			C.sqlite3_result_int64(ctx, 0)
		}
	case byte:
		s := string([]byte{v})
		cs := C.CString(s)
		C.air_result_text_copy(ctx, cs, C.int(len(s)))
		C.free(unsafe.Pointer(cs))
	case int16:
		C.sqlite3_result_int64(ctx, C.sqlite3_int64(v))
	case int32:
		C.sqlite3_result_int64(ctx, C.sqlite3_int64(v))
	case int64:
		C.sqlite3_result_int64(ctx, C.sqlite3_int64(v))
	case uint32:
		C.sqlite3_result_int64(ctx, C.sqlite3_int64(v))
	case float64:
		C.sqlite3_result_double(ctx, C.double(v))
	case string:
		if len(v) == 0 {
			cs := C.CString("")
			C.air_result_text_copy(ctx, cs, 0)
			C.free(unsafe.Pointer(cs))
		} else {
			cs := C.CString(v)
			C.air_result_text_copy(ctx, cs, C.int(len(v)))
			C.free(unsafe.Pointer(cs))
		}
	case []byte:
		if len(v) == 0 {
			C.air_result_blob_copy(ctx, nil, 0)
		} else {
			C.air_result_blob_copy(ctx, unsafe.Pointer(&v[0]), C.int(len(v)))
		}
	default:
		s := fmt.Sprint(v)
		cs := C.CString(s)
		C.air_result_text_copy(ctx, cs, C.int(len(s)))
		C.free(unsafe.Pointer(cs))
	}
	return C.SQLITE_OK
}

//export goAirPGRowid
func goAirPGRowid(cursorID C.ulonglong) C.sqlite3_int64 {
	cursorMu.RLock()
	cur, ok := cursorMap[uint64(cursorID)]
	cursorMu.RUnlock()
	if !ok || cur == nil {
		return 0
	}
	return C.sqlite3_int64(cur.rowid)
}
