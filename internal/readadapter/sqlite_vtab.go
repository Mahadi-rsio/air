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
extern void goAirPGBestIndexCost(unsigned long long vtab_id, char *idx_str, double *out_cost, sqlite3_int64 *out_rows);
extern int goAirPGOpen(unsigned long long vtab_id, unsigned long long *out_cursor_id);
extern int goAirPGClose(unsigned long long cursor_id);
extern int goAirPGFilter(unsigned long long cursor_id, char *idx_str, int argc, sqlite3_value **argv);
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
	air_pg_vtab *v = (air_pg_vtab *)pVtab;
	int arg_idx = 1;

	sqlite3_str *str = sqlite3_str_new(NULL);
	sqlite3_str_appendf(str, "%llx|", (unsigned long long)pIdxInfo->colUsed);

	for (int i = 0; i < pIdxInfo->nConstraint; i++) {
		if (!pIdxInfo->aConstraint[i].usable || pIdxInfo->aConstraint[i].iColumn < 0) {
			continue;
		}
		unsigned char op = pIdxInfo->aConstraint[i].op;
		int needs_arg = 0;
		switch (op) {
			case SQLITE_INDEX_CONSTRAINT_EQ:
			case SQLITE_INDEX_CONSTRAINT_GT:
			case SQLITE_INDEX_CONSTRAINT_LE:
			case SQLITE_INDEX_CONSTRAINT_LT:
			case SQLITE_INDEX_CONSTRAINT_GE:
				needs_arg = 1;
				break;
			case SQLITE_INDEX_CONSTRAINT_ISNULL:
			case SQLITE_INDEX_CONSTRAINT_ISNOTNULL:
				needs_arg = 0;
				break;
			default:
				continue;
		}
		if (needs_arg) {
			pIdxInfo->aConstraintUsage[i].argvIndex = arg_idx++;
		}
		pIdxInfo->aConstraintUsage[i].omit = 0;
		sqlite3_str_appendf(str, "%d:%d,", pIdxInfo->aConstraint[i].iColumn, (int)op);
	}

	char *encoded = sqlite3_str_finish(str);
	if (encoded == NULL) {
		return SQLITE_NOMEM;
	}
	pIdxInfo->idxStr = encoded;
	pIdxInfo->needToFreeIdxStr = 1;

	double cost = 10000.0;
	sqlite3_int64 rows = 10000;
	goAirPGBestIndexCost(v != NULL ? v->go_id : 0, encoded, &cost, &rows);
	pIdxInfo->estimatedCost = cost;
	pIdxInfo->estimatedRows = rows;
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
	air_pg_cursor *c = (air_pg_cursor *)pCur;
	return goAirPGFilter(c->go_id, (char *)idxStr, argc, argv);
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

const (
	sqliteConstraintEQ        = int(C.SQLITE_INDEX_CONSTRAINT_EQ)
	sqliteConstraintGT        = int(C.SQLITE_INDEX_CONSTRAINT_GT)
	sqliteConstraintLE        = int(C.SQLITE_INDEX_CONSTRAINT_LE)
	sqliteConstraintLT        = int(C.SQLITE_INDEX_CONSTRAINT_LT)
	sqliteConstraintGE        = int(C.SQLITE_INDEX_CONSTRAINT_GE)
	sqliteConstraintISNULL    = int(C.SQLITE_INDEX_CONSTRAINT_ISNULL)
	sqliteConstraintISNOTNULL = int(C.SQLITE_INDEX_CONSTRAINT_ISNOTNULL)

	sqliteTypeInteger = int(C.SQLITE_INTEGER)
	sqliteTypeFloat   = int(C.SQLITE_FLOAT)
	sqliteTypeText    = int(C.SQLITE_TEXT)
	sqliteTypeBlob    = int(C.SQLITE_BLOB)
	sqliteTypeNull    = int(C.SQLITE_NULL)
)

type pgVTabSpec struct {
	ID         uint64
	db         unsafe.Pointer // *C.sqlite3
	TenantID   string
	Generation string
	Table      DiscoveredTable
	AllCols    []ColumnType // includes dropped columns for heap offset alignment
	VisibleIdx []int        // maps SQLite column index -> index in AllCols
	IndexPlans []btreeIndexPlan
	Adapter    *Adapter
	CLOG       *CLOGResolver
}

type vtabFilter struct {
	allColIdx int
	op        int
	valType   int // C.SQLITE_INTEGER, C.SQLITE_FLOAT, C.SQLITE_TEXT, C.SQLITE_BLOB, C.SQLITE_NULL
	intVal    int64
	floatVal  float64
	strVal    string
}

type pgVTabCursor struct {
	spec          *pgVTabSpec
	ctx           context.Context
	snap          *ReadSnapshot
	ownsSnap      bool
	rel           *RelationHandle
	toastRel      *RelationHandle
	toastFn       TOASTResolverFunc
	nblocks       uint32
	nextBlock     uint32
	indexScanDone bool
	neededCols    []bool
	filters       []vtabFilter
	rowStride     int
	rows          []HeapVal
	numRows       int
	rowBuf        []HeapVal
	lpBuf         []LinePointer
	pageRowIdx    int
	rowid         int64
	eof           bool
}

type queryExecContext struct {
	ctx     context.Context
	snap    *ReadSnapshot
	lastErr error
}

var (
	vtabNextID   atomic.Uint64
	cursorNextID atomic.Uint64

	vtabMu   sync.RWMutex
	vtabMap  = make(map[uint64]*pgVTabSpec)

	cursorMu  sync.RWMutex
	cursorMap = make(map[uint64]*pgVTabCursor)

	activeQueryMu  sync.RWMutex
	activeQueryMap = make(map[unsafe.Pointer]*queryExecContext)

	vtabPagesRead     atomic.Uint64
	vtabTuplesDecoded atomic.Uint64
	vtabColsDecoded   atomic.Uint64
	vtabRowsMatched   atomic.Uint64
)

// VTabStats returns cumulative virtual table scan, decode, and match counters.
func VTabStats() (pagesRead, tuplesDecoded, colsDecoded, rowsMatched uint64) {
	return vtabPagesRead.Load(), vtabTuplesDecoded.Load(), vtabColsDecoded.Load(), vtabRowsMatched.Load()
}

func setActiveQueryContext(db unsafe.Pointer, qctx *queryExecContext) {
	if db == nil {
		return
	}
	activeQueryMu.Lock()
	activeQueryMap[db] = qctx
	activeQueryMu.Unlock()
}

func clearActiveQueryContext(db unsafe.Pointer) {
	if db == nil {
		return
	}
	activeQueryMu.Lock()
	delete(activeQueryMap, db)
	activeQueryMu.Unlock()
}

func getActiveQueryContext(db unsafe.Pointer) *queryExecContext {
	if db == nil {
		return nil
	}
	activeQueryMu.RLock()
	qctx := activeQueryMap[db]
	activeQueryMu.RUnlock()
	return qctx
}

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
		db:         unsafe.Pointer(c.db),
		TenantID:   tenantID,
		Generation: generation,
		Table:      tbl,
		AllCols:    allCols,
		VisibleIdx: visibleIdx,
		IndexPlans: buildTableIndexPlans(tbl, allCols, visibleIdx),
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

//export goAirPGBestIndexCost
func goAirPGBestIndexCost(vtabID C.ulonglong, idxStr *C.char, outCost *C.double, outRows *C.sqlite3_int64) {
	*outCost = 10000.0
	*outRows = 10000
	if idxStr == nil {
		return
	}
	encoded := C.GoString(idxStr)
	barIdx := strings.IndexByte(encoded, '|')
	if barIdx < 0 {
		return
	}
	condPart := encoded[barIdx+1:]
	if condPart == "" {
		return
	}

	vtabMu.RLock()
	spec := vtabMap[uint64(vtabID)]
	vtabMu.RUnlock()

	hasEQ := false
	hasRange := false
	hasIndexedEQ := false
	hasUniqueIndexedEQ := false
	hasIndexedRange := false

	for len(condPart) > 0 {
		commaIdx := strings.IndexByte(condPart, ',')
		var token string
		if commaIdx >= 0 {
			token = condPart[:commaIdx]
			condPart = condPart[commaIdx+1:]
		} else {
			token = condPart
			condPart = ""
		}
		if token == "" {
			continue
		}
		colonIdx := strings.IndexByte(token, ':')
		if colonIdx < 0 {
			continue
		}
		vIdx, err1 := strconv.Atoi(token[:colonIdx])
		op, err2 := strconv.Atoi(token[colonIdx+1:])
		if err1 != nil || err2 != nil {
			continue
		}
		if op == sqliteConstraintEQ || op == sqliteConstraintISNULL {
			hasEQ = true
		} else {
			hasRange = true
		}
		if spec != nil && vIdx >= 0 && vIdx < len(spec.VisibleIdx) {
			allIdx := spec.VisibleIdx[vIdx]
			for _, plan := range spec.IndexPlans {
				if len(plan.KeyAllCols) > 0 && plan.KeyAllCols[0] == allIdx {
					if op == sqliteConstraintEQ || op == sqliteConstraintISNULL {
						hasIndexedEQ = true
						if plan.Index.IsUnique && plan.Index.IndNKeyAtts == 1 {
							hasUniqueIndexedEQ = true
						}
					} else if op == sqliteConstraintGT || op == sqliteConstraintGE || op == sqliteConstraintLT || op == sqliteConstraintLE {
						hasIndexedRange = true
					}
				}
			}
		}
	}

	switch {
	case hasUniqueIndexedEQ:
		*outCost = 2.0
		*outRows = 1
	case hasIndexedEQ:
		*outCost = 10.0
		*outRows = 5
	case hasIndexedRange:
		*outCost = 40.0
		*outRows = 50
	case hasEQ:
		*outCost = 100.0
		*outRows = 10
	case hasRange:
		*outCost = 500.0
		*outRows = 100
	default:
		*outCost = 10000.0
		*outRows = 10000
	}
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
	cur.toastFn = cur.resolveTOASTPointer
	cursorMu.Lock()
	cursorMap[cid] = cur
	cursorMu.Unlock()
	*outCursorID = C.ulonglong(cid)
	return C.SQLITE_OK
}

func (cur *pgVTabCursor) resetResources() {
	if cur.toastRel != nil {
		_ = cur.spec.Adapter.CloseRelation(cur.toastRel)
		cur.toastRel = nil
	}
	if cur.rel != nil {
		_ = cur.spec.Adapter.CloseRelation(cur.rel)
		cur.rel = nil
	}
	if cur.snap != nil {
		if cur.ownsSnap {
			_ = cur.spec.Adapter.ReleaseSnapshot(cur.snap)
		}
		cur.snap = nil
		cur.ownsSnap = false
	}
	cur.ctx = nil
	cur.indexScanDone = false
	cur.neededCols = cur.neededCols[:0]
	cur.filters = cur.filters[:0]
	cur.rows = cur.rows[:0]
	cur.numRows = 0
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

func heapValToFloat64(v HeapVal) (float64, bool) {
	switch v.Kind {
	case ValInt64, ValBool, ValByte:
		return float64(v.I64), true
	case ValFloat64:
		return v.F64, true
	default:
		return 0, false
	}
}

func heapValToInt64(v HeapVal) (int64, bool) {
	switch v.Kind {
	case ValInt64, ValBool, ValByte:
		return v.I64, true
	default:
		return 0, false
	}
}

func evalFilterVal(f vtabFilter, val HeapVal) bool {
	switch f.op {
	case int(C.SQLITE_INDEX_CONSTRAINT_ISNULL):
		return val.Kind == ValNull
	case int(C.SQLITE_INDEX_CONSTRAINT_ISNOTNULL):
		return val.Kind != ValNull
	}
	if val.Kind == ValNull || f.valType == int(C.SQLITE_NULL) {
		return false
	}

	var cmp int
	switch f.valType {
	case int(C.SQLITE_INTEGER):
		if iv, ok := heapValToInt64(val); ok {
			if iv < f.intVal {
				cmp = -1
			} else if iv > f.intVal {
				cmp = 1
			}
		} else if fv, ok := heapValToFloat64(val); ok {
			target := float64(f.intVal)
			if fv < target {
				cmp = -1
			} else if fv > target {
				cmp = 1
			}
		} else {
			return true
		}
	case int(C.SQLITE_FLOAT):
		if fv, ok := heapValToFloat64(val); ok {
			if fv < f.floatVal {
				cmp = -1
			} else if fv > f.floatVal {
				cmp = 1
			}
		} else {
			return true
		}
	case int(C.SQLITE_TEXT):
		if val.Kind == ValString {
			if val.Str < f.strVal {
				cmp = -1
			} else if val.Str > f.strVal {
				cmp = 1
			}
		} else if val.Kind == ValByte {
			b := byte(val.I64)
			if len(f.strVal) == 0 || b > f.strVal[0] {
				cmp = 1
			} else if b < f.strVal[0] {
				cmp = -1
			} else if len(f.strVal) > 1 {
				cmp = -1
			}
		} else {
			return true
		}
	default:
		return true
	}

	switch f.op {
	case int(C.SQLITE_INDEX_CONSTRAINT_EQ):
		return cmp == 0
	case int(C.SQLITE_INDEX_CONSTRAINT_LT):
		return cmp < 0
	case int(C.SQLITE_INDEX_CONSTRAINT_LE):
		return cmp <= 0
	case int(C.SQLITE_INDEX_CONSTRAINT_GT):
		return cmp > 0
	case int(C.SQLITE_INDEX_CONSTRAINT_GE):
		return cmp >= 0
	default:
		return true
	}
}

func (cur *pgVTabCursor) matchesFilters(vals []HeapVal) bool {
	for _, f := range cur.filters {
		if f.allColIdx < 0 || f.allColIdx >= len(vals) {
			continue
		}
		if !evalFilterVal(f, vals[f.allColIdx]) {
			return false
		}
	}
	return true
}

func (cur *pgVTabCursor) decodeHeapTupleColumns(_ context.Context, tup []byte, thdr HeapTupleHeader, dst []HeapVal) error {
	return DecodeHeapColumnsWithTOASTInto(tup, thdr, cur.spec.AllCols, cur.neededCols, dst, false, cur.toastFn)
}

func (cur *pgVTabCursor) loadNextBatch(ctx context.Context) error {
	cur.rows = cur.rows[:0]
	cur.numRows = 0
	cur.pageRowIdx = 0
	if cur.indexScanDone {
		cur.eof = true
		return nil
	}
	ncols := len(cur.spec.AllCols)
	cur.rowStride = ncols
	if cap(cur.rowBuf) < ncols {
		cur.rowBuf = make([]HeapVal, ncols)
	} else {
		cur.rowBuf = cur.rowBuf[:ncols]
	}

	var decodedColsPerTuple uint64
	if cur.neededCols == nil {
		decodedColsPerTuple = uint64(ncols)
	} else {
		for _, need := range cur.neededCols {
			if need {
				decodedColsPerTuple++
			}
		}
	}

	var filterCols []bool
	if len(cur.filters) > 0 {
		filterCols = make([]bool, ncols)
		for _, f := range cur.filters {
			if f.allColIdx >= 0 && f.allColIdx < ncols {
				filterCols[f.allColIdx] = true
			}
		}
	}

	const batchBlocks uint32 = 16
	for cur.nextBlock < cur.nblocks {
		if err := ctx.Err(); err != nil {
			return err
		}
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
			vtabPagesRead.Add(1)
			if IsZeroPage(pd.Data) {
				continue
			}
			phdr, err := ParsePageHeader(pd.Data)
			if err != nil {
				return err
			}
			cur.lpBuf = ParseLinePointersInto(pd.Data, phdr, cur.lpBuf)
			for _, lp := range cur.lpBuf {
				if lp.Flags != LPNormal {
					continue
				}
				if err := ValidateLinePointer(lp, phdr); err != nil {
					return err
				}
				start := int(lp.Offset)
				end := start + int(lp.Length)
				tup := pd.Data[start:end]
				thdr, err := ParseHeapTupleHeader(tup)
				if err != nil {
					return err
				}
				vis, err := IsVisibleWithCLOG(ctx, thdr, cur.snap, cur.spec.CLOG)
				if err != nil {
					return err
				}
				if !vis {
					continue
				}
				vtabTuplesDecoded.Add(1)
				vtabColsDecoded.Add(decodedColsPerTuple)
				if len(filterCols) > 0 {
					if err := DecodeHeapColumnsWithTOASTInto(tup, thdr, cur.spec.AllCols, filterCols, cur.rowBuf, false, cur.toastFn); err != nil {
						return err
					}
					if !cur.matchesFilters(cur.rowBuf) {
						continue
					}
				}
				if err := cur.decodeHeapTupleColumns(ctx, tup, thdr, cur.rowBuf); err != nil {
					return err
				}
				if len(filterCols) == 0 && !cur.matchesFilters(cur.rowBuf) {
					continue
				}
				for j := range cur.rowBuf {
					cur.rowBuf[j].Materialize()
				}
				vtabRowsMatched.Add(1)
				cur.rows = append(cur.rows, cur.rowBuf...)
				cur.numRows++
			}
		}
		if cur.numRows > 0 {
			return nil
		}
	}
	cur.eof = true
	return nil
}

func (cur *pgVTabCursor) parseFilterPlan(idxStr string, argc int, argv **C.sqlite3_value) {
	needed := make([]bool, len(cur.spec.AllCols))
	hasColMask := false

	if idxStr != "" {
		parts := strings.SplitN(idxStr, "|", 2)
		if len(parts) >= 1 && parts[0] != "" {
			if colUsed, err := strconv.ParseUint(parts[0], 16, 64); err == nil {
				hasColMask = true
				if (colUsed >> 63) != 0 {
					for _, allIdx := range cur.spec.VisibleIdx {
						needed[allIdx] = true
					}
				} else {
					for visIdx, allIdx := range cur.spec.VisibleIdx {
						if visIdx < 63 && (colUsed&(1<<uint(visIdx))) != 0 {
							needed[allIdx] = true
						}
					}
				}
			}
		}

		if len(parts) == 2 && parts[1] != "" {
			var argVals []*C.sqlite3_value
			if argc > 0 && argv != nil {
				argVals = unsafe.Slice(argv, argc)
			}
			argIdx := 0
			for _, item := range strings.Split(parts[1], ",") {
				if item == "" {
					continue
				}
				kv := strings.SplitN(item, ":", 2)
				if len(kv) != 2 {
					continue
				}
				visCol, err1 := strconv.Atoi(kv[0])
				op, err2 := strconv.Atoi(kv[1])
				if err1 != nil || err2 != nil || visCol < 0 || visCol >= len(cur.spec.VisibleIdx) {
					continue
				}
				allCol := cur.spec.VisibleIdx[visCol]
				needed[allCol] = true

				f := vtabFilter{
					allColIdx: allCol,
					op:        op,
				}
				if op != int(C.SQLITE_INDEX_CONSTRAINT_ISNULL) && op != int(C.SQLITE_INDEX_CONSTRAINT_ISNOTNULL) {
					if argIdx >= len(argVals) {
						continue
					}
					v := argVals[argIdx]
					argIdx++
					f.valType = int(C.sqlite3_value_type(v))
					switch f.valType {
					case int(C.SQLITE_INTEGER):
						f.intVal = int64(C.sqlite3_value_int64(v))
					case int(C.SQLITE_FLOAT):
						f.floatVal = float64(C.sqlite3_value_double(v))
					case int(C.SQLITE_TEXT):
						n := C.sqlite3_value_bytes(v)
						p := C.sqlite3_value_text(v)
						if n > 0 && p != nil {
							f.strVal = C.GoStringN((*C.char)(unsafe.Pointer(p)), n)
						}
					}
				}
				cur.filters = append(cur.filters, f)
			}
		}
	}

	if hasColMask {
		cur.neededCols = needed
	} else {
		cur.neededCols = nil
	}
}

//export goAirPGFilter
func goAirPGFilter(cursorID C.ulonglong, idxStr *C.char, argc C.int, argv **C.sqlite3_value) C.int {
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
	if idxStr != nil {
		cur.parseFilterPlan(C.GoString(idxStr), int(argc), argv)
	}

	ctx := context.Background()
	var sharedSnap *ReadSnapshot
	if qctx := getActiveQueryContext(cur.spec.db); qctx != nil {
		if qctx.ctx != nil {
			ctx = qctx.ctx
		}
		if qctx.snap != nil {
			if qctx.snap.TenantID != cur.spec.TenantID || qctx.snap.Generation != cur.spec.Generation || qctx.snap.released.Load() {
				cur.eof = true
				return C.SQLITE_ERROR
			}
			sharedSnap = qctx.snap
		}
	}
	if err := ctx.Err(); err != nil {
		cur.eof = true
		return C.SQLITE_INTERRUPT
	}
	cur.ctx = ctx

	if sharedSnap != nil {
		cur.snap = sharedSnap
		cur.ownsSnap = false
	} else {
		snap, err := cur.spec.Adapter.GetSnapshot(ctx, cur.spec.TenantID, cur.spec.Generation)
		if err != nil {
			cur.eof = true
			return C.SQLITE_ERROR
		}
		cur.snap = snap
		cur.ownsSnap = true
	}

	rel, err := cur.spec.Adapter.OpenRelation(ctx, RelationSpec{
		TenantID:   cur.spec.TenantID,
		Generation: cur.spec.Generation,
		SpcOID:     cur.spec.Table.SpcOID,
		DbOID:      cur.spec.Table.DbOID,
		RelNumber:  cur.spec.Table.RelNumber,
		ForkNum:    0,
	})
	if err != nil {
		if qctx := getActiveQueryContext(cur.spec.db); qctx != nil && qctx.lastErr == nil {
			qctx.lastErr = err
		}
		cur.resetResources()
		cur.eof = true
		return C.SQLITE_ERROR
	}
	cur.rel = rel

	meta, err := cur.spec.Adapter.GetRelationMetadata(ctx, rel, cur.snap)
	if err != nil {
		if qctx := getActiveQueryContext(cur.spec.db); qctx != nil && qctx.lastErr == nil {
			qctx.lastErr = err
		}
		cur.resetResources()
		cur.eof = true
		return C.SQLITE_ERROR
	}
	if !meta.Exists || meta.NBlocks == 0 {
		cur.eof = true
		return C.SQLITE_OK
	}
	cur.nblocks = meta.NBlocks

	// Try B-tree index scan pushdown if an eligible index exists for the pushed-down filters.
	if planIdx, _, _ := selectBestBTreeIndex(cur.spec.IndexPlans, cur.filters); planIdx >= 0 {
		tids, idxErr := scanBTreeIndexForTIDs(ctx, cur.spec.Adapter, cur.snap, cur.spec.IndexPlans[planIdx], cur.filters)
		if idxErr == nil {
			cur.indexScanDone = true
			if err := cur.fetchVisibleTuplesByTIDs(ctx, tids); err != nil {
				if qctx := getActiveQueryContext(cur.spec.db); qctx != nil && qctx.lastErr == nil {
					qctx.lastErr = err
				}
				cur.resetResources()
				cur.eof = true
				return C.SQLITE_ERROR
			}
			if !cur.eof {
				cur.rowid = 1
			}
			return C.SQLITE_OK
		}
		// If the index relation was missing/corrupt/in-transition (e.g. REINDEX), fall back safely to sequential heap scan!
	}

	if err := cur.loadNextBatch(ctx); err != nil {
		if qctx := getActiveQueryContext(cur.spec.db); qctx != nil && qctx.lastErr == nil {
			qctx.lastErr = err
		}
		cur.resetResources()
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
	if cur.pageRowIdx < cur.numRows {
		cur.rowid++
		return C.SQLITE_OK
	}

	ctx := cur.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	if err := cur.loadNextBatch(ctx); err != nil {
		cur.resetResources()
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

var emptyCByte byte

//export goAirPGColumn
func goAirPGColumn(cursorID C.ulonglong, ctx *C.sqlite3_context, colIdx C.int) C.int {
	cursorMu.RLock()
	cur, ok := cursorMap[uint64(cursorID)]
	cursorMu.RUnlock()
	if !ok || cur == nil || cur.eof || cur.pageRowIdx >= cur.numRows {
		C.sqlite3_result_null(ctx)
		return C.SQLITE_OK
	}
	idx := int(colIdx)
	if idx < 0 || idx >= len(cur.spec.VisibleIdx) {
		C.sqlite3_result_null(ctx)
		return C.SQLITE_OK
	}
	val := &cur.rows[cur.pageRowIdx*cur.rowStride+cur.spec.VisibleIdx[idx]]
	switch val.Kind {
	case ValNull:
		C.sqlite3_result_null(ctx)
	case ValBool, ValInt64:
		C.sqlite3_result_int64(ctx, C.sqlite3_int64(val.I64))
	case ValFloat64:
		C.sqlite3_result_double(ctx, C.double(val.F64))
	case ValByte:
		b := byte(val.I64)
		C.air_result_text_copy(ctx, (*C.char)(unsafe.Pointer(&b)), 1)
	case ValString:
		if len(val.Str) == 0 {
			C.air_result_text_copy(ctx, (*C.char)(unsafe.Pointer(&emptyCByte)), 0)
		} else {
			C.air_result_text_copy(ctx, (*C.char)(unsafe.Pointer(unsafe.StringData(val.Str))), C.int(len(val.Str)))
		}
	case ValBytes:
		if len(val.Raw) == 0 {
			C.air_result_blob_copy(ctx, unsafe.Pointer(&emptyCByte), 0)
		} else {
			C.air_result_blob_copy(ctx, unsafe.Pointer(&val.Raw[0]), C.int(len(val.Raw)))
		}
	default:
		C.sqlite3_result_null(ctx)
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
