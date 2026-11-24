package readadapter

/*
#include <sqlite3.h>
#include <stdlib.h>

extern void goAirPGIlike(sqlite3_context *ctx, int argc, sqlite3_value **argv);
extern void goAirPGRegex(sqlite3_context *ctx, int argc, sqlite3_value **argv);
extern void goAirPGNumOp(sqlite3_context *ctx, int argc, sqlite3_value **argv, int op);
extern void goAirPGNumAggStep(sqlite3_context *ctx, void *aggCtx, int argc, sqlite3_value **argv);
extern void goAirPGNumAggFinal(sqlite3_context *ctx, void *aggCtx, int isAvg);

static void air_pg_ilike_fn(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
	goAirPGIlike(ctx, argc, argv);
}

static void air_pg_regex_fn(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
	goAirPGRegex(ctx, argc, argv);
}

static void air_pg_num_add_fn(sqlite3_context *ctx, int argc, sqlite3_value **argv) { goAirPGNumOp(ctx, argc, argv, 0); }
static void air_pg_num_sub_fn(sqlite3_context *ctx, int argc, sqlite3_value **argv) { goAirPGNumOp(ctx, argc, argv, 1); }
static void air_pg_num_mul_fn(sqlite3_context *ctx, int argc, sqlite3_value **argv) { goAirPGNumOp(ctx, argc, argv, 2); }
static void air_pg_num_div_fn(sqlite3_context *ctx, int argc, sqlite3_value **argv) { goAirPGNumOp(ctx, argc, argv, 3); }

static void air_pg_num_agg_step(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
	void *p = sqlite3_aggregate_context(ctx, 8);
	goAirPGNumAggStep(ctx, p, argc, argv);
}

static void air_pg_num_sum_final(sqlite3_context *ctx) {
	void *p = sqlite3_aggregate_context(ctx, 0);
	goAirPGNumAggFinal(ctx, p, 0);
}

static void air_pg_num_avg_final(sqlite3_context *ctx) {
	void *p = sqlite3_aggregate_context(ctx, 0);
	goAirPGNumAggFinal(ctx, p, 1);
}

static int air_register_pg_funcs(sqlite3 *db) {
	int rc = sqlite3_create_function_v2(db, "air_pg_ilike", 2, SQLITE_UTF8 | SQLITE_DETERMINISTIC, NULL, air_pg_ilike_fn, NULL, NULL, NULL);
	if (rc != SQLITE_OK) return rc;
	rc = sqlite3_create_function_v2(db, "air_pg_ilike", 3, SQLITE_UTF8 | SQLITE_DETERMINISTIC, NULL, air_pg_ilike_fn, NULL, NULL, NULL);
	if (rc != SQLITE_OK) return rc;
	rc = sqlite3_create_function_v2(db, "air_pg_regex", 3, SQLITE_UTF8 | SQLITE_DETERMINISTIC, NULL, air_pg_regex_fn, NULL, NULL, NULL);
	if (rc != SQLITE_OK) return rc;
	rc = sqlite3_create_function_v2(db, "air_pg_num_add", 2, SQLITE_UTF8 | SQLITE_DETERMINISTIC, NULL, air_pg_num_add_fn, NULL, NULL, NULL);
	if (rc != SQLITE_OK) return rc;
	rc = sqlite3_create_function_v2(db, "air_pg_num_sub", 2, SQLITE_UTF8 | SQLITE_DETERMINISTIC, NULL, air_pg_num_sub_fn, NULL, NULL, NULL);
	if (rc != SQLITE_OK) return rc;
	rc = sqlite3_create_function_v2(db, "air_pg_num_mul", 2, SQLITE_UTF8 | SQLITE_DETERMINISTIC, NULL, air_pg_num_mul_fn, NULL, NULL, NULL);
	if (rc != SQLITE_OK) return rc;
	rc = sqlite3_create_function_v2(db, "air_pg_num_div", 2, SQLITE_UTF8 | SQLITE_DETERMINISTIC, NULL, air_pg_num_div_fn, NULL, NULL, NULL);
	if (rc != SQLITE_OK) return rc;
	rc = sqlite3_create_function_v2(db, "air_pg_num_sum", 1, SQLITE_UTF8, NULL, NULL, air_pg_num_agg_step, air_pg_num_sum_final, NULL);
	if (rc != SQLITE_OK) return rc;
	return sqlite3_create_function_v2(db, "air_pg_num_avg", 1, SQLITE_UTF8, NULL, NULL, air_pg_num_agg_step, air_pg_num_avg_final, NULL);
}
*/
import "C"

import (
	"fmt"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"unicode"
	"unicode/utf8"
	"unsafe"
)

func (c *SQLiteConn) registerPGFuncs() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.db == nil {
		return fmt.Errorf("sqlite db is closed")
	}
	if rc := C.air_register_pg_funcs(c.db); rc != C.SQLITE_OK {
		return fmt.Errorf("sqlite3_create_function_v2 pg_funcs failed: rc=%d %s", int(rc), C.GoString(C.sqlite3_errmsg(c.db)))
	}
	return c.registerPGFuncsExt()
}

func sqliteValToString(v *C.sqlite3_value) (string, bool) {
	if C.sqlite3_value_type(v) == C.SQLITE_NULL {
		return "", false
	}
	p := C.sqlite3_value_text(v)
	if p == nil {
		return "", true
	}
	n := C.sqlite3_value_bytes(v)
	return C.GoStringN((*C.char)(unsafe.Pointer(p)), n), true
}

func setSQLiteError(ctx *C.sqlite3_context, msg string) {
	cs := C.CString(msg)
	defer C.free(unsafe.Pointer(cs))
	C.sqlite3_result_error(ctx, cs, -1)
}

//export goAirPGIlike
func goAirPGIlike(ctx *C.sqlite3_context, argc C.int, argv **C.sqlite3_value) {
	args := unsafe.Slice(argv, int(argc))
	if len(args) < 2 {
		C.sqlite3_result_null(ctx)
		return
	}
	str, ok1 := sqliteValToString(args[0])
	pat, ok2 := sqliteValToString(args[1])
	if !ok1 || !ok2 {
		C.sqlite3_result_null(ctx)
		return
	}
	esc := '\\'
	if len(args) >= 3 {
		escStr, ok3 := sqliteValToString(args[2])
		if !ok3 {
			C.sqlite3_result_null(ctx)
			return
		}
		if len(escStr) == 0 {
			esc = -1
		} else {
			r, size := utf8.DecodeRuneInString(escStr)
			if size != len(escStr) {
				setSQLiteError(ctx, "invalid escape string")
				return
			}
			esc = r
		}
	}
	if pgILikeMatch(str, pat, esc) {
		C.sqlite3_result_int(ctx, 1)
	} else {
		C.sqlite3_result_int(ctx, 0)
	}
}

// pgILikeMatch implements PostgreSQL ILIKE pattern matching with UTF-8 case folding,
// '%' (0+ runes including '\n'), '_' (1 rune including '\n'), and escape character.
func pgILikeMatch(str, pat string, esc rune) bool {
	// Parse pattern into tokens (literal rune or wildcard '%' / '_')
	type tokKind uint8
	const (
		tokLit tokKind = iota
		tokAnyOne
		tokAnyMany
	)
	type patTok struct {
		kind tokKind
		r    rune
	}
	var toks []patTok
	for i := 0; i < len(pat); {
		r, w := utf8.DecodeRuneInString(pat[i:])
		i += w
		if esc >= 0 && r == esc {
			if i >= len(pat) {
				// Trailing escape in PG is an error, treat as literal
				toks = append(toks, patTok{kind: tokLit, r: r})
				break
			}
			nr, nw := utf8.DecodeRuneInString(pat[i:])
			i += nw
			toks = append(toks, patTok{kind: tokLit, r: unicode.ToLower(nr)})
			continue
		}
		switch r {
		case '%':
			if len(toks) == 0 || toks[len(toks)-1].kind != tokAnyMany {
				toks = append(toks, patTok{kind: tokAnyMany})
			}
		case '_':
			toks = append(toks, patTok{kind: tokAnyOne})
		default:
			toks = append(toks, patTok{kind: tokLit, r: unicode.ToLower(r)})
		}
	}

	// Two-pointer / backtracking match over string runes
	si, ti := 0, 0
	starTI, starSI := -1, 0
	for si < len(str) {
		sr, sw := utf8.DecodeRuneInString(str[si:])
		if ti < len(toks) {
			switch toks[ti].kind {
			case tokLit:
				if unicode.ToLower(sr) == toks[ti].r {
					si += sw
					ti++
					continue
				}
			case tokAnyOne:
				si += sw
				ti++
				continue
			case tokAnyMany:
				starTI = ti
				starSI = si
				ti++
				continue
			}
		}
		if starTI >= 0 {
			ti = starTI + 1
			_, swStar := utf8.DecodeRuneInString(str[starSI:])
			starSI += swStar
			si = starSI
			continue
		}
		return false
	}
	for ti < len(toks) && toks[ti].kind == tokAnyMany {
		ti++
	}
	return ti == len(toks)
}

// isSafePGRegex checks whether a PostgreSQL regex pattern belongs to the subset
// whose semantics are identical to Go RE2 (with (?s) dotall enabled).
func isSafePGRegex(pat string) bool {
	if strings.Contains(pat, "[[:<:]]") || strings.Contains(pat, "[[:>:]]") {
		return false
	}
	inClass := false
	for i := 0; i < len(pat); i++ {
		c := pat[i]
		if c == '\\' {
			if i+1 >= len(pat) {
				return false
			}
			next := pat[i+1]
			if !inClass {
				// Backreferences \1..\9, PG word boundaries \y,\Y,\m,\M, or \A,\Z
				if (next >= '1' && next <= '9') || next == 'y' || next == 'Y' || next == 'm' || next == 'M' || next == 'A' || next == 'Z' {
					return false
				}
			}
			i++
			continue
		}
		if c == '[' && !inClass {
			inClass = true
			continue
		}
		if c == ']' && inClass {
			inClass = false
			continue
		}
		if !inClass && c == '(' && i+1 < len(pat) && pat[i+1] == '?' {
			// Lookarounds or PG-only embedded flags
			if i+2 >= len(pat) {
				return false
			}
			n := pat[i+2]
			switch n {
			case '=', '!', '<': // lookahead / lookbehind
				return false
			case 'b', 'e', 'q', 'c', 'n', 'p', 'w', 't', 'x': // PG ARE flags
				return false
			}
		}
	}
	return true
}

var pgRegexCache sync.Map // key: flags+"\x00"+pat -> *regexp.Regexp

func compileSafePGRegex(pat, flags string) (*regexp.Regexp, error) {
	if !isSafePGRegex(pat) {
		return nil, fmt.Errorf("%w: regex construct requires primary fallback", ErrUnsupportedType)
	}
	key := flags + "\x00" + pat
	if v, ok := pgRegexCache.Load(key); ok {
		return v.(*regexp.Regexp), nil
	}
	// In PostgreSQL, '.' matches '\n' by default (RE2 's' flag).
	prefix := "(?s)"
	if strings.Contains(flags, "i") {
		prefix = "(?is)"
	}
	re, err := regexp.Compile(prefix + pat)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid or unsupported regex %q: %v", ErrUnsupportedType, pat, err)
	}
	pgRegexCache.Store(key, re)
	return re, nil
}

//export goAirPGRegex
func goAirPGRegex(ctx *C.sqlite3_context, argc C.int, argv **C.sqlite3_value) {
	args := unsafe.Slice(argv, int(argc))
	if len(args) < 3 {
		C.sqlite3_result_null(ctx)
		return
	}
	str, ok1 := sqliteValToString(args[0])
	pat, ok2 := sqliteValToString(args[1])
	flags, ok3 := sqliteValToString(args[2])
	if !ok1 || !ok2 || !ok3 {
		C.sqlite3_result_null(ctx)
		return
	}
	re, err := compileSafePGRegex(pat, flags)
	if err != nil {
		setSQLiteError(ctx, err.Error())
		return
	}
	if re.MatchString(str) {
		C.sqlite3_result_int(ctx, 1)
	} else {
		C.sqlite3_result_int(ctx, 0)
	}
}

func setSQLiteText(ctx *C.sqlite3_context, s string) {
	if len(s) == 0 {
		cs := C.CString("")
		C.sqlite3_result_text(ctx, cs, 0, (*[0]byte)(C.free))
		return
	}
	cs := C.CString(s)
	C.sqlite3_result_text(ctx, cs, C.int(len(s)), (*[0]byte)(C.free))
}

//export goAirPGNumOp
func goAirPGNumOp(ctx *C.sqlite3_context, argc C.int, argv **C.sqlite3_value, op C.int) {
	args := unsafe.Slice(argv, int(argc))
	if len(args) < 2 {
		C.sqlite3_result_null(ctx)
		return
	}
	a, ok1 := sqliteValToString(args[0])
	b, ok2 := sqliteValToString(args[1])
	if !ok1 || !ok2 {
		C.sqlite3_result_null(ctx)
		return
	}
	var res string
	var err error
	switch int(op) {
	case 0:
		res, err = PGNumericAdd(a, b)
	case 1:
		res, err = PGNumericSub(a, b)
	case 2:
		res, err = PGNumericMul(a, b)
	case 3:
		res, err = PGNumericDiv(a, b)
	default:
		err = fmt.Errorf("unknown numeric op %d", int(op))
	}
	if err != nil {
		setSQLiteError(ctx, err.Error())
		return
	}
	setSQLiteText(ctx, res)
}

var (
	numAggSeq    atomic.Uint64
	numAggMu     sync.Mutex
	numAggStates = make(map[uint64]*pgNumericAgg)
)

//export goAirPGNumAggStep
func goAirPGNumAggStep(ctx *C.sqlite3_context, aggCtx unsafe.Pointer, argc C.int, argv **C.sqlite3_value) {
	if aggCtx == nil || int(argc) < 1 {
		return
	}
	args := unsafe.Slice(argv, int(argc))
	val, ok := sqliteValToString(args[0])
	if !ok {
		return
	}
	idPtr := (*uint64)(aggCtx)
	id := *idPtr
	var agg *pgNumericAgg
	numAggMu.Lock()
	if id == 0 {
		id = numAggSeq.Add(1)
		*idPtr = id
		agg = newPGNumericAgg()
		numAggStates[id] = agg
	} else {
		agg = numAggStates[id]
	}
	numAggMu.Unlock()
	if agg != nil {
		if err := agg.Step(val); err != nil {
			setSQLiteError(ctx, err.Error())
		}
	}
}

//export goAirPGNumAggFinal
func goAirPGNumAggFinal(ctx *C.sqlite3_context, aggCtx unsafe.Pointer, isAvg C.int) {
	if aggCtx == nil {
		C.sqlite3_result_null(ctx)
		return
	}
	id := *(*uint64)(aggCtx)
	if id == 0 {
		C.sqlite3_result_null(ctx)
		return
	}
	numAggMu.Lock()
	agg := numAggStates[id]
	delete(numAggStates, id)
	numAggMu.Unlock()
	if agg == nil {
		C.sqlite3_result_null(ctx)
		return
	}
	if isAvg != 0 {
		res, ok, err := agg.AvgResult()
		if err != nil {
			setSQLiteError(ctx, err.Error())
			return
		}
		if !ok {
			C.sqlite3_result_null(ctx)
			return
		}
		setSQLiteText(ctx, res)
		return
	}
	res, ok := agg.SumResult()
	if !ok {
		C.sqlite3_result_null(ctx)
		return
	}
	setSQLiteText(ctx, res)
}

