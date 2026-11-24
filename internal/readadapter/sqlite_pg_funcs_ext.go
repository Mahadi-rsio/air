package readadapter

/*
#include <sqlite3.h>
#include <stdlib.h>

extern void goAirPGCastBool(sqlite3_context *ctx, int argc, sqlite3_value **argv);
extern void goAirPGCastNumeric(sqlite3_context *ctx, int argc, sqlite3_value **argv);
extern void goAirPGLeft(sqlite3_context *ctx, int argc, sqlite3_value **argv);
extern void goAirPGRight(sqlite3_context *ctx, int argc, sqlite3_value **argv);
extern void goAirPGSplitPart(sqlite3_context *ctx, int argc, sqlite3_value **argv);
extern void goAirPGStrpos(sqlite3_context *ctx, int argc, sqlite3_value **argv);
extern void goAirPGRepeat(sqlite3_context *ctx, int argc, sqlite3_value **argv);
extern void goAirPGReverse(sqlite3_context *ctx, int argc, sqlite3_value **argv);
extern void goAirPGLpad(sqlite3_context *ctx, int argc, sqlite3_value **argv);
extern void goAirPGRpad(sqlite3_context *ctx, int argc, sqlite3_value **argv);
extern void goAirPGInitcap(sqlite3_context *ctx, int argc, sqlite3_value **argv);
extern void goAirPGConcat(sqlite3_context *ctx, int argc, sqlite3_value **argv);
extern void goAirPGConcatWS(sqlite3_context *ctx, int argc, sqlite3_value **argv);
extern void goAirPGCharLen(sqlite3_context *ctx, int argc, sqlite3_value **argv);
extern void goAirPGMD5(sqlite3_context *ctx, int argc, sqlite3_value **argv);
extern void goAirPGTranslate(sqlite3_context *ctx, int argc, sqlite3_value **argv);
extern void goAirPGRegexpReplace(sqlite3_context *ctx, int argc, sqlite3_value **argv);
extern void goAirPGGreatestLeast(sqlite3_context *ctx, int argc, sqlite3_value **argv, int isLeast);
extern void goAirPGNow(sqlite3_context *ctx);
extern void goAirPGDatePart(sqlite3_context *ctx, int argc, sqlite3_value **argv);
extern void goAirPGDateTrunc(sqlite3_context *ctx, int argc, sqlite3_value **argv);
extern void goAirPGToChar(sqlite3_context *ctx, int argc, sqlite3_value **argv);
extern void goAirPGGenRandomUUID(sqlite3_context *ctx);
extern void goAirPGCurrentSchema(sqlite3_context *ctx);
extern void goAirPGStringAggStep(sqlite3_context *ctx, void *aggCtx, int argc, sqlite3_value **argv);
extern void goAirPGStringAggFinal(sqlite3_context *ctx, void *aggCtx);
extern void goAirPGBoolAggStep(sqlite3_context *ctx, void *aggCtx, int argc, sqlite3_value **argv, int isOr);
extern void goAirPGBoolAggFinal(sqlite3_context *ctx, void *aggCtx);

static void air_pg_cast_bool_fn(sqlite3_context *c, int a, sqlite3_value **v) { goAirPGCastBool(c, a, v); }
static void air_pg_cast_num_fn(sqlite3_context *c, int a, sqlite3_value **v) { goAirPGCastNumeric(c, a, v); }
static void air_pg_left_fn(sqlite3_context *c, int a, sqlite3_value **v) { goAirPGLeft(c, a, v); }
static void air_pg_right_fn(sqlite3_context *c, int a, sqlite3_value **v) { goAirPGRight(c, a, v); }
static void air_pg_split_part_fn(sqlite3_context *c, int a, sqlite3_value **v) { goAirPGSplitPart(c, a, v); }
static void air_pg_strpos_fn(sqlite3_context *c, int a, sqlite3_value **v) { goAirPGStrpos(c, a, v); }
static void air_pg_repeat_fn(sqlite3_context *c, int a, sqlite3_value **v) { goAirPGRepeat(c, a, v); }
static void air_pg_reverse_fn(sqlite3_context *c, int a, sqlite3_value **v) { goAirPGReverse(c, a, v); }
static void air_pg_lpad_fn(sqlite3_context *c, int a, sqlite3_value **v) { goAirPGLpad(c, a, v); }
static void air_pg_rpad_fn(sqlite3_context *c, int a, sqlite3_value **v) { goAirPGRpad(c, a, v); }
static void air_pg_initcap_fn(sqlite3_context *c, int a, sqlite3_value **v) { goAirPGInitcap(c, a, v); }
static void air_pg_concat_fn(sqlite3_context *c, int a, sqlite3_value **v) { goAirPGConcat(c, a, v); }
static void air_pg_concat_ws_fn(sqlite3_context *c, int a, sqlite3_value **v) { goAirPGConcatWS(c, a, v); }
static void air_pg_char_len_fn(sqlite3_context *c, int a, sqlite3_value **v) { goAirPGCharLen(c, a, v); }
static void air_pg_md5_fn(sqlite3_context *c, int a, sqlite3_value **v) { goAirPGMD5(c, a, v); }
static void air_pg_translate_fn(sqlite3_context *c, int a, sqlite3_value **v) { goAirPGTranslate(c, a, v); }
static void air_pg_regexp_replace_fn(sqlite3_context *c, int a, sqlite3_value **v) { goAirPGRegexpReplace(c, a, v); }
static void air_pg_greatest_fn(sqlite3_context *c, int a, sqlite3_value **v) { goAirPGGreatestLeast(c, a, v, 0); }
static void air_pg_least_fn(sqlite3_context *c, int a, sqlite3_value **v) { goAirPGGreatestLeast(c, a, v, 1); }
static void air_pg_now_fn(sqlite3_context *c, int a, sqlite3_value **v) { (void)a; (void)v; goAirPGNow(c); }
static void air_pg_date_part_fn(sqlite3_context *c, int a, sqlite3_value **v) { goAirPGDatePart(c, a, v); }
static void air_pg_date_trunc_fn(sqlite3_context *c, int a, sqlite3_value **v) { goAirPGDateTrunc(c, a, v); }
static void air_pg_to_char_fn(sqlite3_context *c, int a, sqlite3_value **v) { goAirPGToChar(c, a, v); }
static void air_pg_uuid_fn(sqlite3_context *c, int a, sqlite3_value **v) { (void)a; (void)v; goAirPGGenRandomUUID(c); }
static void air_pg_cur_schema_fn(sqlite3_context *c, int a, sqlite3_value **v) { (void)a; (void)v; goAirPGCurrentSchema(c); }

static void air_pg_string_agg_step(sqlite3_context *c, int a, sqlite3_value **v) {
	void *p = sqlite3_aggregate_context(c, 8);
	goAirPGStringAggStep(c, p, a, v);
}
static void air_pg_string_agg_final(sqlite3_context *c) {
	void *p = sqlite3_aggregate_context(c, 0);
	goAirPGStringAggFinal(c, p);
}
static void air_pg_bool_and_step(sqlite3_context *c, int a, sqlite3_value **v) {
	void *p = sqlite3_aggregate_context(c, 8);
	goAirPGBoolAggStep(c, p, a, v, 0);
}
static void air_pg_bool_or_step(sqlite3_context *c, int a, sqlite3_value **v) {
	void *p = sqlite3_aggregate_context(c, 8);
	goAirPGBoolAggStep(c, p, a, v, 1);
}
static void air_pg_bool_agg_final(sqlite3_context *c) {
	void *p = sqlite3_aggregate_context(c, 0);
	goAirPGBoolAggFinal(c, p);
}

static int air_register_pg_funcs_ext(sqlite3 *db) {
	int det = SQLITE_UTF8 | SQLITE_DETERMINISTIC;
	int rc;
	if ((rc = sqlite3_create_function_v2(db, "air_pg_cast_bool", 1, det, NULL, air_pg_cast_bool_fn, NULL, NULL, NULL)) != SQLITE_OK) return rc;
	if ((rc = sqlite3_create_function_v2(db, "air_pg_cast_numeric", 1, det, NULL, air_pg_cast_num_fn, NULL, NULL, NULL)) != SQLITE_OK) return rc;
	if ((rc = sqlite3_create_function_v2(db, "left", 2, det, NULL, air_pg_left_fn, NULL, NULL, NULL)) != SQLITE_OK) return rc;
	if ((rc = sqlite3_create_function_v2(db, "right", 2, det, NULL, air_pg_right_fn, NULL, NULL, NULL)) != SQLITE_OK) return rc;
	if ((rc = sqlite3_create_function_v2(db, "split_part", 3, det, NULL, air_pg_split_part_fn, NULL, NULL, NULL)) != SQLITE_OK) return rc;
	if ((rc = sqlite3_create_function_v2(db, "strpos", 2, det, NULL, air_pg_strpos_fn, NULL, NULL, NULL)) != SQLITE_OK) return rc;
	if ((rc = sqlite3_create_function_v2(db, "repeat", 2, det, NULL, air_pg_repeat_fn, NULL, NULL, NULL)) != SQLITE_OK) return rc;
	if ((rc = sqlite3_create_function_v2(db, "reverse", 1, det, NULL, air_pg_reverse_fn, NULL, NULL, NULL)) != SQLITE_OK) return rc;
	if ((rc = sqlite3_create_function_v2(db, "lpad", 2, det, NULL, air_pg_lpad_fn, NULL, NULL, NULL)) != SQLITE_OK) return rc;
	if ((rc = sqlite3_create_function_v2(db, "lpad", 3, det, NULL, air_pg_lpad_fn, NULL, NULL, NULL)) != SQLITE_OK) return rc;
	if ((rc = sqlite3_create_function_v2(db, "rpad", 2, det, NULL, air_pg_rpad_fn, NULL, NULL, NULL)) != SQLITE_OK) return rc;
	if ((rc = sqlite3_create_function_v2(db, "rpad", 3, det, NULL, air_pg_rpad_fn, NULL, NULL, NULL)) != SQLITE_OK) return rc;
	if ((rc = sqlite3_create_function_v2(db, "initcap", 1, det, NULL, air_pg_initcap_fn, NULL, NULL, NULL)) != SQLITE_OK) return rc;
	if ((rc = sqlite3_create_function_v2(db, "concat", -1, det, NULL, air_pg_concat_fn, NULL, NULL, NULL)) != SQLITE_OK) return rc;
	if ((rc = sqlite3_create_function_v2(db, "concat_ws", -1, det, NULL, air_pg_concat_ws_fn, NULL, NULL, NULL)) != SQLITE_OK) return rc;
	if ((rc = sqlite3_create_function_v2(db, "char_length", 1, det, NULL, air_pg_char_len_fn, NULL, NULL, NULL)) != SQLITE_OK) return rc;
	if ((rc = sqlite3_create_function_v2(db, "character_length", 1, det, NULL, air_pg_char_len_fn, NULL, NULL, NULL)) != SQLITE_OK) return rc;
	if ((rc = sqlite3_create_function_v2(db, "md5", 1, det, NULL, air_pg_md5_fn, NULL, NULL, NULL)) != SQLITE_OK) return rc;
	if ((rc = sqlite3_create_function_v2(db, "translate", 3, det, NULL, air_pg_translate_fn, NULL, NULL, NULL)) != SQLITE_OK) return rc;
	if ((rc = sqlite3_create_function_v2(db, "regexp_replace", 3, det, NULL, air_pg_regexp_replace_fn, NULL, NULL, NULL)) != SQLITE_OK) return rc;
	if ((rc = sqlite3_create_function_v2(db, "regexp_replace", 4, det, NULL, air_pg_regexp_replace_fn, NULL, NULL, NULL)) != SQLITE_OK) return rc;
	if ((rc = sqlite3_create_function_v2(db, "greatest", -1, det, NULL, air_pg_greatest_fn, NULL, NULL, NULL)) != SQLITE_OK) return rc;
	if ((rc = sqlite3_create_function_v2(db, "least", -1, det, NULL, air_pg_least_fn, NULL, NULL, NULL)) != SQLITE_OK) return rc;
	if ((rc = sqlite3_create_function_v2(db, "now", 0, SQLITE_UTF8, NULL, air_pg_now_fn, NULL, NULL, NULL)) != SQLITE_OK) return rc;
	if ((rc = sqlite3_create_function_v2(db, "air_pg_date_part", 2, det, NULL, air_pg_date_part_fn, NULL, NULL, NULL)) != SQLITE_OK) return rc;
	if ((rc = sqlite3_create_function_v2(db, "date_part", 2, det, NULL, air_pg_date_part_fn, NULL, NULL, NULL)) != SQLITE_OK) return rc;
	if ((rc = sqlite3_create_function_v2(db, "date_trunc", 2, det, NULL, air_pg_date_trunc_fn, NULL, NULL, NULL)) != SQLITE_OK) return rc;
	if ((rc = sqlite3_create_function_v2(db, "to_char", 2, det, NULL, air_pg_to_char_fn, NULL, NULL, NULL)) != SQLITE_OK) return rc;
	if ((rc = sqlite3_create_function_v2(db, "gen_random_uuid", 0, SQLITE_UTF8, NULL, air_pg_uuid_fn, NULL, NULL, NULL)) != SQLITE_OK) return rc;
	if ((rc = sqlite3_create_function_v2(db, "uuid_generate_v4", 0, SQLITE_UTF8, NULL, air_pg_uuid_fn, NULL, NULL, NULL)) != SQLITE_OK) return rc;
	if ((rc = sqlite3_create_function_v2(db, "current_schema", 0, SQLITE_UTF8, NULL, air_pg_cur_schema_fn, NULL, NULL, NULL)) != SQLITE_OK) return rc;
	if ((rc = sqlite3_create_function_v2(db, "string_agg", 2, SQLITE_UTF8, NULL, NULL, air_pg_string_agg_step, air_pg_string_agg_final, NULL)) != SQLITE_OK) return rc;
	if ((rc = sqlite3_create_function_v2(db, "bool_and", 1, SQLITE_UTF8, NULL, NULL, air_pg_bool_and_step, air_pg_bool_agg_final, NULL)) != SQLITE_OK) return rc;
	if ((rc = sqlite3_create_function_v2(db, "every", 1, SQLITE_UTF8, NULL, NULL, air_pg_bool_and_step, air_pg_bool_agg_final, NULL)) != SQLITE_OK) return rc;
	return sqlite3_create_function_v2(db, "bool_or", 1, SQLITE_UTF8, NULL, NULL, air_pg_bool_or_step, air_pg_bool_agg_final, NULL);
}
*/
import "C"

import (
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"
	"unicode/utf8"
	"unsafe"
)

func parsePGBoolValue(v *C.sqlite3_value) (bool, bool, error) {
	vt := C.sqlite3_value_type(v)
	if vt == C.SQLITE_NULL {
		return false, false, nil
	}
	if vt == C.SQLITE_INTEGER {
		return C.sqlite3_value_int64(v) != 0, true, nil
	}
	s, ok := sqliteValToString(v)
	if !ok {
		return false, false, nil
	}
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "t", "true", "y", "yes", "on", "1":
		return true, true, nil
	case "f", "false", "n", "no", "off", "0":
		return false, true, nil
	default:
		return false, true, fmt.Errorf("invalid input syntax for type boolean: %q", s)
	}
}

//export goAirPGCastBool
func goAirPGCastBool(ctx *C.sqlite3_context, argc C.int, argv **C.sqlite3_value) {
	args := unsafe.Slice(argv, int(argc))
	if len(args) < 1 {
		C.sqlite3_result_null(ctx)
		return
	}
	b, nonNull, err := parsePGBoolValue(args[0])
	if err != nil {
		setSQLiteError(ctx, err.Error())
		return
	}
	if !nonNull {
		C.sqlite3_result_null(ctx)
		return
	}
	if b {
		C.sqlite3_result_int(ctx, 1)
	} else {
		C.sqlite3_result_int(ctx, 0)
	}
}

//export goAirPGCastNumeric
func goAirPGCastNumeric(ctx *C.sqlite3_context, argc C.int, argv **C.sqlite3_value) {
	args := unsafe.Slice(argv, int(argc))
	if len(args) < 1 {
		C.sqlite3_result_null(ctx)
		return
	}
	s, ok := sqliteValToString(args[0])
	if !ok {
		C.sqlite3_result_null(ctx)
		return
	}
	norm, err := PGNumericAdd(strings.TrimSpace(s), "0")
	if err != nil {
		setSQLiteError(ctx, fmt.Sprintf("invalid input syntax for type numeric: %q", s))
		return
	}
	setSQLiteText(ctx, norm)
}

//export goAirPGLeft
func goAirPGLeft(ctx *C.sqlite3_context, argc C.int, argv **C.sqlite3_value) {
	args := unsafe.Slice(argv, int(argc))
	if len(args) < 2 || C.sqlite3_value_type(args[0]) == C.SQLITE_NULL || C.sqlite3_value_type(args[1]) == C.SQLITE_NULL {
		C.sqlite3_result_null(ctx)
		return
	}
	s, _ := sqliteValToString(args[0])
	n := int(C.sqlite3_value_int64(args[1]))
	runes := []rune(s)
	if n >= 0 {
		if n > len(runes) {
			n = len(runes)
		}
		setSQLiteText(ctx, string(runes[:n]))
		return
	}
	cut := len(runes) + n
	if cut <= 0 {
		setSQLiteText(ctx, "")
		return
	}
	setSQLiteText(ctx, string(runes[:cut]))
}

//export goAirPGRight
func goAirPGRight(ctx *C.sqlite3_context, argc C.int, argv **C.sqlite3_value) {
	args := unsafe.Slice(argv, int(argc))
	if len(args) < 2 || C.sqlite3_value_type(args[0]) == C.SQLITE_NULL || C.sqlite3_value_type(args[1]) == C.SQLITE_NULL {
		C.sqlite3_result_null(ctx)
		return
	}
	s, _ := sqliteValToString(args[0])
	n := int(C.sqlite3_value_int64(args[1]))
	runes := []rune(s)
	if n >= 0 {
		if n >= len(runes) {
			setSQLiteText(ctx, s)
			return
		}
		setSQLiteText(ctx, string(runes[len(runes)-n:]))
		return
	}
	start := -n
	if start >= len(runes) {
		setSQLiteText(ctx, "")
		return
	}
	setSQLiteText(ctx, string(runes[start:]))
}

//export goAirPGSplitPart
func goAirPGSplitPart(ctx *C.sqlite3_context, argc C.int, argv **C.sqlite3_value) {
	args := unsafe.Slice(argv, int(argc))
	if len(args) < 3 || C.sqlite3_value_type(args[0]) == C.SQLITE_NULL || C.sqlite3_value_type(args[1]) == C.SQLITE_NULL || C.sqlite3_value_type(args[2]) == C.SQLITE_NULL {
		C.sqlite3_result_null(ctx)
		return
	}
	s, _ := sqliteValToString(args[0])
	delim, _ := sqliteValToString(args[1])
	field := int(C.sqlite3_value_int64(args[2]))
	if field <= 0 {
		setSQLiteError(ctx, "field position must be greater than zero")
		return
	}
	if delim == "" {
		if field == 1 {
			setSQLiteText(ctx, s)
		} else {
			setSQLiteText(ctx, "")
		}
		return
	}
	parts := strings.Split(s, delim)
	if field > len(parts) {
		setSQLiteText(ctx, "")
		return
	}
	setSQLiteText(ctx, parts[field-1])
}

//export goAirPGStrpos
func goAirPGStrpos(ctx *C.sqlite3_context, argc C.int, argv **C.sqlite3_value) {
	args := unsafe.Slice(argv, int(argc))
	if len(args) < 2 {
		C.sqlite3_result_null(ctx)
		return
	}
	s, ok1 := sqliteValToString(args[0])
	sub, ok2 := sqliteValToString(args[1])
	if !ok1 || !ok2 {
		C.sqlite3_result_null(ctx)
		return
	}
	idx := strings.Index(s, sub)
	if idx < 0 {
		C.sqlite3_result_int(ctx, 0)
		return
	}
	C.sqlite3_result_int64(ctx, C.sqlite3_int64(utf8.RuneCountInString(s[:idx])+1))
}

//export goAirPGRepeat
func goAirPGRepeat(ctx *C.sqlite3_context, argc C.int, argv **C.sqlite3_value) {
	args := unsafe.Slice(argv, int(argc))
	if len(args) < 2 || C.sqlite3_value_type(args[0]) == C.SQLITE_NULL || C.sqlite3_value_type(args[1]) == C.SQLITE_NULL {
		C.sqlite3_result_null(ctx)
		return
	}
	s, _ := sqliteValToString(args[0])
	n := int(C.sqlite3_value_int64(args[1]))
	if n <= 0 {
		setSQLiteText(ctx, "")
		return
	}
	if n > 100000 {
		setSQLiteError(ctx, "repeat count exceeds limit")
		return
	}
	setSQLiteText(ctx, strings.Repeat(s, n))
}

//export goAirPGReverse
func goAirPGReverse(ctx *C.sqlite3_context, argc C.int, argv **C.sqlite3_value) {
	args := unsafe.Slice(argv, int(argc))
	if len(args) < 1 {
		C.sqlite3_result_null(ctx)
		return
	}
	s, ok := sqliteValToString(args[0])
	if !ok {
		C.sqlite3_result_null(ctx)
		return
	}
	runes := []rune(s)
	for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
		runes[i], runes[j] = runes[j], runes[i]
	}
	setSQLiteText(ctx, string(runes))
}

func pgPad(s string, targetLen int, fill string, left bool) string {
	if targetLen <= 0 {
		return ""
	}
	sr := []rune(s)
	if len(sr) >= targetLen {
		return string(sr[:targetLen])
	}
	fr := []rune(fill)
	if len(fr) == 0 {
		return s
	}
	need := targetLen - len(sr)
	pad := make([]rune, need)
	for i := 0; i < need; i++ {
		pad[i] = fr[i%len(fr)]
	}
	if left {
		return string(pad) + s
	}
	return s + string(pad)
}

//export goAirPGLpad
func goAirPGLpad(ctx *C.sqlite3_context, argc C.int, argv **C.sqlite3_value) {
	args := unsafe.Slice(argv, int(argc))
	if len(args) < 2 || C.sqlite3_value_type(args[0]) == C.SQLITE_NULL || C.sqlite3_value_type(args[1]) == C.SQLITE_NULL {
		C.sqlite3_result_null(ctx)
		return
	}
	s, _ := sqliteValToString(args[0])
	n := int(C.sqlite3_value_int64(args[1]))
	fill := " "
	if len(args) >= 3 {
		var ok bool
		fill, ok = sqliteValToString(args[2])
		if !ok {
			C.sqlite3_result_null(ctx)
			return
		}
	}
	setSQLiteText(ctx, pgPad(s, n, fill, true))
}

//export goAirPGRpad
func goAirPGRpad(ctx *C.sqlite3_context, argc C.int, argv **C.sqlite3_value) {
	args := unsafe.Slice(argv, int(argc))
	if len(args) < 2 || C.sqlite3_value_type(args[0]) == C.SQLITE_NULL || C.sqlite3_value_type(args[1]) == C.SQLITE_NULL {
		C.sqlite3_result_null(ctx)
		return
	}
	s, _ := sqliteValToString(args[0])
	n := int(C.sqlite3_value_int64(args[1]))
	fill := " "
	if len(args) >= 3 {
		var ok bool
		fill, ok = sqliteValToString(args[2])
		if !ok {
			C.sqlite3_result_null(ctx)
			return
		}
	}
	setSQLiteText(ctx, pgPad(s, n, fill, false))
}

//export goAirPGInitcap
func goAirPGInitcap(ctx *C.sqlite3_context, argc C.int, argv **C.sqlite3_value) {
	args := unsafe.Slice(argv, int(argc))
	if len(args) < 1 {
		C.sqlite3_result_null(ctx)
		return
	}
	s, ok := sqliteValToString(args[0])
	if !ok {
		C.sqlite3_result_null(ctx)
		return
	}
	var b strings.Builder
	b.Grow(len(s))
	newWord := true
	for _, r := range s {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			if newWord {
				b.WriteRune(unicode.ToUpper(r))
				newWord = false
			} else {
				b.WriteRune(unicode.ToLower(r))
			}
		} else {
			b.WriteRune(r)
			newWord = true
		}
	}
	setSQLiteText(ctx, b.String())
}

//export goAirPGConcat
func goAirPGConcat(ctx *C.sqlite3_context, argc C.int, argv **C.sqlite3_value) {
	args := unsafe.Slice(argv, int(argc))
	var b strings.Builder
	for _, a := range args {
		if s, ok := sqliteValToString(a); ok {
			b.WriteString(s)
		}
	}
	setSQLiteText(ctx, b.String())
}

//export goAirPGConcatWS
func goAirPGConcatWS(ctx *C.sqlite3_context, argc C.int, argv **C.sqlite3_value) {
	args := unsafe.Slice(argv, int(argc))
	if len(args) < 1 {
		C.sqlite3_result_null(ctx)
		return
	}
	sep, ok := sqliteValToString(args[0])
	if !ok {
		C.sqlite3_result_null(ctx)
		return
	}
	var b strings.Builder
	first := true
	for _, a := range args[1:] {
		if s, ok := sqliteValToString(a); ok {
			if !first {
				b.WriteString(sep)
			}
			b.WriteString(s)
			first = false
		}
	}
	setSQLiteText(ctx, b.String())
}

//export goAirPGCharLen
func goAirPGCharLen(ctx *C.sqlite3_context, argc C.int, argv **C.sqlite3_value) {
	args := unsafe.Slice(argv, int(argc))
	if len(args) < 1 {
		C.sqlite3_result_null(ctx)
		return
	}
	s, ok := sqliteValToString(args[0])
	if !ok {
		C.sqlite3_result_null(ctx)
		return
	}
	C.sqlite3_result_int64(ctx, C.sqlite3_int64(utf8.RuneCountInString(s)))
}

//export goAirPGMD5
func goAirPGMD5(ctx *C.sqlite3_context, argc C.int, argv **C.sqlite3_value) {
	args := unsafe.Slice(argv, int(argc))
	if len(args) < 1 {
		C.sqlite3_result_null(ctx)
		return
	}
	s, ok := sqliteValToString(args[0])
	if !ok {
		C.sqlite3_result_null(ctx)
		return
	}
	sum := md5.Sum([]byte(s))
	setSQLiteText(ctx, hex.EncodeToString(sum[:]))
}

//export goAirPGTranslate
func goAirPGTranslate(ctx *C.sqlite3_context, argc C.int, argv **C.sqlite3_value) {
	args := unsafe.Slice(argv, int(argc))
	if len(args) < 3 {
		C.sqlite3_result_null(ctx)
		return
	}
	s, ok1 := sqliteValToString(args[0])
	from, ok2 := sqliteValToString(args[1])
	to, ok3 := sqliteValToString(args[2])
	if !ok1 || !ok2 || !ok3 {
		C.sqlite3_result_null(ctx)
		return
	}
	fromRunes := []rune(from)
	toRunes := []rune(to)
	trans := make(map[rune]int, len(fromRunes))
	for i, r := range fromRunes {
		if _, exists := trans[r]; !exists {
			trans[r] = i
		}
	}
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		if idx, found := trans[r]; found {
			if idx < len(toRunes) {
				b.WriteRune(toRunes[idx])
			}
		} else {
			b.WriteRune(r)
		}
	}
	setSQLiteText(ctx, b.String())
}

//export goAirPGRegexpReplace
func goAirPGRegexpReplace(ctx *C.sqlite3_context, argc C.int, argv **C.sqlite3_value) {
	args := unsafe.Slice(argv, int(argc))
	if len(args) < 3 {
		C.sqlite3_result_null(ctx)
		return
	}
	s, ok1 := sqliteValToString(args[0])
	pat, ok2 := sqliteValToString(args[1])
	repl, ok3 := sqliteValToString(args[2])
	if !ok1 || !ok2 || !ok3 {
		C.sqlite3_result_null(ctx)
		return
	}
	flags := ""
	if len(args) >= 4 {
		var ok4 bool
		flags, ok4 = sqliteValToString(args[3])
		if !ok4 {
			C.sqlite3_result_null(ctx)
			return
		}
	}
	re, err := compileSafePGRegex(pat, flags)
	if err != nil {
		setSQLiteError(ctx, err.Error())
		return
	}
	if strings.Contains(flags, "g") {
		setSQLiteText(ctx, re.ReplaceAllString(s, repl))
		return
	}
	loc := re.FindStringIndex(s)
	if loc == nil {
		setSQLiteText(ctx, s)
		return
	}
	out := s[:loc[0]] + re.ReplaceAllString(s[loc[0]:loc[1]], repl) + s[loc[1]:]
	setSQLiteText(ctx, out)
}

func compareSQLiteValues(a, b *C.sqlite3_value) int {
	ta := C.sqlite3_value_type(a)
	tb := C.sqlite3_value_type(b)
	if (ta == C.SQLITE_INTEGER || ta == C.SQLITE_FLOAT) && (tb == C.SQLITE_INTEGER || tb == C.SQLITE_FLOAT) {
		fa := float64(C.sqlite3_value_double(a))
		fb := float64(C.sqlite3_value_double(b))
		if fa < fb {
			return -1
		}
		if fa > fb {
			return 1
		}
		return 0
	}
	sa, _ := sqliteValToString(a)
	sb, _ := sqliteValToString(b)
	return strings.Compare(sa, sb)
}

//export goAirPGGreatestLeast
func goAirPGGreatestLeast(ctx *C.sqlite3_context, argc C.int, argv **C.sqlite3_value, isLeast C.int) {
	args := unsafe.Slice(argv, int(argc))
	var best *C.sqlite3_value
	for _, v := range args {
		if C.sqlite3_value_type(v) == C.SQLITE_NULL {
			continue
		}
		if best == nil {
			best = v
			continue
		}
		cmp := compareSQLiteValues(v, best)
		if (isLeast == 0 && cmp > 0) || (isLeast != 0 && cmp < 0) {
			best = v
		}
	}
	if best == nil {
		C.sqlite3_result_null(ctx)
		return
	}
	C.sqlite3_result_value(ctx, best)
}

//export goAirPGNow
func goAirPGNow(ctx *C.sqlite3_context) {
	setSQLiteText(ctx, time.Now().UTC().Format("2006-01-02 15:04:05.000000+00"))
}

func parsePGTimestamp(s string) (time.Time, error) {
	s = strings.TrimSpace(s)
	layouts := []string{
		time.RFC3339Nano,
		"2006-01-02 15:04:05.999999999-07:00",
		"2006-01-02 15:04:05.999999999-07",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05.999999999",
		"2006-01-02T15:04:05",
		"2006-01-02",
	}
	for _, l := range layouts {
		if t, err := time.Parse(l, s); err == nil {
			return t.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("invalid input syntax for type timestamp: %q", s)
}

//export goAirPGDatePart
func goAirPGDatePart(ctx *C.sqlite3_context, argc C.int, argv **C.sqlite3_value) {
	args := unsafe.Slice(argv, int(argc))
	if len(args) < 2 {
		C.sqlite3_result_null(ctx)
		return
	}
	field, ok1 := sqliteValToString(args[0])
	tsStr, ok2 := sqliteValToString(args[1])
	if !ok1 || !ok2 {
		C.sqlite3_result_null(ctx)
		return
	}
	t, err := parsePGTimestamp(tsStr)
	if err != nil {
		setSQLiteError(ctx, err.Error())
		return
	}
	switch strings.ToLower(strings.TrimSpace(field)) {
	case "year", "years":
		C.sqlite3_result_double(ctx, C.double(t.Year()))
	case "month", "months":
		C.sqlite3_result_double(ctx, C.double(int(t.Month())))
	case "day", "days":
		C.sqlite3_result_double(ctx, C.double(t.Day()))
	case "hour", "hours":
		C.sqlite3_result_double(ctx, C.double(t.Hour()))
	case "minute", "minutes":
		C.sqlite3_result_double(ctx, C.double(t.Minute()))
	case "second", "seconds":
		sec := float64(t.Second()) + float64(t.Nanosecond())/1e9
		C.sqlite3_result_double(ctx, C.double(sec))
	case "dow":
		C.sqlite3_result_double(ctx, C.double(int(t.Weekday())))
	case "isodow":
		d := int(t.Weekday())
		if d == 0 {
			d = 7
		}
		C.sqlite3_result_double(ctx, C.double(d))
	case "doy":
		C.sqlite3_result_double(ctx, C.double(t.YearDay()))
	case "quarter":
		C.sqlite3_result_double(ctx, C.double((int(t.Month())-1)/3+1))
	case "week":
		_, w := t.ISOWeek()
		C.sqlite3_result_double(ctx, C.double(w))
	case "epoch":
		ep := float64(t.Unix()) + float64(t.Nanosecond())/1e9
		C.sqlite3_result_double(ctx, C.double(ep))
	default:
		setSQLiteError(ctx, fmt.Sprintf("timestamp units %q not recognized", field))
	}
}

//export goAirPGDateTrunc
func goAirPGDateTrunc(ctx *C.sqlite3_context, argc C.int, argv **C.sqlite3_value) {
	args := unsafe.Slice(argv, int(argc))
	if len(args) < 2 {
		C.sqlite3_result_null(ctx)
		return
	}
	field, ok1 := sqliteValToString(args[0])
	tsStr, ok2 := sqliteValToString(args[1])
	if !ok1 || !ok2 {
		C.sqlite3_result_null(ctx)
		return
	}
	t, err := parsePGTimestamp(tsStr)
	if err != nil {
		setSQLiteError(ctx, err.Error())
		return
	}
	var out time.Time
	switch strings.ToLower(strings.TrimSpace(field)) {
	case "second", "seconds":
		out = time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), 0, time.UTC)
	case "minute", "minutes":
		out = time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), 0, 0, time.UTC)
	case "hour", "hours":
		out = time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, time.UTC)
	case "day", "days":
		out = time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
	case "week", "weeks":
		wd := int(t.Weekday())
		if wd == 0 {
			wd = 7
		}
		d := t.AddDate(0, 0, -(wd - 1))
		out = time.Date(d.Year(), d.Month(), d.Day(), 0, 0, 0, 0, time.UTC)
	case "month", "months":
		out = time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, time.UTC)
	case "quarter":
		qm := time.Month(((int(t.Month())-1)/3)*3 + 1)
		out = time.Date(t.Year(), qm, 1, 0, 0, 0, 0, time.UTC)
	case "year", "years":
		out = time.Date(t.Year(), 1, 1, 0, 0, 0, 0, time.UTC)
	default:
		setSQLiteError(ctx, fmt.Sprintf("timestamp units %q not recognized", field))
		return
	}
	setSQLiteText(ctx, out.Format("2006-01-02 15:04:05+00"))
}

//export goAirPGToChar
func goAirPGToChar(ctx *C.sqlite3_context, argc C.int, argv **C.sqlite3_value) {
	args := unsafe.Slice(argv, int(argc))
	if len(args) < 2 {
		C.sqlite3_result_null(ctx)
		return
	}
	tsStr, ok1 := sqliteValToString(args[0])
	fmtStr, ok2 := sqliteValToString(args[1])
	if !ok1 || !ok2 {
		C.sqlite3_result_null(ctx)
		return
	}
	t, err := parsePGTimestamp(tsStr)
	if err != nil {
		setSQLiteError(ctx, err.Error())
		return
	}
	r := strings.NewReplacer(
		"YYYY", fmt.Sprintf("%04d", t.Year()),
		"MM", fmt.Sprintf("%02d", int(t.Month())),
		"DD", fmt.Sprintf("%02d", t.Day()),
		"HH24", fmt.Sprintf("%02d", t.Hour()),
		"MI", fmt.Sprintf("%02d", t.Minute()),
		"SS", fmt.Sprintf("%02d", t.Second()),
	)
	setSQLiteText(ctx, r.Replace(fmtStr))
}

//export goAirPGGenRandomUUID
func goAirPGGenRandomUUID(ctx *C.sqlite3_context) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		setSQLiteError(ctx, err.Error())
		return
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	setSQLiteText(ctx, fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]))
}

//export goAirPGCurrentSchema
func goAirPGCurrentSchema(ctx *C.sqlite3_context) {
	setSQLiteText(ctx, "public")
}

type stringAggState struct {
	hasAny bool
	sep    string
	b      strings.Builder
}

var (
	strAggSeq    atomic.Uint64
	strAggMu     sync.Mutex
	strAggStates = make(map[uint64]*stringAggState)
)

//export goAirPGStringAggStep
func goAirPGStringAggStep(ctx *C.sqlite3_context, aggCtx unsafe.Pointer, argc C.int, argv **C.sqlite3_value) {
	if aggCtx == nil || int(argc) < 2 {
		return
	}
	args := unsafe.Slice(argv, int(argc))
	val, ok1 := sqliteValToString(args[0])
	if !ok1 {
		return
	}
	sep, ok2 := sqliteValToString(args[1])
	if !ok2 {
		return
	}
	idPtr := (*uint64)(aggCtx)
	id := *idPtr
	strAggMu.Lock()
	var st *stringAggState
	if id == 0 {
		id = strAggSeq.Add(1)
		*idPtr = id
		st = &stringAggState{sep: sep}
		strAggStates[id] = st
	} else {
		st = strAggStates[id]
	}
	strAggMu.Unlock()
	if st != nil {
		if st.hasAny {
			st.b.WriteString(sep)
		}
		st.b.WriteString(val)
		st.hasAny = true
	}
}

//export goAirPGStringAggFinal
func goAirPGStringAggFinal(ctx *C.sqlite3_context, aggCtx unsafe.Pointer) {
	if aggCtx == nil {
		C.sqlite3_result_null(ctx)
		return
	}
	id := *(*uint64)(aggCtx)
	if id == 0 {
		C.sqlite3_result_null(ctx)
		return
	}
	strAggMu.Lock()
	st := strAggStates[id]
	delete(strAggStates, id)
	strAggMu.Unlock()
	if st == nil || !st.hasAny {
		C.sqlite3_result_null(ctx)
		return
	}
	setSQLiteText(ctx, st.b.String())
}

type boolAggState struct {
	isOr   bool
	hasAny bool
	res    bool
}

var (
	boolAggSeq    atomic.Uint64
	boolAggMu     sync.Mutex
	boolAggStates = make(map[uint64]*boolAggState)
)

//export goAirPGBoolAggStep
func goAirPGBoolAggStep(ctx *C.sqlite3_context, aggCtx unsafe.Pointer, argc C.int, argv **C.sqlite3_value, isOr C.int) {
	if aggCtx == nil || int(argc) < 1 {
		return
	}
	args := unsafe.Slice(argv, int(argc))
	b, nonNull, err := parsePGBoolValue(args[0])
	if err != nil {
		setSQLiteError(ctx, err.Error())
		return
	}
	if !nonNull {
		return
	}
	idPtr := (*uint64)(aggCtx)
	id := *idPtr
	boolAggMu.Lock()
	var st *boolAggState
	if id == 0 {
		id = boolAggSeq.Add(1)
		*idPtr = id
		st = &boolAggState{isOr: isOr != 0, res: isOr == 0}
		boolAggStates[id] = st
	} else {
		st = boolAggStates[id]
	}
	boolAggMu.Unlock()
	if st != nil {
		st.hasAny = true
		if st.isOr {
			st.res = st.res || b
		} else {
			st.res = st.res && b
		}
	}
}

//export goAirPGBoolAggFinal
func goAirPGBoolAggFinal(ctx *C.sqlite3_context, aggCtx unsafe.Pointer) {
	if aggCtx == nil {
		C.sqlite3_result_null(ctx)
		return
	}
	id := *(*uint64)(aggCtx)
	if id == 0 {
		C.sqlite3_result_null(ctx)
		return
	}
	boolAggMu.Lock()
	st := boolAggStates[id]
	delete(boolAggStates, id)
	boolAggMu.Unlock()
	if st == nil || !st.hasAny {
		C.sqlite3_result_null(ctx)
		return
	}
	if st.res {
		C.sqlite3_result_int(ctx, 1)
	} else {
		C.sqlite3_result_int(ctx, 0)
	}
}

func (c *SQLiteConn) registerPGFuncsExt() error {
	rc := C.air_register_pg_funcs_ext(c.db)
	if rc != C.SQLITE_OK {
		return fmt.Errorf("sqlite3_create_function_v2 pg_funcs_ext failed: rc=%d %s", int(rc), C.GoString(C.sqlite3_errmsg(c.db)))
	}
	return nil
}

var _ = strconv.Itoa
