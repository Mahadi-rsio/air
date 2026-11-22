package readadapter

import (
	"context"
	"fmt"
	"sort"
	"sync/atomic"
)

const (
	// PostgreSQL indoption flags (include/catalog/pg_index.h).
	PGIndOptionDesc       int16 = 0x0001
	PGIndOptionNullsFirst int16 = 0x0002

	maxBTreeDepth     = 32
	maxBTreePagesRead = 65536
	maxHOTChainLen    = 64
)

var (
	vtabIndexPagesRead atomic.Uint64
	vtabIndexScans     atomic.Uint64
)

// VTabIndexStats returns cumulative B-tree index page read and index scan counters.
func VTabIndexStats() (indexPagesRead, indexScans uint64) {
	return vtabIndexPagesRead.Load(), vtabIndexScans.Load()
}

// btreeKeyOrdering describes the sort direction and NULL ordering for one index key column.
type btreeKeyOrdering struct {
	Desc       bool
	NullsFirst bool
}

func parseIndOption(opt int16) btreeKeyOrdering {
	desc := (opt & PGIndOptionDesc) != 0
	// In PostgreSQL B-tree:
	// - ASC default: NULLS LAST (INDOPTION_NULLS_FIRST not set)
	// - DESC default: NULLS FIRST (INDOPTION_NULLS_FIRST IS set in indoption by PG!)
	nullsFirst := (opt & PGIndOptionNullsFirst) != 0
	return btreeKeyOrdering{
		Desc:       desc,
		NullsFirst: nullsFirst,
	}
}

// compareFilterAndVal compares an index key HeapVal against a vtabFilter bound value
// in the raw data domain (ascending, before applying DESC/NULLS FIRST).
// Returns:
//   -1 if keyVal < filterVal
//    0 if keyVal == filterVal
//   +1 if keyVal > filterVal
//   ok=false if types are incompatible and comparison cannot be trusted.
func compareKeyValToFilterAsc(val HeapVal, f vtabFilter) (int, bool) {
	switch f.valType {
	case sqliteTypeInteger:
		if iv, ok := heapValToInt64(val); ok {
			if iv < f.intVal {
				return -1, true
			}
			if iv > f.intVal {
				return 1, true
			}
			return 0, true
		}
		if fv, ok := heapValToFloat64(val); ok {
			target := float64(f.intVal)
			if fv < target {
				return -1, true
			}
			if fv > target {
				return 1, true
			}
			return 0, true
		}
		return 0, false
	case sqliteTypeFloat:
		if fv, ok := heapValToFloat64(val); ok {
			if fv < f.floatVal {
				return -1, true
			}
			if fv > f.floatVal {
				return 1, true
			}
			return 0, true
		}
		return 0, false
	case sqliteTypeText:
		if val.Kind == ValString {
			if val.Str < f.strVal {
				return -1, true
			}
			if val.Str > f.strVal {
				return 1, true
			}
			return 0, true
		}
		if val.Kind == ValByte {
			s := string([]byte{byte(val.I64)})
			if s < f.strVal {
				return -1, true
			}
			if s > f.strVal {
				return 1, true
			}
			return 0, true
		}
		return 0, false
	default:
		return 0, false
	}
}

// btreeIndexPlan holds the resolved B-tree index and key column metadata for a table index.
type btreeIndexPlan struct {
	Index       DiscoveredIndex
	KeyAllCols  []int        // maps index key position -> index in table AllCols
	KeyVisCols  []int        // maps index key position -> index in table VisibleIdx
	KeyColTypes    []ColumnType // PostgreSQL ColumnType of each index key attribute
	AllIdxCols     []int        // key + INCLUDE covering columns in AllCols
	AllIdxColTypes []ColumnType // ColumnType of each key + INCLUDE column
	Orderings      []btreeKeyOrdering
}

// IsCoveringFor reports whether all needed/filter columns are covered by this index's key + INCLUDE attributes.
func (p btreeIndexPlan) IsCoveringFor(neededCols, filterCols []bool, totalCols int) bool {
	if len(p.AllIdxCols) == 0 {
		return false
	}
	for c := 0; c < totalCols; c++ {
		need := neededCols == nil || (c < len(neededCols) && neededCols[c])
		filt := c < len(filterCols) && filterCols[c]
		if !need && !filt {
			continue
		}
		covered := false
		for _, ic := range p.AllIdxCols {
			if ic == c {
				covered = true
				break
			}
		}
		if !covered {
			return false
		}
	}
	return true
}

// buildTableIndexPlans matches discovered B-tree indexes against table columns.
func buildTableIndexPlans(tbl DiscoveredTable, allCols []ColumnType, visibleIdx []int) []btreeIndexPlan {
	if len(tbl.Indexes) == 0 {
		return nil
	}
	attNumToAllIdx := make(map[int16]int, len(tbl.Columns))
	for i, col := range tbl.Columns {
		if !col.IsDropped {
			attNumToAllIdx[col.AttNum] = i
		}
	}
	allIdxToVisIdx := make(map[int]int, len(visibleIdx))
	for visI, allI := range visibleIdx {
		allIdxToVisIdx[allI] = visI
	}

	var plans []btreeIndexPlan
	for _, idx := range tbl.Indexes {
		nkeys := int(idx.IndNKeyAtts)
		if nkeys <= 0 || len(idx.KeyAttNums) < nkeys {
			continue
		}
		// Ensure all key columns exist and have supported B-tree comparison types.
		keyAll := make([]int, nkeys)
		keyVis := make([]int, nkeys)
		keyTypes := make([]ColumnType, nkeys)
		ords := make([]btreeKeyOrdering, nkeys)
		supported := true
		for k := 0; k < nkeys; k++ {
			allI, ok := attNumToAllIdx[idx.KeyAttNums[k]]
			if !ok {
				supported = false
				break
			}
			visI, ok := allIdxToVisIdx[allI]
			if !ok {
				supported = false
				break
			}
			ct := allCols[allI]
			switch ct {
			case ColInt2, ColInt4, ColInt8, ColOID, ColFloat4, ColFloat8, ColBool, ColChar, ColName, ColText, ColDate, ColTimestamp, ColUUID:
			default:
				supported = false
			}
			if !supported {
				break
			}
			keyAll[k] = allI
			keyVis[k] = visI
			keyTypes[k] = ct
			var opt int16
			if k < len(idx.KeyOptions) {
				opt = idx.KeyOptions[k]
			}
			ords[k] = parseIndOption(opt)
		}
		if !supported {
			continue
		}
		// Map all index attributes (keys + INCLUDE covering columns).
		natts := int(idx.IndNAtts)
		if natts < nkeys || natts > len(idx.KeyAttNums) {
			natts = nkeys
		}
		allIdxCols := make([]int, 0, natts)
		allIdxTypes := make([]ColumnType, 0, natts)
		for k := 0; k < natts; k++ {
			allI, ok := attNumToAllIdx[idx.KeyAttNums[k]]
			if !ok {
				allIdxCols = nil
				break
			}
			ct := allCols[allI]
			switch ct {
			case ColInt2, ColInt4, ColInt8, ColOID, ColFloat4, ColFloat8, ColBool, ColChar, ColName, ColText, ColDate, ColTimestamp, ColUUID:
				allIdxCols = append(allIdxCols, allI)
				allIdxTypes = append(allIdxTypes, ct)
			default:
				allIdxCols = nil
			}
			if allIdxCols == nil {
				break
			}
		}
		plans = append(plans, btreeIndexPlan{
			Index:          idx,
			KeyAllCols:     keyAll,
			KeyVisCols:     keyVis,
			KeyColTypes:    keyTypes,
			AllIdxCols:     allIdxCols,
			AllIdxColTypes: allIdxTypes,
			Orderings:      ords,
		})
	}
	return plans
}

func isRangeSafeBTreeColType(ct ColumnType) bool {
	switch ct {
	case ColInt2, ColInt4, ColInt8, ColOID, ColFloat4, ColFloat8, ColBool, ColDate, ColTimestamp:
		return true
	default:
		return false
	}
}

// selectBestBTreeIndex chooses the highest-scoring B-tree index for the given filters.
// Returns (planIndex, eqPrefixLen, hasRangeOnNextKey).
func selectBestBTreeIndex(plans []btreeIndexPlan, filters []vtabFilter) (int, int, bool) {
	if len(plans) == 0 || len(filters) == 0 {
		return -1, 0, false
	}
	bestIdx := -1
	bestScore := 0
	bestEqLen := 0
	bestHasRange := false

	for pi, p := range plans {
		eqLen := 0
		hasRange := false
		for k := 0; k < len(p.KeyAllCols); k++ {
			colIdx := p.KeyAllCols[k]
			ct := p.KeyColTypes[k]
			foundEq := false
			foundRange := false
			for _, f := range filters {
				if f.allColIdx != colIdx {
					continue
				}
				switch f.op {
				case sqliteConstraintEQ, sqliteConstraintISNULL:
					foundEq = true
				case sqliteConstraintGT, sqliteConstraintGE, sqliteConstraintLT, sqliteConstraintLE:
					if isRangeSafeBTreeColType(ct) {
						foundRange = true
					}
				}
			}
			if foundEq {
				eqLen++
				continue
			}
			if foundRange {
				hasRange = true
			}
			break
		}
		if eqLen == 0 && !hasRange {
			continue
		}
		score := eqLen * 100
		if hasRange {
			score += 30
		}
		if p.Index.IsPrimary && eqLen == len(p.KeyAllCols) {
			score += 50
		} else if p.Index.IsUnique && eqLen == len(p.KeyAllCols) {
			score += 40
		}
		if score > bestScore {
			bestScore = score
			bestIdx = pi
			bestEqLen = eqLen
			bestHasRange = hasRange
		}
	}
	return bestIdx, bestEqLen, bestHasRange
}

// comparePivotToLowerBound compares an index tuple (with nDecoded decoded key attributes)
// against the scan's lower bound in the index's physical B-tree sort order.
// Returns:
//   -1 if tuple < lowerBound (i.e. tuple is strictly to the left of the scan start)
//    0 if tuple overlaps/matches lowerBound prefix
//   +1 if tuple > lowerBound (i.e. tuple is at or to the right of the scan start)
func compareIndexTupleToStart(keyVals []HeapVal, nDecoded int, plan btreeIndexPlan, keyFilters [][]vtabFilter) int {
	for k := 0; k < nDecoded && k < len(plan.KeyAllCols); k++ {
		fList := keyFilters[k]
		if len(fList) == 0 {
			// No lower bound on key k -> tuple matches prefix
			return 0
		}
		ord := plan.Orderings[k]
		val := keyVals[k]

		for _, f := range fList {
			if f.op == sqliteConstraintISNULL {
				if val.Kind == ValNull {
					continue
				}
				if ord.NullsFirst {
					// NULLs are first; non-null val is after NULLs
					return 1
				}
				return -1
			}
			// Non-null filter bound (=, >, >=, <, <=)
			if val.Kind == ValNull {
				if ord.NullsFirst {
					return -1
				}
				return 1
			}
			// Check if this filter acts as a lower bound in index sort order:
			// - For ASC: EQ, GT, GE are lower bounds
			// - For DESC: EQ, LT, LE are lower bounds
			isLower := false
			if f.op == sqliteConstraintEQ {
				isLower = true
			} else if !ord.Desc && (f.op == sqliteConstraintGT || f.op == sqliteConstraintGE) {
				isLower = true
			} else if ord.Desc && (f.op == sqliteConstraintLT || f.op == sqliteConstraintLE) {
				isLower = true
			}
			if !isLower {
				continue
			}
			cmpAsc, ok := compareKeyValToFilterAsc(val, f)
			if !ok {
				return 0
			}
			cmpIdx := cmpAsc
			if ord.Desc {
				cmpIdx = -cmpAsc
			}
			if cmpIdx < 0 {
				return -1
			}
			if cmpIdx > 0 {
				return 1
			}
			// cmpIdx == 0: if strict inequality (GT on ASC, LT on DESC), then only if this is the last key attribute
			// could it be strictly before; for prefix keys, equality may still contain matching suffixes.
		}
	}
	return 0
}

// indexTupleExceedsEnd checks whether a leaf index tuple has passed beyond the scan's upper bound
// in the index's physical B-tree sort order, allowing early termination of the leaf page scan.
func indexTupleExceedsEnd(keyVals []HeapVal, nDecoded int, plan btreeIndexPlan, keyFilters [][]vtabFilter) bool {
	for k := 0; k < nDecoded && k < len(plan.KeyAllCols); k++ {
		fList := keyFilters[k]
		if len(fList) == 0 {
			return false
		}
		ord := plan.Orderings[k]
		val := keyVals[k]

		hasEq := false
		for _, f := range fList {
			if f.op == sqliteConstraintISNULL {
				hasEq = true
				if val.Kind == ValNull {
					continue
				}
				// If NULLs are first, any non-NULL value has passed all NULLs!
				return ord.NullsFirst
			}
			if val.Kind == ValNull {
				// Non-null filter: if NULLs are last (!ord.NullsFirst), reaching a NULL means we passed all non-NULL values!
				return !ord.NullsFirst
			}
			isUpper := false
			strict := false
			if f.op == sqliteConstraintEQ {
				isUpper = true
				hasEq = true
			} else if !ord.Desc && (f.op == sqliteConstraintLT || f.op == sqliteConstraintLE) {
				isUpper = true
				strict = f.op == sqliteConstraintLT
			} else if ord.Desc && (f.op == sqliteConstraintGT || f.op == sqliteConstraintGE) {
				isUpper = true
				strict = f.op == sqliteConstraintGT
			}
			if !isUpper {
				continue
			}
			cmpAsc, ok := compareKeyValToFilterAsc(val, f)
			if !ok {
				return false
			}
			cmpIdx := cmpAsc
			if ord.Desc {
				cmpIdx = -cmpAsc
			}
			if cmpIdx > 0 || (strict && cmpIdx == 0) {
				return true
			}
			if cmpIdx < 0 && f.op == sqliteConstraintEQ {
				// Still before equality target on key k, so cannot exceed end on later keys
				return false
			}
		}
		if !hasEq {
			// Key k only had a range condition, so subsequent keys (k+1..) are not monotonically ordered globally.
			return false
		}
	}
	return false
}

// indexTupleMatchesAll checks whether a leaf index tuple satisfies all filters on the indexed columns.
func indexTupleMatchesAll(keyVals []HeapVal, nDecoded int, plan btreeIndexPlan, keyFilters [][]vtabFilter) bool {
	for k := 0; k < nDecoded && k < len(plan.KeyAllCols); k++ {
		for _, f := range keyFilters[k] {
			if !evalFilterVal(f, keyVals[k]) {
				return false
			}
		}
	}
	return true
}

// scanBTreeIndexForTIDs traverses a PostgreSQL B-tree index from metapage -> root -> internal pages -> leaf pages
// and returns the candidate heap ItemPointers (TIDs) matching the query filters, in index scan order.
func scanBTreeIndexForTIDs(
	ctx context.Context,
	adapter *Adapter,
	snap *ReadSnapshot,
	plan btreeIndexPlan,
	filters []vtabFilter,
) ([]ItemPointer, error) {
	idxRel, err := adapter.OpenRelation(ctx, RelationSpec{
		TenantID:   snap.TenantID,
		Generation: snap.Generation,
		SpcOID:     plan.Index.SpcOID,
		DbOID:      plan.Index.DbOID,
		RelNumber:  plan.Index.RelNumber,
		ForkNum:    0,
	})
	if err != nil {
		return nil, err
	}
	defer func() { _ = adapter.CloseRelation(idxRel) }()

	vtabIndexScans.Add(1)

	// Group filters by index key position.
	nkeys := len(plan.KeyAllCols)
	keyFilters := make([][]vtabFilter, nkeys)
	for _, f := range filters {
		for k, colIdx := range plan.KeyAllCols {
			if f.allColIdx == colIdx {
				keyFilters[k] = append(keyFilters[k], f)
			}
		}
	}

	var idxPageBuf [PGPageSize]byte
	readIdxPage := func(blk uint32) ([]byte, PageHeader, BTPageOpaque, error) {
		if rerr := adapter.ReadPageInto(ctx, idxRel, snap, blk, idxPageBuf[:]); rerr != nil {
			return nil, PageHeader{}, BTPageOpaque{}, rerr
		}
		data := idxPageBuf[:]
		vtabIndexPagesRead.Add(1)
		vtabPagesRead.Add(1)
		hdr, herr := ParsePageHeader(data)
		if herr != nil {
			return nil, PageHeader{}, BTPageOpaque{}, herr
		}
		opq, oerr := ParseBTPageOpaque(data, hdr)
		if oerr != nil {
			return nil, PageHeader{}, BTPageOpaque{}, oerr
		}
		return data, hdr, opq, nil
	}

	// 1. Read B-tree metapage (block 0).
	metaBytes, metaHdr, metaOpq, err := readIdxPage(0)
	if err != nil {
		return nil, err
	}
	if (metaOpq.Flags & BTPMeta) == 0 {
		return nil, fmt.Errorf("%w: block 0 of index %s is not a B-tree metapage", ErrInvalidPage, plan.Index.Name)
	}
	meta, err := ParseBTMetaPage(metaBytes, metaHdr)
	if err != nil {
		return nil, err
	}
	curBlk := meta.FastRoot
	if curBlk == BTPNone {
		curBlk = meta.Root
	}
	if curBlk == BTPNone {
		// Empty B-tree index (no root page yet).
		return nil, nil
	}

	keyBuf := make([]HeapVal, nkeys)
	var lpBuf []LinePointer

	// 2. Descend internal pages from root to target starting leaf page.
	for depth := 0; depth < maxBTreeDepth; depth++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		page, hdr, opq, err := readIdxPage(curBlk)
		if err != nil {
			return nil, err
		}
		if (opq.Flags & (BTPDeleted | BTPHalfDead)) != 0 {
			if opq.Next != BTPNone {
				curBlk = opq.Next
				continue
			}
			return nil, nil
		}

		lpBuf = ParseLinePointersInto(page, hdr, lpBuf)
		firstDataIdx := 0
		if opq.Next != BTPNone {
			// Page has a High Key at Item 1 (lpBuf[0]).
			firstDataIdx = 1
		}

		// Handle concurrent page split: if search lower bound > page's High Key, move right!
		if opq.Next != BTPNone && len(lpBuf) > 0 && lpBuf[0].Flags == LPNormal {
			hkLP := lpBuf[0]
			if int(hkLP.Offset)+int(hkLP.Length) <= int(hdr.Special) {
				hkBytes := page[hkLP.Offset : hkLP.Offset+hkLP.Length]
				if hkHdr, herr := ParseIndexTupleHeader(hkBytes, false, nkeys); herr == nil {
					if nd, derr := DecodeIndexTupleColumnsInto(hkBytes, hkHdr, plan.KeyColTypes, keyBuf); derr == nil {
						if compareIndexTupleToStart(keyValsSlice(keyBuf, nd), nd, plan, keyFilters) < 0 {
							curBlk = opq.Next
							continue
						}
					}
				}
			}
		}

		if (opq.Flags & BTPLeaf) != 0 {
			// Reached starting leaf page!
			var tids []ItemPointer
			pagesScanned := 0
			for {
				pagesScanned++
				if pagesScanned > maxBTreePagesRead {
					return nil, fmt.Errorf("%w: btree leaf scan exceeded max page count", ErrInvalidPage)
				}
				exceeded := false
				for i := firstDataIdx; i < len(lpBuf); i++ {
					lp := lpBuf[i]
					if lp.Flags != LPNormal {
						// Skip LP_DEAD / LP_UNUSED items
						continue
					}
					if int(lp.Offset) < int(hdr.Upper) || int(lp.Offset)+int(lp.Length) > int(hdr.Special) {
						return nil, fmt.Errorf("%w: btree leaf lp out of bounds", ErrInvalidPage)
					}
					itup := page[lp.Offset : lp.Offset+lp.Length]
					ihdr, err := ParseIndexTupleHeader(itup, true, nkeys)
					if err != nil {
						return nil, err
					}
					nd, err := DecodeIndexTupleColumnsInto(itup, ihdr, plan.KeyColTypes, keyBuf)
					if err != nil {
						return nil, err
					}
					if indexTupleExceedsEnd(keyBuf, nd, plan, keyFilters) {
						exceeded = true
						break
					}
					if indexTupleMatchesAll(keyBuf, nd, plan, keyFilters) {
						tids = DecodeIndexTuplePostingTIDs(itup, ihdr, tids)
					}
				}
				if exceeded || opq.Next == BTPNone {
					break
				}
				// Before reading the next leaf page, check if this page's High Key already exceeds our upper bound!
				if len(lpBuf) > 0 && lpBuf[0].Flags == LPNormal {
					hkLP := lpBuf[0]
					if int(hkLP.Offset)+int(hkLP.Length) <= int(hdr.Special) {
						hkBytes := page[hkLP.Offset : hkLP.Offset+hkLP.Length]
						if hkHdr, herr := ParseIndexTupleHeader(hkBytes, false, nkeys); herr == nil {
							if nd, derr := DecodeIndexTupleColumnsInto(hkBytes, hkHdr, plan.KeyColTypes, keyBuf); derr == nil {
								if indexTupleExceedsEnd(keyBuf, nd, plan, keyFilters) {
									break
								}
							}
						}
					}
				}
				curBlk = opq.Next
				page, hdr, opq, err = readIdxPage(curBlk)
				if err != nil {
					return nil, err
				}
				lpBuf = ParseLinePointersInto(page, hdr, lpBuf)
				firstDataIdx = 0
				if opq.Next != BTPNone {
					firstDataIdx = 1
				}
			}
			return tids, nil
		}

		// Internal page: find last child pointer whose separator key <= lower bound.
		// Item at firstDataIdx is the "-infinity" key pointing to the leftmost child.
		if firstDataIdx >= len(lpBuf) {
			return nil, nil
		}
		chosenChild := BTPNone
		for i := firstDataIdx; i < len(lpBuf); i++ {
			lp := lpBuf[i]
			if lp.Flags != LPNormal {
				continue
			}
			if int(lp.Offset) < int(hdr.Upper) || int(lp.Offset)+int(lp.Length) > int(hdr.Special) {
				return nil, fmt.Errorf("%w: btree internal lp out of bounds", ErrInvalidPage)
			}
			itup := page[lp.Offset : lp.Offset+lp.Length]
			ihdr, err := ParseIndexTupleHeader(itup, false, nkeys)
			if err != nil {
				return nil, err
			}
			if i == firstDataIdx {
				// Minus-infinity pivot tuple
				chosenChild = ihdr.TID.BlockNum
				continue
			}
			nd, err := DecodeIndexTupleColumnsInto(itup, ihdr, plan.KeyColTypes, keyBuf)
			if err != nil {
				return nil, err
			}
			cmp := compareIndexTupleToStart(keyBuf, nd, plan, keyFilters)
			if cmp < 0 {
				// Pivot key < lower bound -> target could be in this child or a later child
				chosenChild = ihdr.TID.BlockNum
			} else {
				// Pivot key >= lower bound: because B-tree may have duplicate keys spanning the previous child
				// and this child, we MUST descend into chosenChild (the child before the first pivot >= target)
				// and walk right if needed!
				break
			}
		}
		if chosenChild == BTPNone {
			return nil, nil
		}
		curBlk = chosenChild
	}

	return nil, fmt.Errorf("%w: btree depth exceeded %d", ErrInvalidPage, maxBTreeDepth)
}

func keyValsSlice(s []HeapVal, n int) []HeapVal {
	if n > len(s) {
		return s
	}
	return s[:n]
}

// fetchVisibleTuplesByTIDs fetches and validates heap tuples referenced by B-tree index TIDs,
// following HOT chains on each heap page and enforcing MVCC visibility and full filter re-checks.
func (cur *pgVTabCursor) fetchVisibleTuplesByTIDs(ctx context.Context, tids []ItemPointer) error {
	cur.rows = cur.rows[:0]
	cur.numRows = 0
	cur.pageRowIdx = 0
	if len(tids) == 0 {
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

	if len(tids) > 1 {
		sort.Slice(tids, func(i, j int) bool {
			if tids[i].BlockNum != tids[j].BlockNum {
				return tids[i].BlockNum < tids[j].BlockNum
			}
			return tids[i].OffNum < tids[j].OffNum
		})
	}

	// 8 KiB page can hold at most (8192-24)/4 = 2042 line pointers -> 32 uint64 words = 2048 bits on stack (zero heap allocs).
	var visitedBits [32]uint64

	var heapPageBuf [PGPageSize]byte
	i := 0
	for i < len(tids) {
		blkNum := tids[i].BlockNum
		j := i + 1
		for j < len(tids) && tids[j].BlockNum == blkNum {
			j++
		}
		group := tids[i:j]
		i = j

		if err := ctx.Err(); err != nil {
			return err
		}
		if blkNum >= cur.nblocks {
			// Stale index entry pointing beyond current relation size at snapshot -> skip safely
			continue
		}
		if err := cur.spec.Adapter.ReadPageInto(ctx, cur.rel, cur.snap, blkNum, heapPageBuf[:]); err != nil {
			return err
		}
		pageData := heapPageBuf[:]
		vtabPagesRead.Add(1)
		if IsZeroPage(pageData) {
			continue
		}
		phdr, err := ParsePageHeader(pageData)
		if err != nil {
			return err
		}
		cur.lpBuf = ParseLinePointersInto(pageData, phdr, cur.lpBuf)
		visitedBits = [32]uint64{}

		for _, tid := range group {
			curOff := tid.OffNum
			if curOff == 0 {
				continue
			}
			for step := 0; step < maxHOTChainLen; step++ {
				if curOff < 1 || int(curOff) > len(cur.lpBuf) || curOff >= 2048 {
					break
				}
				wordIdx := curOff >> 6
				bitMask := uint64(1) << (curOff & 63)
				if (visitedBits[wordIdx] & bitMask) != 0 {
					break
				}
				visitedBits[wordIdx] |= bitMask

				lp := cur.lpBuf[curOff-1]
				if lp.Flags == LPRedirect {
					// Follow HOT redirect line pointer (lp.Offset holds target OffsetNumber).
					curOff = lp.Offset
					continue
				}
				if lp.Flags != LPNormal {
					break
				}
				if err := ValidateLinePointer(lp, phdr); err != nil {
					return err
				}
				start := int(lp.Offset)
				end := start + int(lp.Length)
				tup := pageData[start:end]
				thdr, err := ParseHeapTupleHeader(tup)
				if err != nil {
					return err
				}
				vis, err := IsVisibleWithCLOG(ctx, thdr, cur.snap, cur.spec.CLOG)
				if err != nil {
					return err
				}
				if vis {
					if err := cur.decodeHeapTupleColumns(ctx, tup, thdr, cur.rowBuf); err != nil {
						return err
					}
					vtabTuplesDecoded.Add(1)
					vtabColsDecoded.Add(decodedColsPerTuple)
					// Always re-evaluate ALL filter predicates on the heap tuple (never trust stale index entry!).
					if cur.matchesFilters(cur.rowBuf) {
						for k := range cur.rowBuf {
							cur.rowBuf[k].Materialize()
						}
						vtabRowsMatched.Add(1)
						cur.rows = append(cur.rows, cur.rowBuf...)
						cur.numRows++
					}
				}
				// If this tuple was HOT-updated on the same page, follow t_ctid to the next version in the HOT chain!
				if (thdr.Infomask2 & HeapHOTUpdate) != 0 {
					ctid := ParseHeapTupleCTID(tup)
					if ctid.BlockNum == blkNum && ctid.OffNum != curOff && ctid.OffNum >= 1 {
						curOff = ctid.OffNum
						continue
					}
				}
				break
			}
		}
	}

	if cur.numRows == 0 {
		cur.eof = true
	}
	return nil
}
