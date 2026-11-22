package readadapter

import (
	"context"
	"fmt"
	"sort"
)

// PostgreSQL 17 well-known catalog OIDs and tablespace OIDs.
const (
	PGDefaultTablespaceOID uint32 = 1663
	PGGlobalTablespaceOID  uint32 = 1664

	PGDatabaseRelID  uint32 = 1262
	PGClassRelID     uint32 = 1259
	PGAttributeRelID uint32 = 1249
	PGIndexRelID     uint32 = 2610
	PGNamespaceRelID uint32 = 2615
	PGStatisticRelID uint32 = 2619

	PGBtreeAMOID uint32 = 403
)

// DiscoveredColumn describes one user column of a PostgreSQL table discovered
// directly from PG17 pg_attribute pages.
type DiscoveredColumn struct {
	Name               string
	AttNum             int16
	PGTypeOID          uint32
	ColType            ColumnType
	SQLiteType         string
	IsDropped          bool
	HasMissing         bool
	MissingVal         HeapVal
	UnsupportedMissing bool
}

// DiscoveredIndex describes a PostgreSQL B-tree index discovered from pg_class and pg_index.
type DiscoveredIndex struct {
	Name        string
	IndexRelOID uint32
	TableRelOID uint32
	SpcOID      uint32
	DbOID       uint32
	RelNumber   uint32
	RelPages    int32
	RelTuples   float32
	IndNAtts    int16
	IndNKeyAtts int16
	IsUnique    bool
	IsPrimary   bool
	KeyAttNums  []int16
	KeyOptions  []int16
}

// DiscoveredTable describes a PostgreSQL table discovered from PG17 catalog pages.
type DiscoveredTable struct {
	Schema         string
	Name           string
	RelOID         uint32
	SpcOID         uint32
	DbOID          uint32
	RelNumber      uint32
	RelPages       int32
	RelTuples      float32
	RelAllVisible  int32
	ToastRelOID    uint32
	ToastRelNumber uint32
	ToastIndexRel  uint32
	Columns        []DiscoveredColumn
	Indexes        []DiscoveredIndex
	ColumnStats    map[int16]ColumnStats
}

var (
	pgIndexCols = []ColumnType{
		ColOID,        // 1: indexrelid
		ColOID,        // 2: indrelid
		ColInt2,       // 3: indnatts
		ColInt2,       // 4: indnkeyatts
		ColBool,       // 5: indisunique
		ColBool,       // 6: indnullsnotdistinct
		ColBool,       // 7: indisprimary
		ColBool,       // 8: indisexclusion
		ColBool,       // 9: indimmediate
		ColBool,       // 10: indisclustered
		ColBool,       // 11: indisvalid
		ColBool,       // 12: indcheckxmin
		ColBool,       // 13: indisready
		ColBool,       // 14: indislive
		ColBool,       // 15: indisreplident
		ColInt2Vector, // 16: indkey
		ColBytea,      // 17: indcollation (oidvector)
		ColBytea,      // 18: indclass (oidvector)
		ColInt2Vector, // 19: indoption
		ColText,       // 20: indexprs (pg_node_tree)
		ColText,       // 21: indpred (pg_node_tree)
	}

	pgDatabaseCols = []ColumnType{
		ColOID,  // 1: oid
		ColName, // 2: datname
		ColOID,  // 3: datdba
		ColInt4, // 4: encoding
		ColChar, // 5: datlocprovider
		ColBool, // 6: datistemplate
		ColBool, // 7: datallowconn
		ColBool, // 8: dathasloginevt (PG17+)
		ColInt4, // 9: datconnlimit
		ColOID,  // 10: datfrozenxid
		ColOID,  // 11: datminmxid
		ColOID,  // 12: dattablespace
	}

	pgDatabaseColsPG15 = []ColumnType{
		ColOID,  // 1: oid
		ColName, // 2: datname
		ColOID,  // 3: datdba
		ColInt4, // 4: encoding
		ColChar, // 5: datlocprovider
		ColBool, // 6: datistemplate
		ColBool, // 7: datallowconn
		ColInt4, // 8: datconnlimit (PG15: no dathasloginevt)
		ColOID,  // 9: datfrozenxid
		ColOID,  // 10: datminmxid
		ColOID,  // 11: dattablespace
	}

	pgNamespaceCols = []ColumnType{
		ColOID,  // 1: oid
		ColName, // 2: nspname
	}

	pgStatisticCols = []ColumnType{
		ColOID,    // 1: starelid
		ColInt2,   // 2: staattnum
		ColBool,   // 3: stainherit
		ColFloat4, // 4: stanullfrac
		ColInt4,   // 5: stawidth
		ColFloat4, // 6: stadistinct
	}

	pgClassCols = []ColumnType{
		ColOID,    // 1: oid
		ColName,   // 2: relname
		ColOID,    // 3: relnamespace
		ColOID,    // 4: reltype
		ColOID,    // 5: reloftype
		ColOID,    // 6: relowner
		ColOID,    // 7: relam
		ColOID,    // 8: relfilenode
		ColOID,    // 9: reltablespace
		ColInt4,   // 10: relpages
		ColFloat4, // 11: reltuples
		ColInt4,   // 12: relallvisible
		ColOID,    // 13: reltoastrelid
		ColBool,   // 14: relhasindex
		ColBool,   // 15: relisshared
		ColChar,   // 16: relpersistence
		ColChar,   // 17: relkind
	}

	pgAttributeCols = []ColumnType{
		ColOID,   // 1: attrelid
		ColName,  // 2: attname
		ColOID,   // 3: atttypid
		ColInt2,  // 4: attlen
		ColInt2,  // 5: attnum
		ColInt4,  // 6: attcacheoff
		ColInt4,  // 7: atttypmod
		ColInt2,  // 8: attndims
		ColBool,  // 9: attbyval
		ColChar,  // 10: attalign
		ColChar,  // 11: attstorage
		ColChar,  // 12: attcompression
		ColBool,  // 13: attnotnull
		ColBool,  // 14: atthasdef
		ColBool,  // 15: atthasmissing
		ColChar,  // 16: attidentity
		ColChar,  // 17: attgenerated
		ColBool,  // 18: attisdropped
		ColBool,  // 19: attislocal
		ColInt2,  // 20: attinhcount
		ColOID,   // 21: attcollation
		ColInt2,  // 22: attstattarget
		ColBytea, // 23: attacl
		ColBytea, // 24: attoptions
		ColBytea, // 25: attfdwoptions
		ColBytea, // 26: attmissingval (anyarray)
	}

	pgAttributeColsPG15 = []ColumnType{
		ColOID,   // 1: attrelid
		ColName,  // 2: attname
		ColOID,   // 3: atttypid
		ColInt4,  // 4: attstattarget (PG15)
		ColInt2,  // 5: attlen
		ColInt2,  // 6: attnum
		ColInt4,  // 7: attndims (int4 in PG15)
		ColInt4,  // 8: attcacheoff
		ColInt4,  // 9: atttypmod
		ColBool,  // 10: attbyval
		ColChar,  // 11: attalign
		ColChar,  // 12: attstorage
		ColChar,  // 13: attcompression
		ColBool,  // 14: attnotnull
		ColBool,  // 15: atthasdef
		ColBool,  // 16: atthasmissing
		ColChar,  // 17: attidentity
		ColChar,  // 18: attgenerated
		ColBool,  // 19: attisdropped
		ColBool,  // 20: attislocal
		ColInt2,  // 21: attinhcount
		ColOID,   // 22: attcollation
		ColBytea, // 23: attacl
		ColBytea, // 24: attoptions
		ColBytea, // 25: attfdwoptions
		ColBytea, // 26: attmissingval (anyarray)
	}
)

var pgAttributeNeeded = func() []bool {
	n := make([]bool, 26)
	for i := range n {
		n[i] = true
	}
	// Skip attacl (22), attoptions (23), attfdwoptions (24) to avoid unnecessary allocations/TOAST.
	n[22] = false
	n[23] = false
	n[24] = false
	return n
}()

// scanCatalogRelation reads all visible tuples of a PG17 catalog relation at snap.
func scanCatalogRelation(
	ctx context.Context,
	adapter *Adapter,
	clog *CLOGResolver,
	snap *ReadSnapshot,
	spcOID, dbOID, relNumber uint32,
	cols []ColumnType,
) ([][]any, error) {
	rel, err := adapter.OpenRelation(ctx, RelationSpec{
		TenantID:   snap.TenantID,
		Generation: snap.Generation,
		SpcOID:     spcOID,
		DbOID:      dbOID,
		RelNumber:  relNumber,
		ForkNum:    0,
	})
	if err != nil {
		return nil, err
	}
	defer func() { _ = adapter.CloseRelation(rel) }()

	meta, err := adapter.GetRelationMetadata(ctx, rel, snap)
	if err != nil {
		return nil, err
	}
	if !meta.Exists || meta.NBlocks == 0 {
		return nil, nil
	}

	var rows [][]any
	const batchSize uint32 = 32
	for blk := uint32(0); blk < meta.NBlocks; blk += batchSize {
		n := batchSize
		if blk+n > meta.NBlocks {
			n = meta.NBlocks - blk
		}
		pages, err := adapter.ReadPages(ctx, rel, snap, blk, n)
		if err != nil {
			return nil, err
		}
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
				vis, err := IsVisibleWithCLOG(ctx, thdr, snap, clog)
				if err != nil {
					return nil, err
				}
				if !vis {
					continue
				}
				decodeCols := cols
				if relNumber == PGDatabaseRelID && (thdr.Infomask2&HeapNattsMask) == 16 {
					decodeCols = pgDatabaseColsPG15
				}
				var vals []any
				if relNumber == PGAttributeRelID && len(decodeCols) == 26 {
					vals, err = DecodeHeapColumnsProjected(tup, thdr, decodeCols, pgAttributeNeeded)
					if err != nil {
						// If attmissingval is TOASTed, fall back to decoding fixed prefix so we still
						// discover the column and mark UnsupportedMissing = true.
						fixedLen := 18
						if len(decodeCols) == len(pgAttributeColsPG15) && decodeCols[3] == ColInt4 {
							fixedLen = 19
						}
						vals, err = DecodeHeapColumns(tup, thdr, decodeCols[:fixedLen])
					}
				} else {
					vals, err = DecodeHeapColumns(tup, thdr, decodeCols)
				}
				if err != nil {
					continue
				}
				rows = append(rows, vals)
			}
		}
	}
	return rows, nil
}

// DiscoverCatalog reads PG17 system catalogs directly from Pageserver pages
// (pg_database -> pg_namespace -> pg_class -> pg_attribute) at snap and returns
// all ordinary tables in the "public" schema for dbName (default "postgres").
func DiscoverCatalog(
	ctx context.Context,
	adapter *Adapter,
	clog *CLOGResolver,
	snap *ReadSnapshot,
	dbName string,
) ([]DiscoveredTable, error) {
	if dbName == "" {
		dbName = "postgres"
	}

	// 1. Scan pg_database (1664/0/1262) to locate dbOID and default tablespace.
	// Try dbName first, then fall back to "postgres".
	dbRows, err := scanCatalogRelation(ctx, adapter, clog, snap, PGGlobalTablespaceOID, 0, PGDatabaseRelID, pgDatabaseCols)
	if err != nil {
		return nil, fmt.Errorf("readadapter: scan pg_database: %w", err)
	}
	var dbOID uint32
	var dbSpcOID uint32 = PGDefaultTablespaceOID
	var fallbackOID uint32
	var fallbackSpc uint32 = PGDefaultTablespaceOID
	isPG15 := false
	for _, r := range dbRows {
		name, _ := r[1].(string)
		oid, _ := r[0].(uint32)
		spc := PGDefaultTablespaceOID
		spcIdx := 11
		if len(r) == len(pgDatabaseColsPG15) {
			spcIdx = 10
			isPG15 = true
		}
		if spcIdx < len(r) {
			if s, ok := r[spcIdx].(uint32); ok && s != 0 {
				spc = s
			}
		}
		if name == dbName {
			dbOID = oid
			dbSpcOID = spc
			break
		}
		if name == "postgres" {
			fallbackOID = oid
			fallbackSpc = spc
		}
	}
	if dbOID == 0 {
		dbOID = fallbackOID
		dbSpcOID = fallbackSpc
	}
	if dbOID == 0 {
		return nil, fmt.Errorf("readadapter: database %q not found in pg_database", dbName)
	}

	// 2. Scan pg_namespace (dbSpcOID/dbOID/2615) to locate "public" schema OID.
	nsRows, err := scanCatalogRelation(ctx, adapter, clog, snap, dbSpcOID, dbOID, PGNamespaceRelID, pgNamespaceCols)
	if err != nil {
		return nil, fmt.Errorf("readadapter: scan pg_namespace: %w", err)
	}
	var publicNspOID uint32
	for _, r := range nsRows {
		name, _ := r[1].(string)
		if name == "public" {
			publicNspOID, _ = r[0].(uint32)
			break
		}
	}
	if publicNspOID == 0 {
		return nil, nil
	}

	// 3. Scan pg_class (dbSpcOID/dbOID/1259) for ordinary tables (relkind='r'),
	// B-tree indexes (relkind='i', relam=403), and TOAST tables (relkind='t').
	classRows, err := scanCatalogRelation(ctx, adapter, clog, snap, dbSpcOID, dbOID, PGClassRelID, pgClassCols)
	if err != nil {
		return nil, fmt.Errorf("readadapter: scan pg_class: %w", err)
	}
	type classMeta struct {
		oid        uint32
		name       string
		nspOID     uint32
		relAM      uint32
		relNode    uint32
		relSpc     uint32
		relPages   int32
		relTuples  float32
		relAllVis  int32
		toastOID   uint32
		hasIndex   bool
		relKind    byte
	}
	classesByOID := make(map[uint32]classMeta, len(classRows))
	tablesByOID := make(map[uint32]*DiscoveredTable)
	for _, r := range classRows {
		relOID, _ := r[0].(uint32)
		relName, _ := r[1].(string)
		nspOID, _ := r[2].(uint32)
		relAM, _ := r[6].(uint32)
		relFileNode, _ := r[7].(uint32)
		relSpc, _ := r[8].(uint32)
		relPages, _ := r[9].(int32)
		relTuples, _ := r[10].(float32)
		relAllVis, _ := r[11].(int32)
		toastOID, _ := r[12].(uint32)
		hasIndex, _ := r[13].(bool)
		relkind, _ := r[16].(byte)
		if relFileNode == 0 {
			relFileNode = relOID
		}
		if relSpc == 0 {
			relSpc = dbSpcOID
		}
		classesByOID[relOID] = classMeta{
			oid:       relOID,
			name:      relName,
			nspOID:    nspOID,
			relAM:     relAM,
			relNode:   relFileNode,
			relSpc:    relSpc,
			relPages:  relPages,
			relTuples: relTuples,
			relAllVis: relAllVis,
			toastOID:  toastOID,
			hasIndex:  hasIndex,
			relKind:   relkind,
		}
		if nspOID != publicNspOID || relkind != 'r' {
			continue
		}
		tablesByOID[relOID] = &DiscoveredTable{
			Schema:        "public",
			Name:          relName,
			RelOID:        relOID,
			SpcOID:        relSpc,
			DbOID:         dbOID,
			RelNumber:     relFileNode,
			RelPages:      relPages,
			RelTuples:     relTuples,
			RelAllVisible: relAllVis,
			ToastRelOID:   toastOID,
		}
	}
	if len(tablesByOID) == 0 {
		return nil, nil
	}

	// Resolve ToastRelNumber from classesByOID if present.
	for _, tbl := range tablesByOID {
		if tbl.ToastRelOID != 0 {
			if tm, ok := classesByOID[tbl.ToastRelOID]; ok {
				tbl.ToastRelNumber = tm.relNode
			}
		}
	}

	// 4. Scan pg_attribute (dbSpcOID/dbOID/1249) for user columns (attnum > 0).
	attCols := pgAttributeCols
	if isPG15 {
		attCols = pgAttributeColsPG15
	}
	attRows, err := scanCatalogRelation(ctx, adapter, clog, snap, dbSpcOID, dbOID, PGAttributeRelID, attCols)
	if err != nil {
		return nil, fmt.Errorf("readadapter: scan pg_attribute: %w", err)
	}
	for _, r := range attRows {
		attRelID, _ := r[0].(uint32)
		tbl, ok := tablesByOID[attRelID]
		if !ok {
			continue
		}
		attNumIdx := 4
		attLenIdx := 3
		hasMissingIdx := 14
		droppedIdx := 17
		if isPG15 {
			attLenIdx = 4
			attNumIdx = 5
			hasMissingIdx = 15
			droppedIdx = 18
		}
		attNum, _ := r[attNumIdx].(int16)
		if attNum <= 0 {
			continue
		}
		attName, _ := r[1].(string)
		attTypID, _ := r[2].(uint32)
		attLen, _ := r[attLenIdx].(int16)
		hasMissing, _ := r[hasMissingIdx].(bool)
		isDropped, _ := r[droppedIdx].(bool)
		colType, sqliteType := PGTypeOIDToColumnType(attTypID)
		if isDropped && colType == ColUnsupported {
			switch attLen {
			case 1:
				colType = ColBool
			case 2:
				colType = ColInt2
			case 4:
				colType = ColInt4
			case 8:
				colType = ColInt8
			case 16:
				colType = ColUUID
			case 64:
				colType = ColName
			case -1:
				colType = ColBytea
			}
		}
		var missingVal HeapVal
		var unsupportedMissing bool
		if hasMissing && !isDropped {
			if len(r) > 25 {
				if rawArr, ok := r[25].([]byte); ok && len(rawArr) > 0 {
					mv, decOK := DecodeMissingValArray(colType, rawArr)
					if decOK {
						missingVal = mv
					} else {
						unsupportedMissing = true
					}
				} else if r[25] == nil {
					missingVal = HeapVal{Kind: ValNull}
				} else {
					unsupportedMissing = true
				}
			} else {
				// Fallback prefix decode was used (e.g., TOASTed attmissingval).
				unsupportedMissing = true
			}
		}
		tbl.Columns = append(tbl.Columns, DiscoveredColumn{
			Name:               attName,
			AttNum:             attNum,
			PGTypeOID:          attTypID,
			ColType:            colType,
			SQLiteType:         sqliteType,
			IsDropped:          isDropped,
			HasMissing:         hasMissing,
			MissingVal:         missingVal,
			UnsupportedMissing: unsupportedMissing,
		})
	}

	// 5. Scan pg_index (dbSpcOID/dbOID/2610) to discover valid, live, non-partial, non-expression B-tree indexes.
	if idxRows, err := scanCatalogRelation(ctx, adapter, clog, snap, dbSpcOID, dbOID, PGIndexRelID, pgIndexCols); err == nil {
		for _, r := range idxRows {
			if len(r) < 19 {
				continue
			}
			indexRelID, _ := r[0].(uint32)
			indRelID, _ := r[1].(uint32)
			indNAtts, _ := r[2].(int16)
			indNKeyAtts, _ := r[3].(int16)
			isUnique, _ := r[4].(bool)
			isPrimary, _ := r[6].(bool)
			isValid, _ := r[10].(bool)
			isReady, _ := r[12].(bool)
			isLive, _ := r[13].(bool)
			indKey, _ := r[15].([]int16)
			indOption, _ := r[18].([]int16)

			if !isValid || !isReady || !isLive || indNKeyAtts <= 0 {
				continue
			}
			// Skip expression indexes (indexprs != nil) and partial indexes (indpred != nil).
			if (len(r) > 19 && r[19] != nil) || (len(r) > 20 && r[20] != nil) {
				continue
			}
			icls, ok := classesByOID[indexRelID]
			if !ok || icls.relKind != 'i' || icls.relAM != PGBtreeAMOID {
				continue
			}

			// Also link TOAST table's B-tree index if indRelID is a table's ToastRelOID.
			for _, tbl := range tablesByOID {
				if tbl.ToastRelOID != 0 && tbl.ToastRelOID == indRelID {
					tbl.ToastIndexRel = icls.relNode
				}
			}

			tbl, ok := tablesByOID[indRelID]
			if !ok {
				continue
			}
			if len(indKey) < int(indNKeyAtts) {
				continue
			}
			keyAtts := make([]int16, int(indNKeyAtts))
			validCols := true
			for i := 0; i < int(indNKeyAtts); i++ {
				if indKey[i] <= 0 {
					// Expression index column (attnum == 0) -> skip index
					validCols = false
					break
				}
				keyAtts[i] = indKey[i]
			}
			if !validCols {
				continue
			}
			keyOpts := make([]int16, int(indNKeyAtts))
			for i := 0; i < int(indNKeyAtts) && i < len(indOption); i++ {
				keyOpts[i] = indOption[i]
			}
			tbl.Indexes = append(tbl.Indexes, DiscoveredIndex{
				Name:        icls.name,
				IndexRelOID: indexRelID,
				TableRelOID: indRelID,
				SpcOID:      icls.relSpc,
				DbOID:       dbOID,
				RelNumber:   icls.relNode,
				RelPages:    icls.relPages,
				RelTuples:   icls.relTuples,
				IndNAtts:    indNAtts,
				IndNKeyAtts: indNKeyAtts,
				IsUnique:    isUnique,
				IsPrimary:   isPrimary,
				KeyAttNums:  keyAtts,
				KeyOptions:  keyOpts,
			})
		}
	}

	// 6. Scan pg_statistic (dbSpcOID/dbOID/2619) when available to populate column statistics.
	// Missing or empty pg_statistic (e.g. before ANALYZE or in synthetic tests) is handled gracefully.
	if statRows, err := scanCatalogRelation(ctx, adapter, clog, snap, dbSpcOID, dbOID, PGStatisticRelID, pgStatisticCols); err == nil {
		for _, r := range statRows {
			if len(r) < 6 {
				continue
			}
			relID, _ := r[0].(uint32)
			tbl, ok := tablesByOID[relID]
			if !ok {
				continue
			}
			attNum, _ := r[1].(int16)
			if attNum <= 0 {
				continue
			}
			staInherit, _ := r[2].(bool)
			if staInherit && tbl.ColumnStats != nil {
				if _, exists := tbl.ColumnStats[attNum]; exists {
					continue
				}
			}
			nullFrac, _ := r[3].(float32)
			avgWidth, _ := r[4].(int32)
			distinct, _ := r[5].(float32)
			if tbl.ColumnStats == nil {
				tbl.ColumnStats = make(map[int16]ColumnStats)
			}
			tbl.ColumnStats[attNum] = ColumnStats{
				AttNum:   attNum,
				NullFrac: nullFrac,
				AvgWidth: avgWidth,
				Distinct: distinct,
			}
		}
	}

	out := make([]DiscoveredTable, 0, len(tablesByOID))
	for _, tbl := range tablesByOID {
		sort.Slice(tbl.Columns, func(i, j int) bool {
			return tbl.Columns[i].AttNum < tbl.Columns[j].AttNum
		})
		sort.Slice(tbl.Indexes, func(i, j int) bool {
			if tbl.Indexes[i].IsPrimary != tbl.Indexes[j].IsPrimary {
				return tbl.Indexes[i].IsPrimary
			}
			if tbl.Indexes[i].IsUnique != tbl.Indexes[j].IsUnique {
				return tbl.Indexes[i].IsUnique
			}
			return tbl.Indexes[i].Name < tbl.Indexes[j].Name
		})
		if len(tbl.Columns) > 0 {
			out = append(out, *tbl)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Name < out[j].Name
	})
	return out, nil
}
