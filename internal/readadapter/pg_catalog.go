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
	PGNamespaceRelID uint32 = 2615
)

// DiscoveredColumn describes one user column of a PostgreSQL table discovered
// directly from PG17 pg_attribute pages.
type DiscoveredColumn struct {
	Name       string
	AttNum     int16
	PGTypeOID  uint32
	ColType    ColumnType
	SQLiteType string
	IsDropped  bool
}

// DiscoveredTable describes a PostgreSQL table discovered from PG17 catalog pages.
type DiscoveredTable struct {
	Schema     string
	Name       string
	RelOID     uint32
	SpcOID     uint32
	DbOID      uint32
	RelNumber  uint32
	Columns    []DiscoveredColumn
}

var (
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
		ColOID,  // 1: attrelid
		ColName, // 2: attname
		ColOID,  // 3: atttypid
		ColInt2, // 4: attlen
		ColInt2, // 5: attnum
		ColInt4, // 6: attcacheoff
		ColInt4, // 7: atttypmod
		ColInt2, // 8: attndims
		ColBool, // 9: attbyval
		ColChar, // 10: attalign
		ColChar, // 11: attstorage
		ColChar, // 12: attcompression
		ColBool, // 13: attnotnull
		ColBool, // 14: atthasdef
		ColBool, // 15: atthasmissing
		ColChar, // 16: attidentity
		ColChar, // 17: attgenerated
		ColBool, // 18: attisdropped
	}

	pgAttributeColsPG15 = []ColumnType{
		ColOID,  // 1: attrelid
		ColName, // 2: attname
		ColOID,  // 3: atttypid
		ColInt4, // 4: attstattarget (PG15)
		ColInt2, // 5: attlen
		ColInt2, // 6: attnum
		ColInt4, // 7: attndims (int4 in PG15)
		ColInt4, // 8: attcacheoff
		ColInt4, // 9: atttypmod
		ColBool, // 10: attbyval
		ColChar, // 11: attalign
		ColChar, // 12: attstorage
		ColChar, // 13: attcompression
		ColBool, // 14: attnotnull
		ColBool, // 15: atthasdef
		ColBool, // 16: atthasmissing
		ColChar, // 17: attidentity
		ColChar, // 18: attgenerated
		ColBool, // 19: attisdropped
	}
)

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
				} else if relNumber == PGAttributeRelID && (thdr.Infomask2&HeapNattsMask) == 26 {
					decodeCols = pgAttributeColsPG15
				}
				vals, err := DecodeHeapColumns(tup, thdr, decodeCols)
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
	for _, r := range dbRows {
		name, _ := r[1].(string)
		oid, _ := r[0].(uint32)
		spc := PGDefaultTablespaceOID
		spcIdx := 11
		if len(r) == len(pgDatabaseColsPG15) {
			spcIdx = 10
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

	// 3. Scan pg_class (dbSpcOID/dbOID/1259) for ordinary tables (relkind='r') in public.
	classRows, err := scanCatalogRelation(ctx, adapter, clog, snap, dbSpcOID, dbOID, PGClassRelID, pgClassCols)
	if err != nil {
		return nil, fmt.Errorf("readadapter: scan pg_class: %w", err)
	}
	tablesByOID := make(map[uint32]*DiscoveredTable)
	for _, r := range classRows {
		nspOID, _ := r[2].(uint32)
		relkind, _ := r[16].(byte)
		if nspOID != publicNspOID || relkind != 'r' {
			continue
		}
		relOID, _ := r[0].(uint32)
		relName, _ := r[1].(string)
		relFileNode, _ := r[7].(uint32)
		relSpc, _ := r[8].(uint32)
		if relFileNode == 0 {
			relFileNode = relOID
		}
		if relSpc == 0 {
			relSpc = dbSpcOID
		}
		tablesByOID[relOID] = &DiscoveredTable{
			Schema:    "public",
			Name:      relName,
			RelOID:    relOID,
			SpcOID:    relSpc,
			DbOID:     dbOID,
			RelNumber: relFileNode,
		}
	}
	if len(tablesByOID) == 0 {
		return nil, nil
	}

	// 4. Scan pg_attribute (dbSpcOID/dbOID/1249) for user columns (attnum > 0).
	attRows, err := scanCatalogRelation(ctx, adapter, clog, snap, dbSpcOID, dbOID, PGAttributeRelID, pgAttributeCols)
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
		droppedIdx := 17
		if len(r) == len(pgAttributeColsPG15) {
			attNumIdx = 5
			droppedIdx = 18
		}
		attNum, _ := r[attNumIdx].(int16)
		if attNum <= 0 {
			continue
		}
		attName, _ := r[1].(string)
		attTypID, _ := r[2].(uint32)
		isDropped, _ := r[droppedIdx].(bool)
		colType, sqliteType := PGTypeOIDToColumnType(attTypID)
		tbl.Columns = append(tbl.Columns, DiscoveredColumn{
			Name:       attName,
			AttNum:     attNum,
			PGTypeOID:  attTypID,
			ColType:    colType,
			SQLiteType: sqliteType,
			IsDropped:  isDropped,
		})
	}

	out := make([]DiscoveredTable, 0, len(tablesByOID))
	for _, tbl := range tablesByOID {
		sort.Slice(tbl.Columns, func(i, j int) bool {
			return tbl.Columns[i].AttNum < tbl.Columns[j].AttNum
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
