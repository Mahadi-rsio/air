package readadapter

import (
	"context"
	"sync"
	"sync/atomic"
)

const parallelChunkBlocks uint32 = 16

type parallelScanBatch struct {
	rows    []HeapVal
	numRows int
	err     error
}

// parallelScanIter streams matching visible rows from [0, nblocks) using a
// bounded number of workers and a bounded channel for backpressure.
type parallelScanIter struct {
	ctx    context.Context
	cancel context.CancelFunc

	rel        *RelationHandle
	adapter    *Adapter
	clog       *CLOGResolver
	snap       *ReadSnapshot
	allCols    []ColumnType
	neededCols []bool
	filterCols []bool
	filters    []vtabFilter
	toastFn    TOASTResolverFunc
	nblocks    uint32
	workers    int

	nextBlock atomic.Uint32
	outCh     chan parallelScanBatch
	wg        sync.WaitGroup
	closed    bool
}

func newParallelScanIter(
	ctx context.Context,
	adapter *Adapter,
	clog *CLOGResolver,
	rel *RelationHandle,
	snap *ReadSnapshot,
	allCols []ColumnType,
	neededCols []bool,
	filterCols []bool,
	filters []vtabFilter,
	toastFn TOASTResolverFunc,
	nblocks uint32,
	workers int,
) *parallelScanIter {
	if workers < 2 {
		workers = 2
	}
	if workers > 4 {
		workers = 4
	}
	pctx, pcancel := context.WithCancel(ctx)
	it := &parallelScanIter{
		ctx:        pctx,
		cancel:     pcancel,
		rel:        rel,
		adapter:    adapter,
		clog:       clog,
		snap:       snap,
		allCols:    allCols,
		neededCols: neededCols,
		filterCols: filterCols,
		filters:    filters,
		toastFn:    toastFn,
		nblocks:    nblocks,
		workers:    workers,
		outCh:      make(chan parallelScanBatch, workers*2), // bounded backpressure buffer
	}
	it.wg.Add(workers)
	for w := 0; w < workers; w++ {
		go it.worker()
	}
	go func() {
		it.wg.Wait()
		close(it.outCh)
	}()
	return it
}

func (it *parallelScanIter) worker() {
	defer it.wg.Done()
	ncols := len(it.allCols)
	rowBuf := make([]HeapVal, ncols)
	var lpBuf []LinePointer

	for {
		if it.ctx.Err() != nil {
			return
		}
		startBlk := it.nextBlock.Add(parallelChunkBlocks) - parallelChunkBlocks
		if startBlk >= it.nblocks {
			return
		}
		n := parallelChunkBlocks
		if startBlk+n > it.nblocks {
			n = it.nblocks - startBlk
		}
		pages, err := it.adapter.ReadPages(it.ctx, it.rel, it.snap, startBlk, n)
		if err != nil {
			select {
			case it.outCh <- parallelScanBatch{err: err}:
			case <-it.ctx.Done():
			}
			return
		}

		var batchRows []HeapVal
		numRows := 0
		for _, pd := range pages {
			vtabPagesRead.Add(1)
			if IsZeroPage(pd.Data) {
				continue
			}
			phdr, err := ParsePageHeader(pd.Data)
			if err != nil {
				select {
				case it.outCh <- parallelScanBatch{err: err}:
				case <-it.ctx.Done():
				}
				return
			}
			lpBuf = ParseLinePointersInto(pd.Data, phdr, lpBuf)
			for _, lp := range lpBuf {
				if lp.Flags != LPNormal {
					continue
				}
				if err := ValidateLinePointer(lp, phdr); err != nil {
					select {
					case it.outCh <- parallelScanBatch{err: err}:
					case <-it.ctx.Done():
					}
					return
				}
				tup := pd.Data[int(lp.Offset) : int(lp.Offset)+int(lp.Length)]
				thdr, err := ParseHeapTupleHeader(tup)
				if err != nil {
					select {
					case it.outCh <- parallelScanBatch{err: err}:
					case <-it.ctx.Done():
					}
					return
				}
				vis, err := IsVisibleWithCLOG(it.ctx, thdr, it.snap, it.clog)
				if err != nil {
					select {
					case it.outCh <- parallelScanBatch{err: err}:
					case <-it.ctx.Done():
					}
					return
				}
				if !vis {
					continue
				}
				vtabTuplesDecoded.Add(1)
				if len(it.filterCols) > 0 {
					if err := DecodeHeapColumnsWithTOASTInto(tup, thdr, it.allCols, it.filterCols, rowBuf, false, it.toastFn); err != nil {
						select {
						case it.outCh <- parallelScanBatch{err: err}:
						case <-it.ctx.Done():
						}
						return
					}
					if !evalFiltersOnRow(it.filters, rowBuf) {
						continue
					}
				}
				if err := DecodeHeapColumnsWithTOASTInto(tup, thdr, it.allCols, it.neededCols, rowBuf, false, it.toastFn); err != nil {
					select {
					case it.outCh <- parallelScanBatch{err: err}:
					case <-it.ctx.Done():
					}
					return
				}
				if len(it.filterCols) == 0 && !evalFiltersOnRow(it.filters, rowBuf) {
					continue
				}
				for j := range rowBuf {
					rowBuf[j].Materialize()
				}
				vtabRowsMatched.Add(1)
				batchRows = append(batchRows, rowBuf...)
				numRows++
			}
		}
		if numRows > 0 {
			select {
			case it.outCh <- parallelScanBatch{rows: batchRows, numRows: numRows}:
			case <-it.ctx.Done():
				return
			}
		}
	}
}

func (it *parallelScanIter) Next() ([]HeapVal, int, bool, error) {
	if it == nil {
		return nil, 0, false, nil
	}
	b, ok := <-it.outCh
	if !ok {
		return nil, 0, false, nil
	}
	if b.err != nil {
		return nil, 0, false, b.err
	}
	return b.rows, b.numRows, true, nil
}

func (it *parallelScanIter) Close() {
	if it == nil || it.closed {
		return
	}
	it.closed = true
	it.cancel()
	for range it.outCh {
	}
}

func evalFiltersOnRow(filters []vtabFilter, row []HeapVal) bool {
	for _, f := range filters {
		if f.allColIdx < 0 || f.allColIdx >= len(row) {
			return false
		}
		if !evalFilterVal(f, row[f.allColIdx]) {
			return false
		}
	}
	return true
}

// ParallelHeapAggregate performs a parallel block-range scan computing COUNT(*)
// and optional SUM(colIdx) across workers without materializing rows.
func ParallelHeapAggregate(
	ctx context.Context,
	adapter *Adapter,
	clog *CLOGResolver,
	rel *RelationHandle,
	snap *ReadSnapshot,
	allCols []ColumnType,
	sumColIdx int,
	nblocks uint32,
	workers int,
) (count int64, sum int64, err error) {
	if workers < 1 {
		workers = 1
	}
	if nblocks == 0 {
		return 0, 0, nil
	}
	var nextBlock atomic.Uint32
	var totalCount atomic.Int64
	var totalSum atomic.Int64
	var firstErr atomic.Value
	var needed []bool
	if sumColIdx >= 0 && sumColIdx < len(allCols) {
		needed = make([]bool, len(allCols))
		needed[sumColIdx] = true
	}

	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func() {
			defer wg.Done()
			var localCnt int64
			var localSum int64
			rowBuf := make([]HeapVal, len(allCols))
			var lpBuf []LinePointer
			for {
				if ctx.Err() != nil {
					return
				}
				startBlk := nextBlock.Add(parallelChunkBlocks) - parallelChunkBlocks
				if startBlk >= nblocks {
					break
				}
				n := parallelChunkBlocks
				if startBlk+n > nblocks {
					n = nblocks - startBlk
				}
				pages, perr := adapter.ReadPages(ctx, rel, snap, startBlk, n)
				if perr != nil {
					firstErr.CompareAndSwap(nil, perr)
					return
				}
				for _, pd := range pages {
					if IsZeroPage(pd.Data) {
						continue
					}
					phdr, herr := ParsePageHeader(pd.Data)
					if herr != nil {
						firstErr.CompareAndSwap(nil, herr)
						return
					}
					lpBuf = ParseLinePointersInto(pd.Data, phdr, lpBuf)
					for _, lp := range lpBuf {
						if lp.Flags != LPNormal {
							continue
						}
						if verr := ValidateLinePointer(lp, phdr); verr != nil {
							firstErr.CompareAndSwap(nil, verr)
							return
						}
						tup := pd.Data[int(lp.Offset) : int(lp.Offset)+int(lp.Length)]
						thdr, terr := ParseHeapTupleHeader(tup)
						if terr != nil {
							firstErr.CompareAndSwap(nil, terr)
							return
						}
						vis, verr := IsVisibleWithCLOG(ctx, thdr, snap, clog)
						if verr != nil {
							firstErr.CompareAndSwap(nil, verr)
							return
						}
						if !vis {
							continue
						}
						localCnt++
						if needed != nil {
							if derr := DecodeHeapColumnsWithTOASTInto(tup, thdr, allCols, needed, rowBuf, false, nil); derr == nil {
								if v, ok := heapValToInt64(rowBuf[sumColIdx]); ok {
									localSum += v
								}
							}
						}
					}
				}
			}
			totalCount.Add(localCnt)
			totalSum.Add(localSum)
		}()
	}
	wg.Wait()
	if v := firstErr.Load(); v != nil {
		return 0, 0, v.(error)
	}
	if ctx.Err() != nil {
		return 0, 0, ctx.Err()
	}
	return totalCount.Load(), totalSum.Load(), nil
}
