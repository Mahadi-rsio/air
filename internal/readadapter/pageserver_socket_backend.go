package readadapter

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Mahadi-rsio/air/internal/pageserver"
)

var (
	// ErrPageserverOverloaded is returned when the Pageserver client request queue
	// reaches QueueCapacity.
	ErrPageserverOverloaded = errors.New("readadapter: pageserver request queue full")
)

// PageserverSocketConfig configures the concurrent Unix-socket pool to AIR Pageserver.
type PageserverSocketConfig struct {
	SocketPath     string
	MaxConns       int
	MaxInFlight    int
	QueueCapacity  int
	RequestTimeout time.Duration
	DialTimeout    time.Duration
}

// PageserverSocketStats reports live concurrency and reliability metrics for PageserverSocketBackend.
type PageserverSocketStats struct {
	ActiveConns   int64
	IdleConns     int
	InFlight      int64
	QueueDepth    int64
	RequestsTotal uint64
	FailuresTotal uint64
	TimeoutsTotal uint64
}

// PageserverSocketBackend implements StorageBackend and SnapshotProvider by
// communicating with an out-of-process AIR Pageserver over a bounded pool of
// Unix domain socket connections.
type PageserverSocketBackend struct {
	cfg PageserverSocketConfig

	connSem     chan struct{}
	inFlightSem chan struct{}

	poolMu    sync.Mutex
	idleConns []net.Conn
	closed    atomic.Bool

	activeConns   atomic.Int64
	inFlight      atomic.Int64
	queueDepth    atomic.Int64
	requestsTotal atomic.Uint64
	failuresTotal atomic.Uint64
	timeoutsTotal atomic.Uint64

	genMu    sync.RWMutex
	genCache map[string]cachedTenantInfo
}

type cachedTenantInfo struct {
	generation string
	lastLSN    uint64
	fetchedAt  time.Time
}

// NewPageserverSocketBackend creates a Unix-socket StorageBackend targeting socketPath
// with production-safe default concurrency limits.
func NewPageserverSocketBackend(socketPath string) *PageserverSocketBackend {
	return NewPageserverSocketBackendWithConfig(PageserverSocketConfig{
		SocketPath: socketPath,
	})
}

// NewPageserverSocketBackendWithConfig creates a Unix-socket StorageBackend with explicit pool limits.
func NewPageserverSocketBackendWithConfig(cfg PageserverSocketConfig) *PageserverSocketBackend {
	if cfg.MaxConns <= 0 {
		cfg.MaxConns = 16
	}
	if cfg.MaxInFlight <= 0 {
		cfg.MaxInFlight = cfg.MaxConns
	}
	if cfg.MaxInFlight > cfg.MaxConns {
		cfg.MaxInFlight = cfg.MaxConns
	}
	if cfg.QueueCapacity <= 0 {
		cfg.QueueCapacity = 256
	}
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = 10 * time.Second
	}
	if cfg.DialTimeout <= 0 {
		cfg.DialTimeout = 3 * time.Second
	}
	return &PageserverSocketBackend{
		cfg:         cfg,
		connSem:     make(chan struct{}, cfg.MaxConns),
		inFlightSem: make(chan struct{}, cfg.MaxInFlight),
		genCache:    make(map[string]cachedTenantInfo),
	}
}

// Stats returns live connection pool, in-flight, queue, and failure counters.
func (b *PageserverSocketBackend) Stats() PageserverSocketStats {
	b.poolMu.Lock()
	idle := len(b.idleConns)
	b.poolMu.Unlock()
	return PageserverSocketStats{
		ActiveConns:   b.activeConns.Load(),
		IdleConns:     idle,
		InFlight:      b.inFlight.Load(),
		QueueDepth:    b.queueDepth.Load(),
		RequestsTotal: b.requestsTotal.Load(),
		FailuresTotal: b.failuresTotal.Load(),
		TimeoutsTotal: b.timeoutsTotal.Load(),
	}
}

// Close closes all pooled Unix domain socket connections.
func (b *PageserverSocketBackend) Close() error {
	if !b.closed.CompareAndSwap(false, true) {
		return nil
	}
	b.poolMu.Lock()
	conns := b.idleConns
	b.idleConns = nil
	b.poolMu.Unlock()
	var firstErr error
	for _, c := range conns {
		if err := c.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		b.activeConns.Add(-1)
	}
	return firstErr
}

func (b *PageserverSocketBackend) acquireConn(ctx context.Context) (net.Conn, error) {
	if b.closed.Load() {
		return nil, fmt.Errorf("readadapter: pageserver backend closed")
	}
	b.poolMu.Lock()
	if n := len(b.idleConns); n > 0 {
		c := b.idleConns[n-1]
		b.idleConns = b.idleConns[:n-1]
		b.poolMu.Unlock()
		return c, nil
	}
	b.poolMu.Unlock()

	dialTimeout := b.cfg.DialTimeout
	if dl, ok := ctx.Deadline(); ok {
		if rem := time.Until(dl); rem < dialTimeout {
			dialTimeout = rem
		}
	}
	if dialTimeout <= 0 {
		return nil, context.DeadlineExceeded
	}
	d := net.Dialer{Timeout: dialTimeout}
	c, err := d.DialContext(ctx, "unix", b.cfg.SocketPath)
	if err != nil {
		return nil, fmt.Errorf("readadapter: dial pageserver %s: %w", b.cfg.SocketPath, err)
	}
	b.activeConns.Add(1)
	return c, nil
}

func (b *PageserverSocketBackend) releaseConn(c net.Conn, broken bool) {
	if c == nil {
		return
	}
	if broken || b.closed.Load() {
		_ = c.Close()
		b.activeConns.Add(-1)
		return
	}
	_ = c.SetDeadline(time.Time{})
	b.poolMu.Lock()
	if b.closed.Load() || len(b.idleConns) >= b.cfg.MaxConns {
		b.poolMu.Unlock()
		_ = c.Close()
		b.activeConns.Add(-1)
		return
	}
	b.idleConns = append(b.idleConns, c)
	b.poolMu.Unlock()
}

func writeFull(w io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data)
		if err != nil {
			return err
		}
		if n <= 0 {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}

func encodeReqHeader(dst []byte, h pageserver.ReqHeader) {
	for i := range dst {
		dst[i] = 0
	}
	binary.LittleEndian.PutUint32(dst[0:4], pageserver.ProtocolMagic)
	binary.LittleEndian.PutUint16(dst[4:6], pageserver.ProtocolVersion)
	binary.LittleEndian.PutUint16(dst[6:8], h.Op)
	n := len(h.TenantID)
	if n > pageserver.TenantIDMaxLen {
		n = pageserver.TenantIDMaxLen
	}
	copy(dst[8:8+n], h.TenantID[:n])
	binary.LittleEndian.PutUint32(dst[72:76], h.SpcOID)
	binary.LittleEndian.PutUint32(dst[76:80], h.DbOID)
	binary.LittleEndian.PutUint32(dst[80:84], h.RelNumber)
	dst[84] = h.ForkNum
	binary.LittleEndian.PutUint32(dst[88:92], h.BlockNum)
	binary.LittleEndian.PutUint32(dst[92:96], h.NBlocks)
	binary.LittleEndian.PutUint64(dst[96:104], h.LSN)
	binary.LittleEndian.PutUint64(dst[104:112], h.NotModifiedSince)
}

func decodeRespHeader(buf []byte) (pageserver.RespHeader, error) {
	if len(buf) != pageserver.RespHeaderSize {
		return pageserver.RespHeader{}, pageserver.ErrBadProtocol
	}
	magic := binary.LittleEndian.Uint32(buf[0:4])
	ver := binary.LittleEndian.Uint16(buf[4:6])
	if magic != pageserver.ProtocolMagic || ver != pageserver.ProtocolVersion {
		return pageserver.RespHeader{}, fmt.Errorf("%w: resp magic=0x%x ver=%d", pageserver.ErrBadProtocol, magic, ver)
	}
	return pageserver.RespHeader{
		Magic:        magic,
		Version:      ver,
		Status:       binary.LittleEndian.Uint16(buf[6:8]),
		PayloadLen:   binary.LittleEndian.Uint32(buf[8:12]),
		NBlocksOrVal: binary.LittleEndian.Uint32(buf[12:16]),
		U64Val:       binary.LittleEndian.Uint64(buf[16:24]),
	}, nil
}

func (b *PageserverSocketBackend) roundTrip(ctx context.Context, req pageserver.ReqHeader) (pageserver.RespHeader, []byte, error) {
	if b.closed.Load() {
		return pageserver.RespHeader{}, nil, fmt.Errorf("readadapter: pageserver backend closed")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, b.cfg.RequestTimeout)
		defer cancel()
	}

	b.requestsTotal.Add(1)

	// Bounded queue admission check.
	qd := b.queueDepth.Add(1)
	if qd > int64(b.cfg.QueueCapacity) {
		b.queueDepth.Add(-1)
		b.failuresTotal.Add(1)
		return pageserver.RespHeader{}, nil, ErrPageserverOverloaded
	}

	// Wait for in-flight permit respecting ctx cancellation.
	select {
	case b.inFlightSem <- struct{}{}:
		b.queueDepth.Add(-1)
	case <-ctx.Done():
		b.queueDepth.Add(-1)
		b.failuresTotal.Add(1)
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			b.timeoutsTotal.Add(1)
		}
		return pageserver.RespHeader{}, nil, ctx.Err()
	}
	defer func() { <-b.inFlightSem }()

	select {
	case b.connSem <- struct{}{}:
	case <-ctx.Done():
		b.failuresTotal.Add(1)
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			b.timeoutsTotal.Add(1)
		}
		return pageserver.RespHeader{}, nil, ctx.Err()
	}
	defer func() { <-b.connSem }()

	b.inFlight.Add(1)
	defer b.inFlight.Add(-1)

	var reqBuf [pageserver.ReqHeaderSize]byte
	var respBuf [pageserver.RespHeaderSize]byte
	encodeReqHeader(reqBuf[:], req)

	const maxPayloadLen = uint32(pageserver.MaxBatchBlocks*pageserver.PageSize + 64*1024)

	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		if err := ctx.Err(); err != nil {
			b.failuresTotal.Add(1)
			if errors.Is(err, context.DeadlineExceeded) {
				b.timeoutsTotal.Add(1)
			}
			return pageserver.RespHeader{}, nil, err
		}

		conn, err := b.acquireConn(ctx)
		if err != nil {
			lastErr = err
			continue
		}

		dl, _ := ctx.Deadline()
		_ = conn.SetDeadline(dl)

		// Cancel socket I/O immediately if ctx is cancelled mid-flight.
		var cancelDone chan struct{}
		if ctx.Done() != nil {
			stopWatch := make(chan struct{})
			cancelDone = make(chan struct{})
			go func(c net.Conn) {
				defer close(cancelDone)
				select {
				case <-ctx.Done():
					_ = c.SetDeadline(time.Now())
				case <-stopWatch:
				}
			}(conn)
			defer func(sw chan struct{}, cd chan struct{}) {
				close(sw)
				<-cd
			}(stopWatch, cancelDone)
		}

		if err := writeFull(conn, reqBuf[:]); err != nil {
			b.releaseConn(conn, true)
			lastErr = err
			continue
		}
		if _, err := io.ReadFull(conn, respBuf[:]); err != nil {
			b.releaseConn(conn, true)
			lastErr = err
			continue
		}
		resp, err := decodeRespHeader(respBuf[:])
		if err != nil {
			b.releaseConn(conn, true)
			b.failuresTotal.Add(1)
			return pageserver.RespHeader{}, nil, err
		}
		if resp.PayloadLen > maxPayloadLen {
			b.releaseConn(conn, true)
			b.failuresTotal.Add(1)
			return pageserver.RespHeader{}, nil, fmt.Errorf("%w: payload len %d exceeds max %d", pageserver.ErrBadProtocol, resp.PayloadLen, maxPayloadLen)
		}
		var payload []byte
		if resp.PayloadLen > 0 {
			payload = make([]byte, resp.PayloadLen)
			if _, err := io.ReadFull(conn, payload); err != nil {
				b.releaseConn(conn, true)
				lastErr = err
				continue
			}
		}

		b.releaseConn(conn, false)
		switch resp.Status {
		case pageserver.StatusOK:
			return resp, payload, nil
		case pageserver.StatusNotFound:
			return resp, nil, ErrNotFound
		default:
			b.failuresTotal.Add(1)
			return resp, nil, fmt.Errorf("readadapter: pageserver op=%d status=%d", req.Op, resp.Status)
		}
	}

	b.failuresTotal.Add(1)
	if errors.Is(lastErr, context.DeadlineExceeded) {
		b.timeoutsTotal.Add(1)
	}
	return pageserver.RespHeader{}, nil, lastErr
}

// GetTenantInfo queries the active generation and latest ingested WAL LSN for tenantID.
func (b *PageserverSocketBackend) GetTenantInfo(ctx context.Context, tenantID string) (string, uint64, error) {
	resp, payload, err := b.roundTrip(ctx, pageserver.ReqHeader{
		Op:       pageserver.OpGetTenantInfo,
		TenantID: tenantID,
	})
	if err != nil {
		return "", 0, err
	}
	gen := string(payload)
	b.genMu.Lock()
	b.genCache[tenantID] = cachedTenantInfo{
		generation: gen,
		lastLSN:    resp.U64Val,
		fetchedAt:  time.Now(),
	}
	b.genMu.Unlock()
	return gen, resp.U64Val, nil
}

// GetTenantGeneration implements StorageBackend.
func (b *PageserverSocketBackend) GetTenantGeneration(tenantID string) string {
	b.genMu.RLock()
	if c, ok := b.genCache[tenantID]; ok && time.Since(c.fetchedAt) < 2*time.Second {
		b.genMu.RUnlock()
		return c.generation
	}
	b.genMu.RUnlock()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	gen, _, err := b.GetTenantInfo(ctx, tenantID)
	if err != nil {
		return ""
	}
	return gen
}

// CurrentSnapshot implements SnapshotProvider, returning a read snapshot at the
// tenant's latest ingested WAL LSN.
func (b *PageserverSocketBackend) CurrentSnapshot(ctx context.Context, tenantID, generation string) (SnapshotInfo, error) {
	gen, lastLSN, err := b.GetTenantInfo(ctx, tenantID)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return SnapshotInfo{}, fmt.Errorf("%w: tenant %s not found in pageserver", ErrNotFound, tenantID)
		}
		return SnapshotInfo{}, err
	}
	if generation != "" && gen != "" && gen != generation {
		return SnapshotInfo{}, fmt.Errorf("%w: pageserver generation %q != %q", ErrStaleGeneration, gen, generation)
	}
	if lastLSN == 0 {
		lastLSN = ^uint64(0)
	}
	return SnapshotInfo{
		LSN:  lastLSN,
		Xmin: 0,
		Xmax: 0,
	}, nil
}

// GetPageAtLSN implements StorageBackend.
func (b *PageserverSocketBackend) GetPageAtLSN(ctx context.Context, tenantID string, key pageserver.Key, lsn uint64) ([]byte, error) {
	op := pageserver.OpGetPageAtLSN
	if key.Kind == pageserver.KeyKindSLRU {
		op = pageserver.OpGetSLRUPage
	}
	_, payload, err := b.roundTrip(ctx, pageserver.ReqHeader{
		Op:        op,
		TenantID:  tenantID,
		SpcOID:    key.SpcOID,
		DbOID:     key.DbOID,
		RelNumber: key.RelNumber,
		ForkNum:   key.ForkNum,
		BlockNum:  key.BlockNum,
		NBlocks:   1,
		LSN:       lsn,
	})
	if err != nil {
		return nil, err
	}
	return payload, nil
}

// GetPageBatch implements StorageBackend.
func (b *PageserverSocketBackend) GetPageBatch(ctx context.Context, tenantID string, spcOID, dbOID, relNumber uint32, forkNum uint8, startBlock, nblocks uint32, lsn uint64) ([]byte, uint32, error) {
	resp, payload, err := b.roundTrip(ctx, pageserver.ReqHeader{
		Op:        pageserver.OpGetPageBatch,
		TenantID:  tenantID,
		SpcOID:    spcOID,
		DbOID:     dbOID,
		RelNumber: relNumber,
		ForkNum:   forkNum,
		BlockNum:  startBlock,
		NBlocks:   nblocks,
		LSN:       lsn,
	})
	if err != nil {
		return nil, 0, err
	}
	return payload, resp.NBlocksOrVal, nil
}

// GetRelSize implements StorageBackend.
func (b *PageserverSocketBackend) GetRelSize(ctx context.Context, tenantID string, spcOID, dbOID, relNumber uint32, forkNum uint8, lsn uint64) (uint32, error) {
	resp, _, err := b.roundTrip(ctx, pageserver.ReqHeader{
		Op:        pageserver.OpGetRelSize,
		TenantID:  tenantID,
		SpcOID:    spcOID,
		DbOID:     dbOID,
		RelNumber: relNumber,
		ForkNum:   forkNum,
		LSN:       lsn,
	})
	if err != nil {
		return 0, err
	}
	return resp.NBlocksOrVal, nil
}

// GetRelExists implements StorageBackend.
func (b *PageserverSocketBackend) GetRelExists(ctx context.Context, tenantID string, spcOID, dbOID, relNumber uint32, forkNum uint8, lsn uint64) (bool, error) {
	resp, _, err := b.roundTrip(ctx, pageserver.ReqHeader{
		Op:        pageserver.OpGetRelExists,
		TenantID:  tenantID,
		SpcOID:    spcOID,
		DbOID:     dbOID,
		RelNumber: relNumber,
		ForkNum:   forkNum,
		LSN:       lsn,
	})
	if err != nil {
		return false, err
	}
	return resp.NBlocksOrVal != 0, nil
}
