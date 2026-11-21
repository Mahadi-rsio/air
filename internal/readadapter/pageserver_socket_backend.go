package readadapter

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"sync"
	"time"

	"github.com/Mahadi-rsio/air/internal/pageserver"
)

// PageserverSocketBackend implements StorageBackend and SnapshotProvider by
// communicating with an out-of-process AIR Pageserver over its Unix domain socket.
type PageserverSocketBackend struct {
	socketPath string
	timeout    time.Duration

	mu   sync.Mutex
	conn net.Conn

	genMu   sync.RWMutex
	genCache map[string]cachedTenantInfo
}

type cachedTenantInfo struct {
	generation string
	lastLSN    uint64
	fetchedAt  time.Time
}

// NewPageserverSocketBackend creates a Unix-socket StorageBackend targeting pageserver.sock.
func NewPageserverSocketBackend(socketPath string) *PageserverSocketBackend {
	return &PageserverSocketBackend{
		socketPath: socketPath,
		timeout:    10 * time.Second,
		genCache:   make(map[string]cachedTenantInfo),
	}
}

// Close closes the underlying Unix domain socket connection if open.
func (b *PageserverSocketBackend) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.conn != nil {
		err := b.conn.Close()
		b.conn = nil
		return err
	}
	return nil
}

func (b *PageserverSocketBackend) ensureConnLocked() (net.Conn, error) {
	if b.conn != nil {
		return b.conn, nil
	}
	c, err := net.DialTimeout("unix", b.socketPath, b.timeout)
	if err != nil {
		return nil, fmt.Errorf("readadapter: dial pageserver %s: %w", b.socketPath, err)
	}
	b.conn = c
	return c, nil
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
	b.mu.Lock()
	defer b.mu.Unlock()

	var reqBuf [pageserver.ReqHeaderSize]byte
	var respBuf [pageserver.RespHeaderSize]byte
	encodeReqHeader(reqBuf[:], req)

	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		conn, err := b.ensureConnLocked()
		if err != nil {
			return pageserver.RespHeader{}, nil, err
		}
		if deadline, ok := ctx.Deadline(); ok {
			_ = conn.SetDeadline(deadline)
		} else {
			_ = conn.SetDeadline(time.Now().Add(b.timeout))
		}
		if _, err := conn.Write(reqBuf[:]); err != nil {
			_ = conn.Close()
			b.conn = nil
			lastErr = err
			continue
		}
		if _, err := io.ReadFull(conn, respBuf[:]); err != nil {
			_ = conn.Close()
			b.conn = nil
			lastErr = err
			continue
		}
		resp, err := decodeRespHeader(respBuf[:])
		if err != nil {
			_ = conn.Close()
			b.conn = nil
			return pageserver.RespHeader{}, nil, err
		}
		var payload []byte
		if resp.PayloadLen > 0 {
			payload = make([]byte, resp.PayloadLen)
			if _, err := io.ReadFull(conn, payload); err != nil {
				_ = conn.Close()
				b.conn = nil
				lastErr = err
				continue
			}
		}
		switch resp.Status {
		case pageserver.StatusOK:
			return resp, payload, nil
		case pageserver.StatusNotFound:
			return resp, nil, ErrNotFound
		default:
			return resp, nil, fmt.Errorf("readadapter: pageserver op=%d status=%d", req.Op, resp.Status)
		}
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
		lastLSN = math.MaxUint64
	}
	return SnapshotInfo{
		LSN:  lastLSN,
		Xmin: 0,
		Xmax: math.MaxUint32,
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
