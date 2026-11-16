package pageserver

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
)

// Wire protocol constants matching extensions/air_smgr/protocol.h.
const (
	ProtocolMagic   uint32 = 0x41495250 // "AIRP"
	ProtocolVersion uint16 = 1
	TenantIDMaxLen  int    = 64
	PageSize        int    = 8192

	ReqHeaderSize  int = 112
	RespHeaderSize int = 24
	MaxBatchBlocks int = 128
)

// Wire operations matching AirPsOp in protocol.h.
const (
	OpGetPageAtLSN uint16 = 1
	OpGetRelSize   uint16 = 2
	OpGetRelExists uint16 = 3
	OpGetDBSize    uint16 = 4
	OpGetPageBatch uint16 = 5
	OpPutPageBatch uint16 = 6
)

// Wire response statuses matching AirPsStatus in protocol.h.
const (
	StatusOK       uint16 = 0
	StatusNotFound uint16 = 1
	StatusError    uint16 = 2
)

// Sentinel errors for pageserver operations.
var (
	ErrNotFound    = errors.New("pageserver: key not found")
	ErrBadProtocol = errors.New("pageserver: invalid wire protocol header")
	ErrFenced      = errors.New("pageserver: tenant fenced by newer epoch")
)

// RelFileLocator identifies a PostgreSQL relation file (PG17 RelFileLocator).
type RelFileLocator struct {
	SpcOID    uint32
	DbOID     uint32
	RelNumber uint32
}

// KeyKind distinguishes storage key families in the 2D (Key, LSN) space.
type KeyKind uint8

const (
	KeyKindRelBlock KeyKind = 1
	KeyKindRelSize  KeyKind = 2
	KeyKindDBSize   KeyKind = 3
	KeyKindSLRU     KeyKind = 4
)

// Key is a 24-byte coordinate in the Pageserver storage space.
type Key struct {
	Kind      KeyKind
	ForkNum   uint8
	Reserved  uint16
	SpcOID    uint32
	DbOID     uint32
	RelNumber uint32
	BlockNum  uint32
	Extra     uint32
}

// RelBlockKey constructs a Key for an 8 KB relation block.
func RelBlockKey(spcOID, dbOID, relNumber uint32, forkNum uint8, blockNum uint32) Key {
	return Key{
		Kind:      KeyKindRelBlock,
		ForkNum:   forkNum,
		SpcOID:    spcOID,
		DbOID:     dbOID,
		RelNumber: relNumber,
		BlockNum:  blockNum,
	}
}

// RelSizeKey constructs a Key for a relation fork's block count.
func RelSizeKey(spcOID, dbOID, relNumber uint32, forkNum uint8) Key {
	return Key{
		Kind:      KeyKindRelSize,
		ForkNum:   forkNum,
		SpcOID:    spcOID,
		DbOID:     dbOID,
		RelNumber: relNumber,
	}
}

// Compare orders two Keys deterministically for sorted layer indexes.
func (k Key) Compare(other Key) int {
	if k.Kind != other.Kind {
		if k.Kind < other.Kind {
			return -1
		}
		return 1
	}
	fields := [6][2]uint32{
		{k.SpcOID, other.SpcOID},
		{k.DbOID, other.DbOID},
		{k.RelNumber, other.RelNumber},
		{uint32(k.ForkNum), uint32(other.ForkNum)},
		{k.BlockNum, other.BlockNum},
		{k.Extra, other.Extra},
	}
	for _, pair := range fields {
		if pair[0] < pair[1] {
			return -1
		}
		if pair[0] > pair[1] {
			return 1
		}
	}
	return 0
}

// Encode writes the 24-byte big-endian Key representation.
func (k Key) Encode(dst []byte) {
	dst[0] = byte(k.Kind)
	dst[1] = k.ForkNum
	binary.BigEndian.PutUint16(dst[2:4], k.Reserved)
	binary.BigEndian.PutUint32(dst[4:8], k.SpcOID)
	binary.BigEndian.PutUint32(dst[8:12], k.DbOID)
	binary.BigEndian.PutUint32(dst[12:16], k.RelNumber)
	binary.BigEndian.PutUint32(dst[16:20], k.BlockNum)
	binary.BigEndian.PutUint32(dst[20:24], k.Extra)
}

// DecodeKey reads a 24-byte big-endian Key.
func DecodeKey(src []byte) Key {
	return Key{
		Kind:      KeyKind(src[0]),
		ForkNum:   src[1],
		Reserved:  binary.BigEndian.Uint16(src[2:4]),
		SpcOID:    binary.BigEndian.Uint32(src[4:8]),
		DbOID:     binary.BigEndian.Uint32(src[8:12]),
		RelNumber: binary.BigEndian.Uint32(src[12:16]),
		BlockNum:  binary.BigEndian.Uint32(src[16:20]),
		Extra:     binary.BigEndian.Uint32(src[20:24]),
	}
}

func (k Key) String() string {
	return fmt.Sprintf("k%d:%d/%d/%d.%d#%d", k.Kind, k.SpcOID, k.DbOID, k.RelNumber, k.ForkNum, k.BlockNum)
}

// ReqHeader mirrors AirPsReqHeader (104 bytes little-endian).
type ReqHeader struct {
	Magic             uint32
	Version           uint16
	Op                uint16
	TenantID          string
	SpcOID            uint32
	DbOID             uint32
	RelNumber         uint32
	ForkNum           uint8
	BlockNum          uint32
	NBlocks           uint32
	LSN               uint64
	NotModifiedSince  uint64
}

// RespHeader mirrors AirPsRespHeader (24 bytes little-endian).
type RespHeader struct {
	Magic       uint32
	Version     uint16
	Status      uint16
	PayloadLen  uint32
	NBlocksOrVal uint32
	U64Val      uint64
}

func decodeReqHeader(buf []byte) (ReqHeader, error) {
	if len(buf) != ReqHeaderSize {
		return ReqHeader{}, ErrBadProtocol
	}
	magic := binary.LittleEndian.Uint32(buf[0:4])
	ver := binary.LittleEndian.Uint16(buf[4:6])
	if magic != ProtocolMagic || ver != ProtocolVersion {
		return ReqHeader{}, fmt.Errorf("%w: magic=0x%x version=%d", ErrBadProtocol, magic, ver)
	}
	rawTenant := buf[8 : 8+TenantIDMaxLen]
	if idx := bytes.IndexByte(rawTenant, 0); idx >= 0 {
		rawTenant = rawTenant[:idx]
	}
	return ReqHeader{
		Magic:            magic,
		Version:          ver,
		Op:               binary.LittleEndian.Uint16(buf[6:8]),
		TenantID:         string(rawTenant),
		SpcOID:           binary.LittleEndian.Uint32(buf[72:76]),
		DbOID:            binary.LittleEndian.Uint32(buf[76:80]),
		RelNumber:        binary.LittleEndian.Uint32(buf[80:84]),
		ForkNum:          buf[84],
		BlockNum:         binary.LittleEndian.Uint32(buf[88:92]),
		NBlocks:          binary.LittleEndian.Uint32(buf[92:96]),
		LSN:              binary.LittleEndian.Uint64(buf[96:104]),
		NotModifiedSince: binary.LittleEndian.Uint64(buf[104:112]),
	}, nil
}

func encodeRespHeader(dst []byte, h RespHeader) {
	binary.LittleEndian.PutUint32(dst[0:4], ProtocolMagic)
	binary.LittleEndian.PutUint16(dst[4:6], ProtocolVersion)
	binary.LittleEndian.PutUint16(dst[6:8], h.Status)
	binary.LittleEndian.PutUint32(dst[8:12], h.PayloadLen)
	binary.LittleEndian.PutUint32(dst[12:16], h.NBlocksOrVal)
	binary.LittleEndian.PutUint64(dst[16:24], h.U64Val)
}

// Service handles Pageserver wire protocol requests from air_smgr.
type Service interface {
	GetPageAtLSN(ctx context.Context, tenantID string, key Key, lsn uint64) ([]byte, error)
	GetPageBatch(ctx context.Context, tenantID string, spcOID, dbOID, relNumber uint32, forkNum uint8, startBlock, nblocks uint32, lsn uint64) ([]byte, uint32, error)
	GetRelSize(ctx context.Context, tenantID string, spcOID, dbOID, relNumber uint32, forkNum uint8, lsn uint64) (uint32, error)
	GetRelExists(ctx context.Context, tenantID string, spcOID, dbOID, relNumber uint32, forkNum uint8, lsn uint64) (bool, error)
	GetDBSize(ctx context.Context, tenantID string, spcOID, dbOID uint32, lsn uint64) (uint64, error)
	PutPageBatch(ctx context.Context, tenantID string, spcOID, dbOID, relNumber uint32, forkNum uint8, startBlock, nblocks uint32, lsn uint64, pages []byte) error
}

// UnixServer serves the binary air_smgr protocol over an AF_UNIX stream socket.
type UnixServer struct {
	socketPath string
	svc        Service

	mu       sync.Mutex
	listener net.Listener
	conns    map[net.Conn]struct{}
	wg       sync.WaitGroup
	closed   bool
}

// NewUnixServer constructs a Unix domain socket server for air_smgr.
func NewUnixServer(socketPath string, svc Service) *UnixServer {
	return &UnixServer{
		socketPath: socketPath,
		svc:        svc,
		conns:      make(map[net.Conn]struct{}),
	}
}

// Start binds the Unix socket and begins accepting connections until ctx is cancelled.
func (s *UnixServer) Start(ctx context.Context) error {
	if err := os.MkdirAll(filepath.Dir(s.socketPath), 0o755); err != nil {
		return err
	}
	_ = os.Remove(s.socketPath)

	ln, err := net.Listen("unix", s.socketPath)
	if err != nil {
		return err
	}
	if err := os.Chmod(s.socketPath, 0o660); err != nil {
		_ = ln.Close()
		return err
	}

	s.mu.Lock()
	s.listener = ln
	s.mu.Unlock()

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			s.wg.Add(1)
			go func(c net.Conn) {
				defer s.wg.Done()
				s.handleConn(ctx, c)
			}(conn)
		}
	}()

	go func() {
		<-ctx.Done()
		_ = s.Close()
	}()
	return nil
}

// Close stops accepting connections and waits for active handlers to exit.
func (s *UnixServer) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	ln := s.listener
	for c := range s.conns {
		_ = c.Close()
	}
	s.mu.Unlock()

	var err error
	if ln != nil {
		err = ln.Close()
	}
	_ = os.Remove(s.socketPath)
	s.wg.Wait()
	return err
}

func (s *UnixServer) handleConn(ctx context.Context, conn net.Conn) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		_ = conn.Close()
		return
	}
	s.conns[conn] = struct{}{}
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		delete(s.conns, conn)
		s.mu.Unlock()
		_ = conn.Close()
	}()
	reqBuf := make([]byte, ReqHeaderSize)
	respHdr := make([]byte, RespHeaderSize)

	for {
		if _, err := io.ReadFull(conn, reqBuf); err != nil {
			return
		}
		req, err := decodeReqHeader(reqBuf)
		if err != nil {
			encodeRespHeader(respHdr, RespHeader{Status: StatusError})
			_, _ = conn.Write(respHdr)
			return
		}

		var status uint16 = StatusOK
		var payload []byte
		var nblocksOrVal uint32
		var u64Val uint64

		switch req.Op {
		case OpGetPageAtLSN:
			key := RelBlockKey(req.SpcOID, req.DbOID, req.RelNumber, req.ForkNum, req.BlockNum)
			page, err := s.svc.GetPageAtLSN(ctx, req.TenantID, key, req.LSN)
			if errors.Is(err, ErrNotFound) {
				status = StatusNotFound
			} else if err != nil {
				status = StatusError
			} else {
				payload = page
				nblocksOrVal = 1
			}
		case OpGetPageBatch:
			n := req.NBlocks
			if n == 0 {
				n = 1
			}
			if int(n) > MaxBatchBlocks {
				n = uint32(MaxBatchBlocks)
			}
			pages, count, err := s.svc.GetPageBatch(ctx, req.TenantID, req.SpcOID, req.DbOID, req.RelNumber, req.ForkNum, req.BlockNum, n, req.LSN)
			if errors.Is(err, ErrNotFound) {
				status = StatusNotFound
			} else if err != nil {
				status = StatusError
			} else {
				payload = pages
				nblocksOrVal = count
			}
		case OpGetRelSize:
			nb, err := s.svc.GetRelSize(ctx, req.TenantID, req.SpcOID, req.DbOID, req.RelNumber, req.ForkNum, req.LSN)
			if errors.Is(err, ErrNotFound) {
				status = StatusNotFound
			} else if err != nil {
				status = StatusError
			} else {
				nblocksOrVal = nb
			}
		case OpGetRelExists:
			exists, err := s.svc.GetRelExists(ctx, req.TenantID, req.SpcOID, req.DbOID, req.RelNumber, req.ForkNum, req.LSN)
			if err != nil {
				status = StatusError
			} else if exists {
				nblocksOrVal = 1
			} else {
				nblocksOrVal = 0
			}
		case OpGetDBSize:
			sz, err := s.svc.GetDBSize(ctx, req.TenantID, req.SpcOID, req.DbOID, req.LSN)
			if errors.Is(err, ErrNotFound) {
				status = StatusNotFound
			} else if err != nil {
				status = StatusError
			} else {
				u64Val = sz
			}
		case OpPutPageBatch:
			n := req.NBlocks
			if n == 0 || int(n) > MaxBatchBlocks {
				return
			}
			inBuf := make([]byte, int(n)*PageSize)
			if _, err := io.ReadFull(conn, inBuf); err != nil {
				return
			}
			if err := s.svc.PutPageBatch(ctx, req.TenantID, req.SpcOID, req.DbOID, req.RelNumber, req.ForkNum, req.BlockNum, n, req.LSN, inBuf); err != nil {
				status = StatusError
			} else {
				nblocksOrVal = n
			}
		default:
			status = StatusError
		}

		encodeRespHeader(respHdr, RespHeader{
			Status:       status,
			PayloadLen:   uint32(len(payload)),
			NBlocksOrVal: nblocksOrVal,
			U64Val:       u64Val,
		})
		if _, err := conn.Write(respHdr); err != nil {
			return
		}
		if len(payload) > 0 {
			if _, err := conn.Write(payload); err != nil {
				return
			}
		}
	}
}
