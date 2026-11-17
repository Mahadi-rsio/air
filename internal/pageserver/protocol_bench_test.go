package pageserver

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"path/filepath"
	"testing"
)

type benchService struct {
	page  []byte
	batch []byte
}

func (s *benchService) GetPageAtLSN(_ context.Context, _ string, _ Key, _ uint64) ([]byte, error) {
	return s.page, nil
}

func (s *benchService) GetPageBatch(_ context.Context, _ string, _, _, _ uint32, _ uint8, _, nblocks uint32, _ uint64) ([]byte, uint32, error) {
	return s.batch[:int(nblocks)*PageSize], nblocks, nil
}

func (s *benchService) GetRelSize(_ context.Context, _ string, _, _, _ uint32, _ uint8, _ uint64) (uint32, error) {
	return 1024, nil
}

func (s *benchService) GetRelExists(_ context.Context, _ string, _, _, _ uint32, _ uint8, _ uint64) (bool, error) {
	return true, nil
}

func (s *benchService) GetDBSize(_ context.Context, _ string, _, _ uint32, _ uint64) (uint64, error) {
	return uint64(1024 * PageSize), nil
}

func (s *benchService) PutPageBatch(_ context.Context, _ string, _, _, _ uint32, _ uint8, _, _ uint32, _ uint64, _ []byte) error {
	return nil
}

func encodeBenchReqHeader(op uint16, tenantID string, nblocks uint32) []byte {
	buf := make([]byte, ReqHeaderSize)
	binary.LittleEndian.PutUint32(buf[0:4], ProtocolMagic)
	binary.LittleEndian.PutUint16(buf[4:6], ProtocolVersion)
	binary.LittleEndian.PutUint16(buf[6:8], op)
	copy(buf[8:8+TenantIDMaxLen], tenantID)
	binary.LittleEndian.PutUint32(buf[72:76], 1663)
	binary.LittleEndian.PutUint32(buf[76:80], 16384)
	binary.LittleEndian.PutUint32(buf[80:84], 12345)
	binary.LittleEndian.PutUint32(buf[92:96], nblocks)
	binary.LittleEndian.PutUint64(buf[96:104], 0x1000)
	return buf
}

func startBenchUnixServer(b *testing.B) net.Conn {
	b.Helper()
	sockPath := filepath.Join(b.TempDir(), "ps.sock")
	svc := &benchService{
		page:  make([]byte, PageSize),
		batch: make([]byte, 16*PageSize),
	}
	srv := NewUnixServer(sockPath, svc)
	ctx, cancel := context.WithCancel(context.Background())
	if err := srv.Start(ctx); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		cancel()
		_ = srv.Close()
	})
	conn, err := net.Dial("unix", sockPath)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = conn.Close() })
	return conn
}

func BenchmarkAIRPGetPageAtLSN(b *testing.B) {
	conn := startBenchUnixServer(b)
	req := encodeBenchReqHeader(OpGetPageAtLSN, "tenant_bench", 1)
	respHdr := make([]byte, RespHeaderSize)
	pageBuf := make([]byte, PageSize)

	b.SetBytes(int64(PageSize))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := conn.Write(req); err != nil {
			b.Fatal(err)
		}
		if _, err := io.ReadFull(conn, respHdr); err != nil {
			b.Fatal(err)
		}
		if _, err := io.ReadFull(conn, pageBuf); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAIRPGetPageBatch16(b *testing.B) {
	conn := startBenchUnixServer(b)
	req := encodeBenchReqHeader(OpGetPageBatch, "tenant_bench", 16)
	respHdr := make([]byte, RespHeaderSize)
	pagesBuf := make([]byte, 16*PageSize)

	b.SetBytes(int64(16 * PageSize))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := conn.Write(req); err != nil {
			b.Fatal(err)
		}
		if _, err := io.ReadFull(conn, respHdr); err != nil {
			b.Fatal(err)
		}
		if _, err := io.ReadFull(conn, pagesBuf); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAIRPPutPageBatch16(b *testing.B) {
	conn := startBenchUnixServer(b)
	req := encodeBenchReqHeader(OpPutPageBatch, "tenant_bench", 16)
	pagesBuf := make([]byte, 16*PageSize)
	respHdr := make([]byte, RespHeaderSize)

	b.SetBytes(int64(16 * PageSize))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := conn.Write(req); err != nil {
			b.Fatal(err)
		}
		if _, err := conn.Write(pagesBuf); err != nil {
			b.Fatal(err)
		}
		if _, err := io.ReadFull(conn, respHdr); err != nil {
			b.Fatal(err)
		}
	}
}
