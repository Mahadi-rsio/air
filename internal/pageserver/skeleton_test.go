package pageserver

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/Mahadi-rsio/air/internal/crypto"
)

func TestSaveAndRestoreMetadataSkeleton(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()

	kms, err := crypto.NewEnvelopeKMS()
	if err != nil {
		t.Fatalf("NewEnvelopeKMS: %v", err)
	}
	if _, err := kms.EnsureTenantDEK("t_skel"); err != nil {
		t.Fatalf("EnsureTenantDEK: %v", err)
	}

	ps, err := New(Config{
		SocketPath: filepath.Join(tmp, "ps.sock"),
		LayerDir:   filepath.Join(tmp, "layers"),
		KMS:        kms,
	})
	if err != nil {
		t.Fatalf("Pageserver New: %v", err)
	}

	pgData := filepath.Join(tmp, "pgdata")
	mustWrite := func(rel string, data []byte) {
		t.Helper()
		full := filepath.Join(pgData, rel)
		if err := os.MkdirAll(filepath.Dir(full), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, data, 0o600); err != nil {
			t.Fatal(err)
		}
	}

	// Non-relation metadata files.
	mustWrite("PG_VERSION", []byte("17\n"))
	mustWrite("postgresql.conf", []byte("port = 5432\n"))
	mustWrite("global/pg_control", bytes.Repeat([]byte{0x42}, 8192))
	mustWrite("global/pg_filenode.map", bytes.Repeat([]byte{0x11}, 512))
	mustWrite("base/5/PG_VERSION", []byte("17\n"))
	mustWrite("base/5/pg_filenode.map", bytes.Repeat([]byte{0x22}, 512))
	mustWrite("pg_xact/0000", bytes.Repeat([]byte{0x55}, 8192))
	mustWrite("pg_wal/000000010000000000000001", make([]byte, 16*1024*1024))

	// Ephemeral runtime files (should be skipped).
	mustWrite("postmaster.pid", []byte("12345\n"))
	mustWrite("pg_stat_tmp/global.stat", []byte("tmp"))

	// Relation fork files (should be skipped because Pageserver owns them).
	mustWrite("global/1262", bytes.Repeat([]byte{0xaa}, 16384))
	mustWrite("base/5/16385", bytes.Repeat([]byte{0xbb}, 1024*1024))
	mustWrite("base/5/16385_fsm", bytes.Repeat([]byte{0xcc}, 24576))
	mustWrite("base/5/16385_vm", bytes.Repeat([]byte{0xdd}, 8192))
	mustWrite("base/5/16385.1", bytes.Repeat([]byte{0xee}, 8192))

	meta, err := ps.SaveMetadataSkeleton(ctx, "t_skel", 1, pgData)
	if err != nil {
		t.Fatalf("SaveMetadataSkeleton: %v", err)
	}
	if meta.SkippedRelFiles != 5 {
		t.Fatalf("expected 5 skipped relation files, got %d", meta.SkippedRelFiles)
	}
	if meta.CompressedBytes <= 0 || meta.CompressedBytes >= 512*1024 {
		t.Fatalf("expected compressed skeleton < 512 KB, got %d bytes", meta.CompressedBytes)
	}
	if !ps.HasMetadataSkeleton(ctx, "t_skel") {
		t.Fatalf("expected HasMetadataSkeleton to return true")
	}

	// Simulate full local compute eviction and restore into a clean directory.
	if err := os.RemoveAll(pgData); err != nil {
		t.Fatal(err)
	}
	restoredDir := filepath.Join(tmp, "restored_pgdata")
	restoredMeta, err := ps.RestoreMetadataSkeleton(ctx, "t_skel", 1, restoredDir)
	if err != nil {
		t.Fatalf("RestoreMetadataSkeleton: %v", err)
	}
	if restoredMeta.FileCount != meta.FileCount {
		t.Fatalf("file count mismatch: saved %d, restored %d", meta.FileCount, restoredMeta.FileCount)
	}

	// Metadata must exist; relation files and postmaster.pid must NOT exist.
	for _, mustExist := range []string{
		"PG_VERSION",
		"postgresql.conf",
		"global/pg_control",
		"global/pg_filenode.map",
		"base/5/PG_VERSION",
		"base/5/pg_filenode.map",
		"pg_xact/0000",
		"pg_wal/000000010000000000000001",
		"pg_stat_tmp",
	} {
		if _, err := os.Stat(filepath.Join(restoredDir, mustExist)); err != nil {
			t.Fatalf("expected %s to be restored: %v", mustExist, err)
		}
	}
	for _, mustSkip := range []string{
		"postmaster.pid",
		"pg_stat_tmp/global.stat",
		"global/1262",
		"base/5/16385",
		"base/5/16385_fsm",
		"base/5/16385_vm",
		"base/5/16385.1",
	} {
		if _, err := os.Stat(filepath.Join(restoredDir, mustSkip)); !os.IsNotExist(err) {
			t.Fatalf("expected %s to be excluded from skeleton", mustSkip)
		}
	}
}
