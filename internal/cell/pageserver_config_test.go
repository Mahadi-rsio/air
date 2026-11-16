package cell

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestWriteTenantConfigsWithPageserver(t *testing.T) {
	volDir := t.TempDir()
	ag, err := NewAgent(volDir, "/usr/lib/postgresql/17/bin", nil)
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}
	defer ag.Close()

	tenantID := "t_ps_cfg"
	pgData := filepath.Join(volDir, "tenants", "active", tenantID, "pgdata")
	if err := os.MkdirAll(pgData, 0o700); err != nil {
		t.Fatalf("mkdir pgdata: %v", err)
	}

	sockPath := filepath.Join(volDir, "pageserver.sock")
	ag.SetPageserverConfig(true, sockPath, 128)
	if err := ag.WriteTenantConfigs(tenantID); err != nil {
		t.Fatalf("WriteTenantConfigs: %v", err)
	}

	raw, err := os.ReadFile(filepath.Join(pgData, "postgresql.conf"))
	if err != nil {
		t.Fatalf("read postgresql.conf: %v", err)
	}
	conf := string(raw)
	for _, want := range []string{
		"shared_preload_libraries = 'supautils,air_smgr'",
		"air_smgr.enabled = on",
		"air_smgr.pageserver_socket = '" + sockPath + "'",
		"air_smgr.tenant_id = '" + tenantID + "'",
		"air_smgr.lfc_max_mb = 128",
	} {
		if !strings.Contains(conf, want) {
			t.Fatalf("postgresql.conf missing %q:\n%s", want, conf)
		}
	}

	lfcDir := filepath.Join(volDir, "lfc", tenantID)
	if st, err := os.Stat(lfcDir); err != nil || !st.IsDir() {
		t.Fatalf("expected lfc dir %s to exist: %v", lfcDir, err)
	}
}
