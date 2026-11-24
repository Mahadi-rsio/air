package config

import "testing"

func TestLoadMetaDSNFlagAndEnv(t *testing.T) {
	t.Setenv("AIR_META_DSN", "postgres://user:pass@10.0.0.5:5432/air_meta?sslmode=require")
	cfg, err := Load(nil)
	if err != nil {
		t.Fatalf("Load env: %v", err)
	}
	if cfg.MetaDSN != "postgres://user:pass@10.0.0.5:5432/air_meta?sslmode=require" {
		t.Fatalf("unexpected MetaDSN from env: %q", cfg.MetaDSN)
	}

	cfgFlag, err := Load([]string{"--meta-dsn=postgres://ha-pg:5432/meta"})
	if err != nil {
		t.Fatalf("Load flag: %v", err)
	}
	if cfgFlag.MetaDSN != "postgres://ha-pg:5432/meta" {
		t.Fatalf("unexpected MetaDSN from flag: %q", cfgFlag.MetaDSN)
	}
}

func TestDefaults64MBAndR2PrimaryEvict(t *testing.T) {
	cfg := Default("/tmp/air-test")
	if cfg.PageserverCacheMB != 64 {
		t.Fatalf("Default PageserverCacheMB = %d, want 64", cfg.PageserverCacheMB)
	}

	t.Setenv("AIR_R2_PRIMARY", "true")
	loaded, err := Load(nil)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if loaded.PageserverCacheMB != 64 {
		t.Fatalf("Load PageserverCacheMB = %d, want 64", loaded.PageserverCacheMB)
	}
	if !loaded.EvictOnSuspend {
		t.Fatalf("expected EvictOnSuspend=true when AIR_R2_PRIMARY=true")
	}

	t.Setenv("AIR_EVICT_ON_SUSPEND", "false")
	overridden, err := Load(nil)
	if err != nil {
		t.Fatalf("Load with override: %v", err)
	}
	if overridden.EvictOnSuspend {
		t.Fatalf("expected EvictOnSuspend=false when explicitly overridden")
	}
}
