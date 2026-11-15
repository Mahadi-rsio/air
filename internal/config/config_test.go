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
