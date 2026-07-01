package config_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/null-ptr-exception/dblog-cdc/internal/config"
)

func TestLoadConfig(t *testing.T) {
	yaml := `
source:
  type: oracle
  dsn: "oracle://user:pass@oracle:1521/FREEPDB1"

target:
  type: yugabytedb
  dsn: "postgres://yugabyte:yugabyte@yb:5433/yugabyte"

cdc:
  host: "olr"
  port: 5000
  scn_type: 4
  format: protobuf

tables:
  - name: "ORDERS"
    chunk_size: 5000
  - name: "CUSTOMERS"

defaults:
  chunk_size: 10000

progress:
  table: "dblog_progress"
`
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	os.WriteFile(path, []byte(yaml), 0644)

	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}

	if cfg.Source.DSN != "oracle://user:pass@oracle:1521/FREEPDB1" {
		t.Errorf("source DSN = %q", cfg.Source.DSN)
	}
	if cfg.CDC.Host != "olr" {
		t.Errorf("cdc host = %q", cfg.CDC.Host)
	}
	if cfg.CDC.Port != 5000 {
		t.Errorf("cdc port = %d", cfg.CDC.Port)
	}
	if len(cfg.Tables) != 2 {
		t.Fatalf("tables count = %d", len(cfg.Tables))
	}
	if cfg.Tables[0].ChunkSize != 5000 {
		t.Errorf("ORDERS chunk_size = %d", cfg.Tables[0].ChunkSize)
	}
	if cfg.Tables[1].ChunkSize != 10000 {
		t.Errorf("CUSTOMERS chunk_size should inherit default, got %d", cfg.Tables[1].ChunkSize)
	}
	if cfg.Progress.Table != "dblog_progress" {
		t.Errorf("progress table = %q", cfg.Progress.Table)
	}
}

func TestLoadConfig_Defaults(t *testing.T) {
	yaml := `
source:
  dsn: "oracle://localhost:1521/XE"
target:
  dsn: "postgres://localhost:5433/yugabyte"
cdc:
  host: "localhost"
  port: 5000
tables:
  - name: "T1"
`
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	os.WriteFile(path, []byte(yaml), 0644)

	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}

	if cfg.Tables[0].ChunkSize != 10000 {
		t.Errorf("default chunk_size = %d, want 10000", cfg.Tables[0].ChunkSize)
	}
	if cfg.Progress.Table != "dblog_progress" {
		t.Errorf("default progress table = %q", cfg.Progress.Table)
	}
}
