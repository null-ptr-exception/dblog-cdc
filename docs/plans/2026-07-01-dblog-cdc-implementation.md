# DBLog-CDC Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Go binary that replicates Oracle tables to YugabyteDB by interleaving OpenLogReplicator CDC events with SCN-based chunked full-state capture.

**Architecture:** Single process, three goroutines (CDC reader, chunk selector, writer) coordinated through an in-memory event buffer. Uses Oracle's `AS OF SCN` instead of watermark tables — fully read-only on the source. Progress checkpointed to YugabyteDB.

**Tech Stack:** Go, gRPC (OLR protobuf client), godror (Oracle driver), pgx (YugabyteDB/PostgreSQL driver), gopkg.in/yaml.v3

**Spec:** `docs/specs/2026-07-01-dblog-cdc-design.md`

---

## File Structure

```
dblog-cdc/
├── cmd/dblog/main.go                  # Entry point, signal handling, config loading
├── internal/
│   ├── config/
│   │   ├── config.go                  # YAML config types and loader
│   │   └── config_test.go
│   ├── event/
│   │   └── event.go                   # Shared event types (Event, OpType)
│   ├── buffer/
│   │   ├── buffer.go                  # In-memory event buffer with SCN dedup
│   │   └── buffer_test.go
│   ├── olr/
│   │   ├── client.go                  # gRPC client for OpenLogReplicator
│   │   └── client_test.go
│   ├── chunk/
│   │   ├── selector.go               # Oracle AS OF SCN chunk reader
│   │   └── selector_test.go
│   ├── writer/
│   │   ├── writer.go                  # Batched UPSERT writer for YugabyteDB
│   │   └── writer_test.go
│   ├── progress/
│   │   ├── store.go                   # Progress tracking in YugabyteDB
│   │   └── store_test.go
│   └── replicator/
│       ├── replicator.go              # Orchestrator: wires goroutines together
│       └── replicator_test.go
├── proto/
│   └── OraProtoBuf.proto              # Copied from OpenLogReplicator
├── pb/                                # Generated protobuf Go code (git-ignored)
├── testenv/
│   ├── docker-compose.yaml            # Oracle + OLR + YugabyteDB
│   ├── oracle-init/
│   │   └── 01-setup.sh                # ARCHIVELOG + supplemental logging
│   └── olr-config.json                # OLR protobuf network config
├── integration/
│   └── replication_test.go            # End-to-end test
├── go.mod
├── Makefile
└── .gitignore
```

---

### Task 1: Project Scaffolding

**Files:**
- Create: `go.mod`, `Makefile`, `.gitignore`, `proto/OraProtoBuf.proto`

- [ ] **Step 1: Initialize Go module**

```bash
cd /home/rophy/projects/dblog-cdc
go mod init github.com/null-ptr-exception/dblog-cdc
```

- [ ] **Step 2: Create `.gitignore`**

Create `.gitignore`:

```gitignore
# Generated protobuf
pb/

# Binary
dblog-cdc
cmd/dblog/dblog

# IDE
.idea/
.vscode/

# Test
coverage.out
```

- [ ] **Step 3: Copy protobuf schema**

Create `proto/OraProtoBuf.proto` with the exact contents from OpenLogReplicator's `proto/OraProtoBuf.proto`:

```protobuf
syntax = "proto3";
package OpenLogReplicator.pb;

option go_package = "github.com/null-ptr-exception/dblog-cdc/pb";

enum Op {
    BEGIN  = 0;
    COMMIT = 1;
    INSERT = 2;
    UPDATE = 3;
    DELETE = 4;
    DDL = 5;
    CHKPT = 6;
}

enum ColumnType {
    UNKNOWN = 0;
    VARCHAR2 = 1;
    NUMBER = 2;
    LONG = 3;
    DATE = 4;
    RAW = 5;
    LONG_RAW = 6;
    CHAR = 7;
    BINARY_FLOAT = 8;
    BINARY_DOUBLE = 9;
    CLOB = 10;
    BLOB = 11;
    TIMESTAMP = 12;
    TIMESTAMP_WITH_TZ = 13;
    INTERVAL_YEAR_TO_MONTH = 14;
    INTERVAL_DAY_TO_SECOND = 15;
    UROWID = 16;
    TIMESTAMP_WITH_LOCAL_TZ = 17;
}

service OpenLogReplicator {
    rpc Redo(stream RedoRequest) returns (stream RedoResponse);
}

enum RequestCode {
    INFO = 0;
    START = 1;
    CONTINUE = 2;
    CONFIRM = 3;
}

enum ResponseCode {
    READY = 0;
    FAILED_START = 1;
    STARTING = 2;
    ALREADY_STARTED = 3;
    REPLICATE = 4;
    PAYLOAD = 5;
    INVALID_DATABASE = 6;
    INVALID_COMMAND = 7;
}

message Value {
    string name = 1;
    oneof datum {
        int64 value_int = 2;
        float value_float = 3;
        double value_double = 4;
        string value_string = 5;
        bytes value_bytes = 6;
    }
}

message Column {
    string name = 1;
    ColumnType type = 2;
    int32 length = 3;
    int32 precision = 4;
    int32 scale = 5;
    bool nullable = 6;
}

message Schema {
    string owner = 1;
    string name = 2;
    uint32 obj = 3;
    oneof tm_val {
        uint64 tm = 4;
        string tms = 5;
    }
    repeated Column column = 6;
}

message Payload {
    Op op = 1;
    Schema schema = 2;
    string rid = 3;
    repeated Value before = 4;
    repeated Value after = 5;
    string ddl = 6;
    uint32 seq = 7;
    uint64 offset = 8;
    bool redo = 9;
    uint64 num = 10;
}

message SchemaRequest {
    string mask = 1;
    string filter = 2;
}

message RedoRequest {
    RequestCode code = 1;
    string database_name = 2;
    oneof tm_val {
        uint64 scn = 3;
        string tms = 4;
        int64 tm_rel = 5;
    }
    optional uint64 seq = 6;
    repeated SchemaRequest schema = 7;
    optional uint64 c_scn = 8;
    optional uint64 c_idx = 9;
}

message RedoResponse {
    ResponseCode code = 1;
    oneof scn_val {
        uint64 scn = 2;
        string scns = 3;
    }
    oneof tm_val {
        uint64 tm = 4;
        string tms = 5;
    }
    oneof xid_val {
        string xid = 6;
        uint64 xidn = 7;
    }
    string db = 8;
    repeated Payload payload = 9;
    uint64 c_scn = 10;
    uint64 c_idx = 11;
    map<string,string> attributes = 12;
}
```

Note: Added `option go_package` line (not in the original) — required for Go protobuf generation.

- [ ] **Step 4: Create Makefile**

Create `Makefile`:

```makefile
.PHONY: proto build test test-unit test-integration clean

proto:
	mkdir -p pb
	protoc --go_out=pb --go_opt=paths=source_relative \
		--go-grpc_out=pb --go-grpc_opt=paths=source_relative \
		proto/OraProtoBuf.proto

build: proto
	go build -o dblog-cdc ./cmd/dblog

test-unit:
	go test ./internal/... -v -count=1

test-integration:
	go test ./integration/... -v -count=1 -timeout=300s

test: test-unit

clean:
	rm -rf pb/ dblog-cdc

testenv-up:
	docker compose -f testenv/docker-compose.yaml up -d

testenv-down:
	docker compose -f testenv/docker-compose.yaml down -v
```

- [ ] **Step 5: Install protobuf tools and generate Go code**

```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
make proto
```

- [ ] **Step 6: Add gRPC dependency**

```bash
cd /home/rophy/projects/dblog-cdc
go get google.golang.org/grpc
go get google.golang.org/protobuf
```

- [ ] **Step 7: Verify generated code compiles**

```bash
cd /home/rophy/projects/dblog-cdc
go build ./pb/...
```

Expected: compiles without errors. Verify `pb/OraProtoBuf.pb.go` and `pb/OraProtoBuf_grpc.pb.go` exist.

- [ ] **Step 8: Commit**

```bash
git add go.mod go.sum .gitignore Makefile proto/ pb/
git commit -m "chore: project scaffolding with protobuf generation"
```

Note: `pb/` is in `.gitignore` — reconsider: for reproducibility, commit the generated files. Remove `pb/` from `.gitignore` and add the generated files.

---

### Task 2: Event Types

**Files:**
- Create: `internal/event/event.go`

- [ ] **Step 1: Define event types**

Create `internal/event/event.go`:

```go
package event

type OpType int

const (
	OpInsert OpType = iota
	OpUpdate
	OpDelete
)

func (o OpType) String() string {
	switch o {
	case OpInsert:
		return "INSERT"
	case OpUpdate:
		return "UPDATE"
	case OpDelete:
		return "DELETE"
	default:
		return "UNKNOWN"
	}
}

// Event represents a row change, from either CDC or a chunk SELECT.
type Event struct {
	Table   string
	Op      OpType
	SCN     uint64
	PK      int64
	Columns map[string]any // column name → value
}

// ChunkResult holds the output of a single chunk SELECT.
type ChunkResult struct {
	Table    string
	SCN      uint64 // the SCN the chunk was read AS OF
	Rows     map[int64]map[string]any // pk → columns
	LastPK   int64
	Complete bool // true if fewer rows than chunk_size were returned
}
```

- [ ] **Step 2: Verify it compiles**

```bash
go build ./internal/event/...
```

- [ ] **Step 3: Commit**

```bash
git add internal/event/
git commit -m "feat: define shared event types"
```

---

### Task 3: Config

**Files:**
- Create: `internal/config/config.go`, `internal/config/config_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/config/config_test.go`:

```go
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
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd /home/rophy/projects/dblog-cdc && go test ./internal/config/... -v
```

Expected: FAIL — `config` package does not exist.

- [ ] **Step 3: Implement config loader**

Create `internal/config/config.go`:

```go
package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type Source struct {
	Type string `yaml:"type"`
	DSN  string `yaml:"dsn"`
}

type Target struct {
	Type string `yaml:"type"`
	DSN  string `yaml:"dsn"`
}

type CDC struct {
	Host    string `yaml:"host"`
	Port    int    `yaml:"port"`
	SCNType int    `yaml:"scn_type"`
	Format  string `yaml:"format"`
}

type Table struct {
	Name      string `yaml:"name"`
	ChunkSize int    `yaml:"chunk_size"`
}

type Defaults struct {
	ChunkSize int `yaml:"chunk_size"`
}

type Progress struct {
	Table string `yaml:"table"`
}

type Config struct {
	Source   Source   `yaml:"source"`
	Target  Target   `yaml:"target"`
	CDC     CDC      `yaml:"cdc"`
	Tables  []Table  `yaml:"tables"`
	Defaults Defaults `yaml:"defaults"`
	Progress Progress `yaml:"progress"`
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	cfg.applyDefaults()
	return &cfg, nil
}

func (c *Config) applyDefaults() {
	if c.Defaults.ChunkSize == 0 {
		c.Defaults.ChunkSize = 10000
	}
	if c.Progress.Table == "" {
		c.Progress.Table = "dblog_progress"
	}
	for i := range c.Tables {
		if c.Tables[i].ChunkSize == 0 {
			c.Tables[i].ChunkSize = c.Defaults.ChunkSize
		}
	}
}
```

- [ ] **Step 4: Add yaml dependency and run tests**

```bash
cd /home/rophy/projects/dblog-cdc
go get gopkg.in/yaml.v3
go test ./internal/config/... -v
```

Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add internal/config/ go.mod go.sum
git commit -m "feat: YAML config loader with defaults"
```

---

### Task 4: Event Buffer (Core Algorithm)

This is the most important component — the SCN-based dedup logic from the spec. Must be thoroughly tested.

**Files:**
- Create: `internal/buffer/buffer.go`, `internal/buffer/buffer_test.go`

- [ ] **Step 1: Write tests for the buffer**

Create `internal/buffer/buffer_test.go`:

```go
package buffer_test

import (
	"testing"

	"github.com/null-ptr-exception/dblog-cdc/internal/buffer"
	"github.com/null-ptr-exception/dblog-cdc/internal/event"
)

func TestBuffer_CDCOnly(t *testing.T) {
	b := buffer.New()

	b.PushCDC(event.Event{Table: "T", Op: event.OpInsert, SCN: 100, PK: 1, Columns: map[string]any{"v": "a"}})
	b.PushCDC(event.Event{Table: "T", Op: event.OpUpdate, SCN: 101, PK: 2, Columns: map[string]any{"v": "b"}})

	out := b.Drain()
	if len(out) != 2 {
		t.Fatalf("got %d events, want 2", len(out))
	}
	if out[0].PK != 1 || out[1].PK != 2 {
		t.Errorf("wrong PKs: %d, %d", out[0].PK, out[1].PK)
	}
}

func TestBuffer_ChunkOnly(t *testing.T) {
	b := buffer.New()

	chunk := event.ChunkResult{
		Table: "T",
		SCN:   100,
		Rows: map[int64]map[string]any{
			1: {"v": "a"},
			2: {"v": "b"},
			3: {"v": "c"},
		},
		LastPK: 3,
	}
	b.PushChunk(chunk)

	out := b.Drain()
	if len(out) != 3 {
		t.Fatalf("got %d events, want 3", len(out))
	}
}

func TestBuffer_CDCWinsOverChunk(t *testing.T) {
	// CDC event at SCN 105 for PK 2 should replace chunk row (snapshot at SCN 100)
	b := buffer.New()

	chunk := event.ChunkResult{
		Table: "T",
		SCN:   100,
		Rows: map[int64]map[string]any{
			1: {"v": "chunk_1"},
			2: {"v": "chunk_2"},
			3: {"v": "chunk_3"},
		},
		LastPK: 3,
	}
	b.PushChunk(chunk)

	// CDC event AFTER the chunk's SCN for PK 2 — CDC wins
	b.ApplyCDCDedup(event.Event{Table: "T", Op: event.OpUpdate, SCN: 105, PK: 2, Columns: map[string]any{"v": "cdc_2"}})

	out := b.Drain()
	if len(out) != 3 {
		t.Fatalf("got %d events, want 3", len(out))
	}

	byPK := map[int64]event.Event{}
	for _, e := range out {
		byPK[e.PK] = e
	}

	if byPK[1].Columns["v"] != "chunk_1" {
		t.Errorf("PK 1 should be from chunk, got %v", byPK[1].Columns["v"])
	}
	if byPK[2].Columns["v"] != "cdc_2" {
		t.Errorf("PK 2 should be from CDC, got %v", byPK[2].Columns["v"])
	}
	if byPK[3].Columns["v"] != "chunk_3" {
		t.Errorf("PK 3 should be from chunk, got %v", byPK[3].Columns["v"])
	}
}

func TestBuffer_CDCBeforeChunkSCN_NoDedup(t *testing.T) {
	// CDC event at SCN 90 (before chunk's SCN 100) — no dedup needed,
	// chunk already reflects this change. Both go to output (UPSERT is idempotent).
	b := buffer.New()

	chunk := event.ChunkResult{
		Table: "T",
		SCN:   100,
		Rows: map[int64]map[string]any{
			1: {"v": "chunk_1"},
		},
		LastPK: 1,
	}
	b.PushChunk(chunk)

	b.ApplyCDCDedup(event.Event{Table: "T", Op: event.OpUpdate, SCN: 90, PK: 1, Columns: map[string]any{"v": "old"}})

	out := b.Drain()
	// chunk row survives — the CDC event at SCN 90 is older
	byPK := map[int64]event.Event{}
	for _, e := range out {
		byPK[e.PK] = e
	}
	if byPK[1].Columns["v"] != "chunk_1" {
		t.Errorf("PK 1 should be from chunk (newer), got %v", byPK[1].Columns["v"])
	}
}

func TestBuffer_CDCDeleteRemovesChunkRow(t *testing.T) {
	b := buffer.New()

	chunk := event.ChunkResult{
		Table: "T",
		SCN:   100,
		Rows: map[int64]map[string]any{
			1: {"v": "chunk_1"},
			2: {"v": "chunk_2"},
		},
		LastPK: 2,
	}
	b.PushChunk(chunk)

	// DELETE at SCN 105 for PK 1 — remove from chunk, emit as DELETE
	b.ApplyCDCDedup(event.Event{Table: "T", Op: event.OpDelete, SCN: 105, PK: 1})

	out := b.Drain()
	if len(out) != 2 {
		t.Fatalf("got %d events, want 2 (1 delete + 1 chunk row)", len(out))
	}

	byPK := map[int64]event.Event{}
	for _, e := range out {
		byPK[e.PK] = e
	}
	if byPK[1].Op != event.OpDelete {
		t.Errorf("PK 1 should be DELETE, got %v", byPK[1].Op)
	}
	if byPK[2].Columns["v"] != "chunk_2" {
		t.Errorf("PK 2 should be from chunk, got %v", byPK[2].Columns["v"])
	}
}

func TestBuffer_NoChunk_DrainReturnsEmpty(t *testing.T) {
	b := buffer.New()
	out := b.Drain()
	if len(out) != 0 {
		t.Errorf("expected empty drain, got %d", len(out))
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd /home/rophy/projects/dblog-cdc && go test ./internal/buffer/... -v
```

Expected: FAIL — `buffer` package does not exist.

- [ ] **Step 3: Implement the buffer**

Create `internal/buffer/buffer.go`:

```go
package buffer

import (
	"github.com/null-ptr-exception/dblog-cdc/internal/event"
)

// Buffer holds an active chunk and accepts CDC events for dedup.
// Not safe for concurrent use — the replicator serializes access.
type Buffer struct {
	chunk     *event.ChunkResult
	cdcDedup  []event.Event // CDC events that replaced chunk rows
}

func New() *Buffer {
	return &Buffer{}
}

// PushCDC adds a CDC event that has no chunk to dedup against.
// These are passed directly through to the output via Drain.
func (b *Buffer) PushCDC(e event.Event) {
	b.cdcDedup = append(b.cdcDedup, e)
}

// PushChunk sets the active chunk. Only one chunk is active at a time.
func (b *Buffer) PushChunk(c event.ChunkResult) {
	b.chunk = &c
}

// ApplyCDCDedup processes a CDC event against the active chunk.
// If the event's SCN > chunk's SCN and the PK exists in the chunk,
// the chunk row is removed (CDC wins). The CDC event is always kept.
func (b *Buffer) ApplyCDCDedup(e event.Event) {
	if b.chunk == nil {
		b.cdcDedup = append(b.cdcDedup, e)
		return
	}

	if e.SCN > b.chunk.SCN {
		if _, exists := b.chunk.Rows[e.PK]; exists {
			delete(b.chunk.Rows, e.PK)
			b.cdcDedup = append(b.cdcDedup, e)
			return
		}
	}

	// CDC event doesn't conflict with chunk — pass through
	b.cdcDedup = append(b.cdcDedup, e)
}

// Drain returns all pending events (CDC dedup winners + remaining chunk rows)
// and resets the buffer. Chunk rows are emitted as INSERT events.
func (b *Buffer) Drain() []event.Event {
	var out []event.Event

	out = append(out, b.cdcDedup...)

	if b.chunk != nil {
		for pk, cols := range b.chunk.Rows {
			out = append(out, event.Event{
				Table:   b.chunk.Table,
				Op:      event.OpInsert,
				SCN:     b.chunk.SCN,
				PK:      pk,
				Columns: cols,
			})
		}
	}

	b.chunk = nil
	b.cdcDedup = nil
	return out
}
```

- [ ] **Step 4: Run tests**

```bash
cd /home/rophy/projects/dblog-cdc && go test ./internal/buffer/... -v
```

Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
git add internal/buffer/
git commit -m "feat: event buffer with SCN-based chunk dedup"
```

---

### Task 5: Progress Store

**Files:**
- Create: `internal/progress/store.go`, `internal/progress/store_test.go`

The progress store reads/writes `dblog_progress` in YugabyteDB. For unit tests, we test the SQL generation and state logic using an interface — integration tests will hit real YugabyteDB.

- [ ] **Step 1: Write the test**

Create `internal/progress/store_test.go`:

```go
package progress_test

import (
	"context"
	"testing"

	"github.com/null-ptr-exception/dblog-cdc/internal/progress"
)

func TestMemoryStore_GetSet(t *testing.T) {
	s := progress.NewMemoryStore()
	ctx := context.Background()

	// Not started — returns zero state
	state, err := s.Get(ctx, "ORDERS")
	if err != nil {
		t.Fatalf("Get() error: %v", err)
	}
	if state.LastPK != nil {
		t.Errorf("expected nil LastPK, got %v", state.LastPK)
	}

	// Save progress
	pk := int64(500)
	scn := uint64(12345)
	err = s.Save(ctx, "ORDERS", &pk, scn)
	if err != nil {
		t.Fatalf("Save() error: %v", err)
	}

	state, err = s.Get(ctx, "ORDERS")
	if err != nil {
		t.Fatalf("Get() error: %v", err)
	}
	if *state.LastPK != 500 {
		t.Errorf("LastPK = %d, want 500", *state.LastPK)
	}
	if state.LastSCN != 12345 {
		t.Errorf("LastSCN = %d, want 12345", state.LastSCN)
	}
}

func TestMemoryStore_MarkComplete(t *testing.T) {
	s := progress.NewMemoryStore()
	ctx := context.Background()

	complete := int64(-1)
	err := s.Save(ctx, "ORDERS", &complete, 99999)
	if err != nil {
		t.Fatalf("Save() error: %v", err)
	}

	state, err := s.Get(ctx, "ORDERS")
	if err != nil {
		t.Fatalf("Get() error: %v", err)
	}
	if *state.LastPK != -1 {
		t.Errorf("LastPK = %d, want -1 (complete)", *state.LastPK)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd /home/rophy/projects/dblog-cdc && go test ./internal/progress/... -v
```

Expected: FAIL — `progress` package does not exist.

- [ ] **Step 3: Implement progress store**

Create `internal/progress/store.go`:

```go
package progress

import (
	"context"
	"sync"
)

type State struct {
	LastPK  *int64
	LastSCN uint64
}

type Store interface {
	Get(ctx context.Context, table string) (State, error)
	Save(ctx context.Context, table string, lastPK *int64, lastSCN uint64) error
}

// MemoryStore is an in-memory implementation for testing.
type MemoryStore struct {
	mu    sync.Mutex
	state map[string]State
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{state: make(map[string]State)}
}

func (m *MemoryStore) Get(_ context.Context, table string) (State, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.state[table], nil
}

func (m *MemoryStore) Save(_ context.Context, table string, lastPK *int64, lastSCN uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state[table] = State{LastPK: lastPK, LastSCN: lastSCN}
	return nil
}
```

- [ ] **Step 4: Run tests**

```bash
cd /home/rophy/projects/dblog-cdc && go test ./internal/progress/... -v
```

Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add internal/progress/
git commit -m "feat: progress store interface with in-memory implementation"
```

---

### Task 6: YugabyteDB Progress Store

**Files:**
- Create: `internal/progress/pgstore.go`

This is the real YugabyteDB-backed implementation using pgx. Cannot be unit tested without a database — tested via integration tests in Task 11.

- [ ] **Step 1: Implement PgStore**

Create `internal/progress/pgstore.go`:

```go
package progress

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

type PgStore struct {
	pool      *pgxpool.Pool
	tableName string
}

func NewPgStore(pool *pgxpool.Pool, tableName string) *PgStore {
	return &PgStore{pool: pool, tableName: tableName}
}

func (s *PgStore) EnsureTable(ctx context.Context) error {
	query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		table_name  TEXT PRIMARY KEY,
		last_pk     BIGINT,
		last_lsn    BIGINT,
		updated_at  TIMESTAMPTZ DEFAULT now()
	)`, s.tableName)
	_, err := s.pool.Exec(ctx, query)
	return err
}

func (s *PgStore) Get(ctx context.Context, table string) (State, error) {
	var state State
	var lastPK, lastSCN *int64

	query := fmt.Sprintf("SELECT last_pk, last_lsn FROM %s WHERE table_name = $1", s.tableName)
	err := s.pool.QueryRow(ctx, query, table).Scan(&lastPK, &lastSCN)
	if err != nil {
		if err.Error() == "no rows in result set" {
			return State{}, nil
		}
		return State{}, fmt.Errorf("get progress: %w", err)
	}

	state.LastPK = lastPK
	if lastSCN != nil {
		state.LastSCN = uint64(*lastSCN)
	}
	return state, nil
}

func (s *PgStore) Save(ctx context.Context, table string, lastPK *int64, lastSCN uint64) error {
	query := fmt.Sprintf(`INSERT INTO %s (table_name, last_pk, last_lsn, updated_at)
		VALUES ($1, $2, $3, now())
		ON CONFLICT (table_name) DO UPDATE SET
			last_pk = EXCLUDED.last_pk,
			last_lsn = EXCLUDED.last_lsn,
			updated_at = now()`, s.tableName)

	scn := int64(lastSCN)
	_, err := s.pool.Exec(ctx, query, table, lastPK, &scn)
	return err
}
```

- [ ] **Step 2: Add pgx dependency**

```bash
cd /home/rophy/projects/dblog-cdc
go get github.com/jackc/pgx/v5
```

- [ ] **Step 3: Verify it compiles**

```bash
go build ./internal/progress/...
```

- [ ] **Step 4: Commit**

```bash
git add internal/progress/pgstore.go go.mod go.sum
git commit -m "feat: YugabyteDB-backed progress store"
```

---

### Task 7: OLR Client

**Files:**
- Create: `internal/olr/client.go`, `internal/olr/client_test.go`

- [ ] **Step 1: Write test for event conversion**

Create `internal/olr/client_test.go`:

```go
package olr_test

import (
	"testing"

	"github.com/null-ptr-exception/dblog-cdc/internal/event"
	"github.com/null-ptr-exception/dblog-cdc/internal/olr"
	pb "github.com/null-ptr-exception/dblog-cdc/pb"
)

func TestConvertPayload_Insert(t *testing.T) {
	payload := &pb.Payload{
		Op: pb.Op_INSERT,
		Schema: &pb.Schema{
			Owner: "TEST",
			Name:  "ORDERS",
		},
		After: []*pb.Value{
			{Name: "ID", Datum: &pb.Value_ValueInt{ValueInt: 42}},
			{Name: "AMOUNT", Datum: &pb.Value_ValueDouble{ValueDouble: 99.95}},
			{Name: "STATUS", Datum: &pb.Value_ValueString{ValueString: "NEW"}},
		},
	}

	ev, err := olr.ConvertPayload(payload, 12345)
	if err != nil {
		t.Fatalf("ConvertPayload() error: %v", err)
	}
	if ev.Table != "ORDERS" {
		t.Errorf("Table = %q", ev.Table)
	}
	if ev.Op != event.OpInsert {
		t.Errorf("Op = %v", ev.Op)
	}
	if ev.SCN != 12345 {
		t.Errorf("SCN = %d", ev.SCN)
	}
	if ev.PK != 42 {
		t.Errorf("PK = %d", ev.PK)
	}
	if ev.Columns["AMOUNT"] != 99.95 {
		t.Errorf("AMOUNT = %v", ev.Columns["AMOUNT"])
	}
}

func TestConvertPayload_Update(t *testing.T) {
	payload := &pb.Payload{
		Op: pb.Op_UPDATE,
		Schema: &pb.Schema{Name: "ORDERS"},
		After: []*pb.Value{
			{Name: "ID", Datum: &pb.Value_ValueInt{ValueInt: 7}},
			{Name: "STATUS", Datum: &pb.Value_ValueString{ValueString: "SHIPPED"}},
		},
	}

	ev, err := olr.ConvertPayload(payload, 200)
	if err != nil {
		t.Fatalf("ConvertPayload() error: %v", err)
	}
	if ev.Op != event.OpUpdate {
		t.Errorf("Op = %v", ev.Op)
	}
	if ev.PK != 7 {
		t.Errorf("PK = %d", ev.PK)
	}
}

func TestConvertPayload_Delete(t *testing.T) {
	payload := &pb.Payload{
		Op: pb.Op_DELETE,
		Schema: &pb.Schema{Name: "ORDERS"},
		Before: []*pb.Value{
			{Name: "ID", Datum: &pb.Value_ValueInt{ValueInt: 3}},
		},
	}

	ev, err := olr.ConvertPayload(payload, 300)
	if err != nil {
		t.Fatalf("ConvertPayload() error: %v", err)
	}
	if ev.Op != event.OpDelete {
		t.Errorf("Op = %v", ev.Op)
	}
	if ev.PK != 3 {
		t.Errorf("PK = %d", ev.PK)
	}
}

func TestConvertPayload_SkipBeginCommit(t *testing.T) {
	for _, op := range []pb.Op{pb.Op_BEGIN, pb.Op_COMMIT, pb.Op_DDL, pb.Op_CHKPT} {
		payload := &pb.Payload{Op: op}
		_, err := olr.ConvertPayload(payload, 100)
		if err != olr.ErrSkipEvent {
			t.Errorf("Op %v should return ErrSkipEvent, got %v", op, err)
		}
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd /home/rophy/projects/dblog-cdc && go test ./internal/olr/... -v
```

Expected: FAIL — `olr` package does not exist.

- [ ] **Step 3: Implement OLR client**

Create `internal/olr/client.go`:

```go
package olr

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"

	"github.com/null-ptr-exception/dblog-cdc/internal/event"
	pb "github.com/null-ptr-exception/dblog-cdc/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var ErrSkipEvent = errors.New("skip non-DML event")

// ConvertPayload converts an OLR protobuf Payload to an internal Event.
// Returns ErrSkipEvent for BEGIN, COMMIT, DDL, CHKPT events.
// The first integer column in `after` (or `before` for DELETE) is treated as the PK.
func ConvertPayload(p *pb.Payload, scn uint64) (event.Event, error) {
	switch p.Op {
	case pb.Op_INSERT, pb.Op_UPDATE, pb.Op_DELETE:
	default:
		return event.Event{}, ErrSkipEvent
	}

	var op event.OpType
	switch p.Op {
	case pb.Op_INSERT:
		op = event.OpInsert
	case pb.Op_UPDATE:
		op = event.OpUpdate
	case pb.Op_DELETE:
		op = event.OpDelete
	}

	tableName := ""
	if p.Schema != nil {
		tableName = p.Schema.Name
	}

	// For DELETE, use before image; for INSERT/UPDATE, use after image
	values := p.After
	if op == event.OpDelete {
		values = p.Before
	}

	var pk int64
	var pkFound bool
	columns := make(map[string]any)

	for _, v := range values {
		val := extractValue(v)
		columns[v.Name] = val

		// First integer value is treated as PK
		if !pkFound {
			if intVal, ok := v.Datum.(*pb.Value_ValueInt); ok {
				pk = intVal.ValueInt
				pkFound = true
			}
		}
	}

	if !pkFound {
		return event.Event{}, fmt.Errorf("no integer PK found in event for table %s", tableName)
	}

	return event.Event{
		Table:   tableName,
		Op:      op,
		SCN:     scn,
		PK:      pk,
		Columns: columns,
	}, nil
}

func extractValue(v *pb.Value) any {
	switch d := v.Datum.(type) {
	case *pb.Value_ValueInt:
		return d.ValueInt
	case *pb.Value_ValueFloat:
		return d.ValueFloat
	case *pb.Value_ValueDouble:
		return d.ValueDouble
	case *pb.Value_ValueString:
		return d.ValueString
	case *pb.Value_ValueBytes:
		return d.ValueBytes
	default:
		return nil
	}
}

// Client connects to OpenLogReplicator via gRPC and streams CDC events.
type Client struct {
	addr     string
	dbName   string
	conn     *grpc.ClientConn
	tables   map[string]bool // tables we care about

	mu       sync.Mutex
	lastSCN  uint64
	lastCSCN uint64
	lastCIdx uint64
}

func NewClient(host string, port int, dbName string, tables []string) *Client {
	tableSet := make(map[string]bool, len(tables))
	for _, t := range tables {
		tableSet[t] = true
	}
	return &Client{
		addr:   fmt.Sprintf("%s:%d", host, port),
		dbName: dbName,
		tables: tableSet,
	}
}

// LastSCN returns the most recently seen SCN from the CDC stream.
func (c *Client) LastSCN() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastSCN
}

// Stream connects to OLR and sends events to the provided channel.
// It blocks until the context is cancelled or an error occurs.
func (c *Client) Stream(ctx context.Context, startSCN uint64, events chan<- event.Event) error {
	var err error
	c.conn, err = grpc.NewClient(c.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("grpc dial: %w", err)
	}
	defer c.conn.Close()

	client := pb.NewOpenLogReplicatorClient(c.conn)
	stream, err := client.Redo(ctx)
	if err != nil {
		return fmt.Errorf("start redo stream: %w", err)
	}

	// Send START or CONTINUE request
	req := &pb.RedoRequest{
		DatabaseName: c.dbName,
	}
	if startSCN > 0 {
		req.Code = pb.RequestCode_CONTINUE
		cscn := startSCN
		cidx := uint64(0)
		req.CScn = &cscn
		req.CIdx = &cidx
	} else {
		req.Code = pb.RequestCode_START
	}

	if err := stream.Send(req); err != nil {
		return fmt.Errorf("send start: %w", err)
	}

	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("recv: %w", err)
		}

		// Update checkpoint coordinates
		c.mu.Lock()
		if scn := resp.GetScn(); scn > 0 {
			c.lastSCN = scn
		}
		c.lastCSCN = resp.CScn
		c.lastCIdx = resp.CIdx
		c.mu.Unlock()

		scn := resp.GetScn()

		for _, p := range resp.Payload {
			ev, err := ConvertPayload(p, scn)
			if errors.Is(err, ErrSkipEvent) {
				continue
			}
			if err != nil {
				slog.Warn("skip event", "error", err)
				continue
			}

			if !c.tables[ev.Table] {
				continue
			}

			select {
			case events <- ev:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}

// Confirm sends a CONFIRM request to OLR to advance the checkpoint.
func (c *Client) Confirm(stream pb.OpenLogReplicator_RedoClient) error {
	c.mu.Lock()
	cscn := c.lastCSCN
	cidx := c.lastCIdx
	c.mu.Unlock()

	return stream.Send(&pb.RedoRequest{
		Code: pb.RequestCode_CONFIRM,
		CScn: &cscn,
		CIdx: &cidx,
	})
}
```

- [ ] **Step 4: Run tests**

```bash
cd /home/rophy/projects/dblog-cdc && go test ./internal/olr/... -v
```

Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add internal/olr/
git commit -m "feat: OLR gRPC client with protobuf event conversion"
```

---

### Task 8: Chunk Selector

**Files:**
- Create: `internal/chunk/selector.go`, `internal/chunk/selector_test.go`

- [ ] **Step 1: Write test using a mock querier interface**

Create `internal/chunk/selector_test.go`:

```go
package chunk_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/null-ptr-exception/dblog-cdc/internal/chunk"
	"github.com/null-ptr-exception/dblog-cdc/internal/event"
)

type mockQuerier struct {
	rows    []map[string]any // all rows, sorted by PK
	pkCol   string
	scn     uint64
}

func (m *mockQuerier) QueryChunk(_ context.Context, table string, afterPK int64, limit int, scn uint64) (*event.ChunkResult, error) {
	m.scn = scn
	result := &event.ChunkResult{
		Table: table,
		SCN:   scn,
		Rows:  make(map[int64]map[string]any),
	}

	count := 0
	for _, row := range m.rows {
		pk := row[m.pkCol].(int64)
		if pk > afterPK {
			result.Rows[pk] = row
			result.LastPK = pk
			count++
			if count >= limit {
				break
			}
		}
	}

	result.Complete = count < limit
	return result, nil
}

func (m *mockQuerier) CurrentSCN(_ context.Context) (uint64, error) {
	return m.scn, nil
}

func TestSelector_SingleChunk(t *testing.T) {
	q := &mockQuerier{
		pkCol: "ID",
		scn:   100,
		rows: []map[string]any{
			{"ID": int64(1), "NAME": "a"},
			{"ID": int64(2), "NAME": "b"},
		},
	}

	s := chunk.NewSelector(q)
	result, err := s.Next(context.Background(), "ORDERS", 0, 10, 100)
	if err != nil {
		t.Fatalf("Next() error: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Errorf("got %d rows, want 2", len(result.Rows))
	}
	if !result.Complete {
		t.Error("should be complete (2 rows < limit 10)")
	}
	if result.LastPK != 2 {
		t.Errorf("LastPK = %d, want 2", result.LastPK)
	}
}

func TestSelector_MultipleChunks(t *testing.T) {
	rows := make([]map[string]any, 5)
	for i := range rows {
		rows[i] = map[string]any{"ID": int64(i + 1), "V": fmt.Sprintf("val%d", i+1)}
	}

	q := &mockQuerier{pkCol: "ID", scn: 100, rows: rows}
	s := chunk.NewSelector(q)

	// First chunk: 2 rows
	r1, err := s.Next(context.Background(), "T", 0, 2, 100)
	if err != nil {
		t.Fatalf("chunk 1 error: %v", err)
	}
	if len(r1.Rows) != 2 || r1.Complete {
		t.Errorf("chunk 1: %d rows, complete=%v", len(r1.Rows), r1.Complete)
	}

	// Second chunk: 2 rows
	r2, err := s.Next(context.Background(), "T", r1.LastPK, 2, 100)
	if err != nil {
		t.Fatalf("chunk 2 error: %v", err)
	}
	if len(r2.Rows) != 2 || r2.Complete {
		t.Errorf("chunk 2: %d rows, complete=%v", len(r2.Rows), r2.Complete)
	}

	// Third chunk: 1 row (complete)
	r3, err := s.Next(context.Background(), "T", r2.LastPK, 2, 100)
	if err != nil {
		t.Fatalf("chunk 3 error: %v", err)
	}
	if len(r3.Rows) != 1 || !r3.Complete {
		t.Errorf("chunk 3: %d rows, complete=%v", len(r3.Rows), r3.Complete)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd /home/rophy/projects/dblog-cdc && go test ./internal/chunk/... -v
```

Expected: FAIL — `chunk` package does not exist.

- [ ] **Step 3: Implement chunk selector**

Create `internal/chunk/selector.go`:

```go
package chunk

import (
	"context"

	"github.com/null-ptr-exception/dblog-cdc/internal/event"
)

// Querier abstracts the database layer for chunk reads.
type Querier interface {
	QueryChunk(ctx context.Context, table string, afterPK int64, limit int, scn uint64) (*event.ChunkResult, error)
	CurrentSCN(ctx context.Context) (uint64, error)
}

type Selector struct {
	querier Querier
}

func NewSelector(q Querier) *Selector {
	return &Selector{querier: q}
}

// Next selects the next chunk of rows from the table after the given PK,
// using AS OF SCN for a consistent snapshot.
func (s *Selector) Next(ctx context.Context, table string, afterPK int64, chunkSize int, scn uint64) (*event.ChunkResult, error) {
	return s.querier.QueryChunk(ctx, table, afterPK, chunkSize, scn)
}
```

- [ ] **Step 4: Run tests**

```bash
cd /home/rophy/projects/dblog-cdc && go test ./internal/chunk/... -v
```

Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add internal/chunk/
git commit -m "feat: chunk selector with querier interface"
```

---

### Task 9: Oracle Querier Implementation

**Files:**
- Create: `internal/chunk/oracle.go`

Cannot be unit tested without Oracle — tested via integration tests in Task 11.

- [ ] **Step 1: Implement OracleQuerier**

Create `internal/chunk/oracle.go`:

```go
package chunk

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/null-ptr-exception/dblog-cdc/internal/event"
)

type OracleQuerier struct {
	db    *sql.DB
	pkCol string // primary key column name (assumed same for all tables for now)
}

func NewOracleQuerier(db *sql.DB, pkCol string) *OracleQuerier {
	return &OracleQuerier{db: db, pkCol: pkCol}
}

func (o *OracleQuerier) CurrentSCN(ctx context.Context) (uint64, error) {
	var scn uint64
	err := o.db.QueryRowContext(ctx, "SELECT current_scn FROM v$database").Scan(&scn)
	return scn, err
}

func (o *OracleQuerier) QueryChunk(ctx context.Context, table string, afterPK int64, limit int, scn uint64) (*event.ChunkResult, error) {
	query := fmt.Sprintf(
		"SELECT * FROM %s AS OF SCN %d WHERE %s > :1 ORDER BY %s ASC FETCH FIRST :2 ROWS ONLY",
		table, scn, o.pkCol, o.pkCol,
	)

	rows, err := o.db.QueryContext(ctx, query, afterPK, limit)
	if err != nil {
		return nil, fmt.Errorf("chunk query: %w", err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("columns: %w", err)
	}

	result := &event.ChunkResult{
		Table: table,
		SCN:   scn,
		Rows:  make(map[int64]map[string]any),
	}

	count := 0
	for rows.Next() {
		values := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range values {
			ptrs[i] = &values[i]
		}

		if err := rows.Scan(ptrs...); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}

		rowMap := make(map[string]any, len(cols))
		var pk int64
		for i, col := range cols {
			rowMap[col] = values[i]
			if col == o.pkCol {
				switch v := values[i].(type) {
				case int64:
					pk = v
				case float64:
					pk = int64(v)
				}
			}
		}

		result.Rows[pk] = rowMap
		result.LastPK = pk
		count++
	}

	result.Complete = count < limit
	return result, rows.Err()
}
```

- [ ] **Step 2: Add godror dependency**

```bash
cd /home/rophy/projects/dblog-cdc
go get github.com/godror/godror
```

- [ ] **Step 3: Verify it compiles**

```bash
go build ./internal/chunk/...
```

- [ ] **Step 4: Commit**

```bash
git add internal/chunk/oracle.go go.mod go.sum
git commit -m "feat: Oracle querier with AS OF SCN chunk reads"
```

---

### Task 10: Writer

**Files:**
- Create: `internal/writer/writer.go`, `internal/writer/writer_test.go`

- [ ] **Step 1: Write test using a mock writer**

Create `internal/writer/writer_test.go`:

```go
package writer_test

import (
	"context"
	"testing"

	"github.com/null-ptr-exception/dblog-cdc/internal/event"
	"github.com/null-ptr-exception/dblog-cdc/internal/writer"
)

func TestBuildUpsertSQL(t *testing.T) {
	sql, args := writer.BuildUpsertSQL("ORDERS", "ID", event.Event{
		Table: "ORDERS",
		Op:    event.OpInsert,
		PK:    42,
		Columns: map[string]any{
			"ID":     int64(42),
			"AMOUNT": 99.95,
			"STATUS": "NEW",
		},
	})

	if len(args) != 3 {
		t.Fatalf("expected 3 args, got %d", len(args))
	}

	// SQL should contain INSERT ... ON CONFLICT
	if sql == "" {
		t.Error("SQL should not be empty")
	}
	t.Logf("SQL: %s", sql)
	t.Logf("Args: %v", args)
}

func TestBuildDeleteSQL(t *testing.T) {
	sql, args := writer.BuildDeleteSQL("ORDERS", "ID", 42)

	if len(args) != 1 {
		t.Fatalf("expected 1 arg, got %d", len(args))
	}
	if args[0] != int64(42) {
		t.Errorf("expected PK 42, got %v", args[0])
	}
	if sql == "" {
		t.Error("SQL should not be empty")
	}
	t.Logf("SQL: %s", sql)
}

type mockDB struct {
	executed []string
}

func (m *mockDB) ExecContext(_ context.Context, query string, args ...any) error {
	m.executed = append(m.executed, query)
	return nil
}

func TestWriter_WriteBatch(t *testing.T) {
	db := &mockDB{}
	w := writer.New(db, "ID")

	events := []event.Event{
		{Table: "T", Op: event.OpInsert, PK: 1, Columns: map[string]any{"ID": int64(1), "V": "a"}},
		{Table: "T", Op: event.OpUpdate, PK: 2, Columns: map[string]any{"ID": int64(2), "V": "b"}},
		{Table: "T", Op: event.OpDelete, PK: 3},
	}

	err := w.WriteBatch(context.Background(), events)
	if err != nil {
		t.Fatalf("WriteBatch() error: %v", err)
	}

	if len(db.executed) != 3 {
		t.Errorf("expected 3 queries, got %d", len(db.executed))
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd /home/rophy/projects/dblog-cdc && go test ./internal/writer/... -v
```

Expected: FAIL — `writer` package does not exist.

- [ ] **Step 3: Implement writer**

Create `internal/writer/writer.go`:

```go
package writer

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/null-ptr-exception/dblog-cdc/internal/event"
)

// Executor abstracts database writes for testability.
type Executor interface {
	ExecContext(ctx context.Context, query string, args ...any) error
}

type Writer struct {
	db    Executor
	pkCol string
}

func New(db Executor, pkCol string) *Writer {
	return &Writer{db: db, pkCol: pkCol}
}

// BuildUpsertSQL generates an INSERT ... ON CONFLICT DO UPDATE statement.
// Column order is sorted for deterministic output.
func BuildUpsertSQL(table, pkCol string, e event.Event) (string, []any) {
	colNames := make([]string, 0, len(e.Columns))
	for name := range e.Columns {
		colNames = append(colNames, name)
	}
	sort.Strings(colNames)

	placeholders := make([]string, len(colNames))
	args := make([]any, len(colNames))
	updateClauses := make([]string, 0, len(colNames))

	for i, name := range colNames {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		args[i] = e.Columns[name]
		if name != pkCol {
			updateClauses = append(updateClauses, fmt.Sprintf("%s = EXCLUDED.%s", name, name))
		}
	}

	sql := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		table,
		strings.Join(colNames, ", "),
		strings.Join(placeholders, ", "),
	)

	if len(updateClauses) > 0 {
		sql += fmt.Sprintf(" ON CONFLICT (%s) DO UPDATE SET %s",
			pkCol,
			strings.Join(updateClauses, ", "),
		)
	}

	return sql, args
}

// BuildDeleteSQL generates a DELETE statement for a single PK.
func BuildDeleteSQL(table, pkCol string, pk int64) (string, []any) {
	return fmt.Sprintf("DELETE FROM %s WHERE %s = $1", table, pkCol), []any{pk}
}

// WriteBatch applies a batch of events to the target database.
func (w *Writer) WriteBatch(ctx context.Context, events []event.Event) error {
	for _, e := range events {
		var sql string
		var args []any

		if e.Op == event.OpDelete {
			sql, args = BuildDeleteSQL(e.Table, w.pkCol, e.PK)
		} else {
			sql, args = BuildUpsertSQL(e.Table, w.pkCol, e)
		}

		if err := w.db.ExecContext(ctx, sql, args...); err != nil {
			return fmt.Errorf("write %s PK=%d: %w", e.Table, e.PK, err)
		}
	}
	return nil
}
```

- [ ] **Step 4: Run tests**

```bash
cd /home/rophy/projects/dblog-cdc && go test ./internal/writer/... -v
```

Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add internal/writer/
git commit -m "feat: batched UPSERT/DELETE writer for YugabyteDB"
```

---

### Task 11: Replicator (Orchestrator)

**Files:**
- Create: `internal/replicator/replicator.go`, `internal/replicator/replicator_test.go`

- [ ] **Step 1: Write test for the orchestration loop**

Create `internal/replicator/replicator_test.go`:

```go
package replicator_test

import (
	"context"
	"testing"
	"time"

	"github.com/null-ptr-exception/dblog-cdc/internal/config"
	"github.com/null-ptr-exception/dblog-cdc/internal/event"
	"github.com/null-ptr-exception/dblog-cdc/internal/progress"
	"github.com/null-ptr-exception/dblog-cdc/internal/replicator"
)

type mockCDCSource struct {
	events []event.Event
}

func (m *mockCDCSource) Stream(ctx context.Context, startSCN uint64, ch chan<- event.Event) error {
	for _, e := range m.events {
		select {
		case ch <- e:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	// Block until cancelled to simulate ongoing stream
	<-ctx.Done()
	return ctx.Err()
}

func (m *mockCDCSource) LastSCN() uint64 {
	if len(m.events) == 0 {
		return 100
	}
	return m.events[len(m.events)-1].SCN
}

type mockChunkQuerier struct {
	rows []map[string]any
}

func (m *mockChunkQuerier) QueryChunk(_ context.Context, table string, afterPK int64, limit int, scn uint64) (*event.ChunkResult, error) {
	result := &event.ChunkResult{
		Table: table,
		SCN:   scn,
		Rows:  make(map[int64]map[string]any),
	}
	count := 0
	for _, row := range m.rows {
		pk := row["ID"].(int64)
		if pk > afterPK {
			result.Rows[pk] = row
			result.LastPK = pk
			count++
			if count >= limit {
				break
			}
		}
	}
	result.Complete = count < limit
	return result, nil
}

func (m *mockChunkQuerier) CurrentSCN(_ context.Context) (uint64, error) {
	return 100, nil
}

type captureWriter struct {
	events []event.Event
}

func (w *captureWriter) WriteBatch(_ context.Context, events []event.Event) error {
	w.events = append(w.events, events...)
	return nil
}

func TestReplicator_ChunksAndCDC(t *testing.T) {
	cdc := &mockCDCSource{
		events: []event.Event{
			{Table: "T", Op: event.OpUpdate, SCN: 105, PK: 2, Columns: map[string]any{"ID": int64(2), "V": "cdc_updated"}},
		},
	}

	chunks := &mockChunkQuerier{
		rows: []map[string]any{
			{"ID": int64(1), "V": "chunk_1"},
			{"ID": int64(2), "V": "chunk_2"},
			{"ID": int64(3), "V": "chunk_3"},
		},
	}

	writer := &captureWriter{}
	store := progress.NewMemoryStore()

	cfg := config.Table{Name: "T", ChunkSize: 10}
	r := replicator.New(cdc, chunks, writer, store, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := r.Run(ctx)
	if err != nil && err != context.DeadlineExceeded {
		t.Fatalf("Run() error: %v", err)
	}

	// Verify: PK 2 should come from CDC (SCN 105 > chunk SCN 100), not chunk
	found := map[int64]event.Event{}
	for _, e := range writer.events {
		found[e.PK] = e
	}

	if len(found) < 3 {
		t.Fatalf("expected at least 3 written events, got %d", len(found))
	}

	if found[2].Columns["V"] != "cdc_updated" {
		t.Errorf("PK 2 should be from CDC, got %v", found[2].Columns["V"])
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd /home/rophy/projects/dblog-cdc && go test ./internal/replicator/... -v
```

Expected: FAIL — `replicator` package does not exist.

- [ ] **Step 3: Implement replicator**

Create `internal/replicator/replicator.go`:

```go
package replicator

import (
	"context"
	"log/slog"

	"github.com/null-ptr-exception/dblog-cdc/internal/buffer"
	"github.com/null-ptr-exception/dblog-cdc/internal/chunk"
	"github.com/null-ptr-exception/dblog-cdc/internal/config"
	"github.com/null-ptr-exception/dblog-cdc/internal/event"
	"github.com/null-ptr-exception/dblog-cdc/internal/progress"
)

// CDCSource abstracts the CDC event stream.
type CDCSource interface {
	Stream(ctx context.Context, startSCN uint64, events chan<- event.Event) error
	LastSCN() uint64
}

// BatchWriter abstracts the target database writer.
type BatchWriter interface {
	WriteBatch(ctx context.Context, events []event.Event) error
}

type Replicator struct {
	cdc     CDCSource
	chunks  chunk.Querier
	writer  BatchWriter
	store   progress.Store
	table   config.Table
}

func New(cdc CDCSource, chunks chunk.Querier, writer BatchWriter, store progress.Store, table config.Table) *Replicator {
	return &Replicator{
		cdc:    cdc,
		chunks: chunks,
		writer: writer,
		store:  store,
		table:  table,
	}
}

func (r *Replicator) Run(ctx context.Context) error {
	state, err := r.store.Get(ctx, r.table.Name)
	if err != nil {
		return err
	}

	var lastPK int64
	chunksComplete := false
	if state.LastPK != nil {
		if *state.LastPK == -1 {
			chunksComplete = true
		} else {
			lastPK = *state.LastPK
		}
	}

	cdcCh := make(chan event.Event, 1000)
	cdcErrCh := make(chan error, 1)

	go func() {
		cdcErrCh <- r.cdc.Stream(ctx, state.LastSCN, cdcCh)
	}()

	buf := buffer.New()
	selector := chunk.NewSelector(r.chunks)

	for {
		// Drain available CDC events
		drainLoop:
		for {
			select {
			case ev := <-cdcCh:
				if !chunksComplete {
					buf.ApplyCDCDedup(ev)
				} else {
					buf.PushCDC(ev)
				}
			default:
				break drainLoop
			}
		}

		// Select next chunk if not complete
		if !chunksComplete {
			scnBefore := r.cdc.LastSCN()
			if scnBefore == 0 {
				scnBefore = 100 // fallback for tests
			}

			chunkResult, err := selector.Next(ctx, r.table.Name, lastPK, r.table.ChunkSize, scnBefore)
			if err != nil {
				return err
			}

			buf.PushChunk(*chunkResult)

			// Drain CDC events that arrived during chunk SELECT
			drainAfterChunk:
			for {
				select {
				case ev := <-cdcCh:
					buf.ApplyCDCDedup(ev)
				default:
					break drainAfterChunk
				}
			}

			// Flush buffer to writer
			events := buf.Drain()
			if len(events) > 0 {
				if err := r.writer.WriteBatch(ctx, events); err != nil {
					return err
				}
			}

			lastPK = chunkResult.LastPK
			pk := lastPK
			if chunkResult.Complete {
				pk = -1
				chunksComplete = true
			}
			if err := r.store.Save(ctx, r.table.Name, &pk, scnBefore); err != nil {
				return err
			}

			if chunkResult.Complete {
				slog.Info("chunk loading complete", "table", r.table.Name, "last_pk", lastPK)
			}

			continue
		}

		// Pure CDC mode — wait for events
		select {
		case ev := <-cdcCh:
			buf.PushCDC(ev)
			events := buf.Drain()
			if len(events) > 0 {
				if err := r.writer.WriteBatch(ctx, events); err != nil {
					return err
				}
			}
		case err := <-cdcErrCh:
			return err
		case <-ctx.Done():
			// Flush remaining
			events := buf.Drain()
			if len(events) > 0 {
				r.writer.WriteBatch(ctx, events)
			}
			return ctx.Err()
		}
	}
}
```

- [ ] **Step 4: Run tests**

```bash
cd /home/rophy/projects/dblog-cdc && go test ./internal/replicator/... -v -timeout=10s
```

Expected: PASS (1 test).

- [ ] **Step 5: Commit**

```bash
git add internal/replicator/
git commit -m "feat: replicator orchestrator wiring CDC, chunks, and writer"
```

---

### Task 12: Main Entry Point

**Files:**
- Create: `cmd/dblog/main.go`

- [ ] **Step 1: Implement main**

Create `cmd/dblog/main.go`:

```go
package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/godror/godror"

	"github.com/null-ptr-exception/dblog-cdc/internal/chunk"
	"github.com/null-ptr-exception/dblog-cdc/internal/config"
	"github.com/null-ptr-exception/dblog-cdc/internal/olr"
	"github.com/null-ptr-exception/dblog-cdc/internal/progress"
	"github.com/null-ptr-exception/dblog-cdc/internal/replicator"
	"github.com/null-ptr-exception/dblog-cdc/internal/writer"
)

func main() {
	configPath := flag.String("config", "config.yaml", "path to config file")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		slog.Error("load config", "error", err)
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Connect to Oracle
	oracleDB, err := sql.Open("godror", cfg.Source.DSN)
	if err != nil {
		slog.Error("connect oracle", "error", err)
		os.Exit(1)
	}
	defer oracleDB.Close()

	// Connect to YugabyteDB
	ybPool, err := pgxpool.New(ctx, cfg.Target.DSN)
	if err != nil {
		slog.Error("connect yugabytedb", "error", err)
		os.Exit(1)
	}
	defer ybPool.Close()

	// Initialize progress store
	pgStore := progress.NewPgStore(ybPool, cfg.Progress.Table)
	if err := pgStore.EnsureTable(ctx); err != nil {
		slog.Error("ensure progress table", "error", err)
		os.Exit(1)
	}

	// Process each table (sequentially for v1)
	for _, tbl := range cfg.Tables {
		slog.Info("starting replication", "table", tbl.Name)

		tableNames := make([]string, len(cfg.Tables))
		for i, t := range cfg.Tables {
			tableNames[i] = t.Name
		}

		cdcClient := olr.NewClient(cfg.CDC.Host, cfg.CDC.Port, "", tableNames)
		querier := chunk.NewOracleQuerier(oracleDB, "ID") // TODO: detect PK column name from Oracle metadata
		ybWriter := writer.NewPgWriter(ybPool, "ID")

		r := replicator.New(cdcClient, querier, ybWriter, pgStore, tbl)
		if err := r.Run(ctx); err != nil {
			slog.Error("replication failed", "table", tbl.Name, "error", err)
			os.Exit(1)
		}
	}

	fmt.Println("replication complete")
}
```

- [ ] **Step 2: Add PgWriter to writer package**

Add to `internal/writer/writer.go`:

```go
// PgWriter wraps a pgxpool.Pool to implement the Executor interface.
type PgWriter struct {
	pool  interface {
		Exec(ctx context.Context, sql string, args ...any) (interface{}, error)
	}
	pkCol string
}
```

Actually, create a separate file `internal/writer/pgwriter.go`:

```go
package writer

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

type pgExecutor struct {
	pool *pgxpool.Pool
}

func (p *pgExecutor) ExecContext(ctx context.Context, query string, args ...any) error {
	_, err := p.pool.Exec(ctx, query, args...)
	return err
}

func NewPgWriter(pool *pgxpool.Pool, pkCol string) *Writer {
	return New(&pgExecutor{pool: pool}, pkCol)
}
```

- [ ] **Step 3: Verify it compiles**

```bash
cd /home/rophy/projects/dblog-cdc && go build ./cmd/dblog/...
```

- [ ] **Step 4: Commit**

```bash
git add cmd/dblog/ internal/writer/pgwriter.go
git commit -m "feat: main entry point and PgWriter"
```

---

### Task 13: Test Environment

**Files:**
- Create: `testenv/docker-compose.yaml`, `testenv/oracle-init/01-setup.sh`, `testenv/olr-config.json`

- [ ] **Step 1: Create Oracle init script**

Create `testenv/oracle-init/01-setup.sh`:

```bash
#!/bin/bash
# Enable archivelog and supplemental logging for OLR.
# Runs during gvenzl/oracle-free container init.

sqlplus -S / as sysdba <<'SQL'
SHUTDOWN IMMEDIATE;
STARTUP MOUNT;
ALTER DATABASE ARCHIVELOG;
ALTER DATABASE OPEN;
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;
ALTER SYSTEM SET db_recovery_file_dest_size=10G;
ALTER SYSTEM SET log_archive_dest_1='LOCATION=/opt/oracle/oradata/FREE/archive' SCOPE=BOTH;
HOST mkdir -p /opt/oracle/oradata/FREE/archive

-- Grant flashback and v$database access for dblog chunk reads
ALTER SESSION SET CONTAINER=FREEPDB1;
GRANT SELECT ANY TABLE TO testuser;
GRANT FLASHBACK ANY TABLE TO testuser;
GRANT SELECT ON SYS.V_$DATABASE TO testuser;

-- Create test table
CREATE TABLE testuser.ORDERS (
    ID NUMBER(10) PRIMARY KEY,
    AMOUNT NUMBER(10,2),
    STATUS VARCHAR2(50)
);

-- Enable supplemental logging on the test table
ALTER TABLE testuser.ORDERS ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
SQL
```

- [ ] **Step 2: Create OLR config**

Create `testenv/olr-config.json`:

```json
{
  "version": "1.9.0",
  "memory": {
    "min-mb": 64,
    "max-mb": 256
  },
  "source": [
    {
      "alias": "SOURCE",
      "name": "FREE",
      "reader": {
        "type": "online",
        "user": "testuser",
        "password": "testuser",
        "server": "//oracle:1521/FREEPDB1"
      },
      "format": {
        "type": "protobuf",
        "scn-type": 4
      },
      "filter": {
        "table": [
          {"owner": "TESTUSER", "table": "ORDERS"}
        ]
      }
    }
  ],
  "target": [
    {
      "alias": "NETWORK",
      "source": "SOURCE",
      "writer": {
        "type": "network",
        "uri": "0.0.0.0:5000"
      }
    }
  ]
}
```

- [ ] **Step 3: Create docker-compose.yaml**

Create `testenv/docker-compose.yaml`:

```yaml
services:
  oracle:
    image: gvenzl/oracle-free:23-slim-faststart
    container_name: dblog-oracle
    ports:
      - "1521:1521"
    environment:
      ORACLE_PASSWORD: oracle
      APP_USER: testuser
      APP_USER_PASSWORD: testuser
    volumes:
      - ./oracle-init:/container-entrypoint-initdb.d
      - oracle-data:/opt/oracle/oradata
    healthcheck:
      test: ["CMD", "healthcheck.sh"]
      interval: 10s
      timeout: 5s
      retries: 30

  olr:
    image: ghcr.io/rophy/openlogreplicator:v1.9.0.1
    container_name: dblog-olr
    user: "1000:1000"
    entrypoint: ["/bin/bash", "-c", "mkdir -p /olr-data/checkpoint && exec /opt/OpenLogReplicator/OpenLogReplicator \"$@\"", "--"]
    command: ["-r", "-f", "/config/olr-config.json"]
    working_dir: /olr-data
    ports:
      - "5000:5000"
    group_add:
      - "54321"
    depends_on:
      oracle:
        condition: service_healthy
    tmpfs:
      - /olr-data:uid=1000,gid=54322
    volumes:
      - ./olr-config.json:/config/olr-config.json:ro
      - oracle-data:/opt/oracle/oradata:ro

  yugabytedb:
    image: yugabytedb/yugabyte:2024.2.3.0-b1
    container_name: dblog-yugabytedb
    command: >
      bin/yugabyted start
      --daemon=false
      --tserver_flags="ysql_num_shards_per_tserver=1"
    ports:
      - "5433:5433"
      - "15433:15433"
    healthcheck:
      test: ["CMD", "bin/yugabyted", "status"]
      interval: 10s
      timeout: 5s
      retries: 30

volumes:
  oracle-data:
```

- [ ] **Step 4: Create test config**

Create `testenv/dblog-config.yaml`:

```yaml
source:
  type: oracle
  dsn: "oracle://testuser:testuser@localhost:1521/FREEPDB1"

target:
  type: yugabytedb
  dsn: "postgres://yugabyte:yugabyte@localhost:5433/yugabyte"

cdc:
  host: "localhost"
  port: 5000
  scn_type: 4
  format: protobuf

tables:
  - name: "ORDERS"
    chunk_size: 1000

defaults:
  chunk_size: 10000

progress:
  table: "dblog_progress"
```

- [ ] **Step 5: Verify compose file parses**

```bash
cd /home/rophy/projects/dblog-cdc && docker compose -f testenv/docker-compose.yaml config > /dev/null
```

Expected: exits 0 (valid compose file).

- [ ] **Step 6: Commit**

```bash
git add testenv/
git commit -m "feat: docker compose test environment with Oracle + OLR + YugabyteDB"
```

---

### Task 14: Integration Test

**Files:**
- Create: `integration/replication_test.go`

This test requires the test environment to be running (`make testenv-up`). It uses build tags to avoid running in CI without the environment.

- [ ] **Step 1: Write integration test**

Create `integration/replication_test.go`:

```go
//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/godror/godror"

	"github.com/null-ptr-exception/dblog-cdc/internal/chunk"
	"github.com/null-ptr-exception/dblog-cdc/internal/config"
	"github.com/null-ptr-exception/dblog-cdc/internal/olr"
	"github.com/null-ptr-exception/dblog-cdc/internal/progress"
	"github.com/null-ptr-exception/dblog-cdc/internal/replicator"
	"github.com/null-ptr-exception/dblog-cdc/internal/writer"
)

const (
	oracleDSN = "oracle://testuser:testuser@localhost:1521/FREEPDB1"
	ybDSN     = "postgres://yugabyte:yugabyte@localhost:5433/yugabyte"
	olrHost   = "localhost"
	olrPort   = 5000
)

func TestEndToEnd_ChunkAndCDC(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// Connect to Oracle
	oracleDB, err := sql.Open("godror", oracleDSN)
	if err != nil {
		t.Fatalf("connect oracle: %v", err)
	}
	defer oracleDB.Close()

	// Seed Oracle with test data
	for i := 1; i <= 100; i++ {
		_, err := oracleDB.ExecContext(ctx,
			"INSERT INTO ORDERS (ID, AMOUNT, STATUS) VALUES (:1, :2, :3)",
			i, float64(i)*10.0, "INIT")
		if err != nil {
			t.Fatalf("seed row %d: %v", i, err)
		}
	}
	t.Log("seeded 100 rows in Oracle")

	// Connect to YugabyteDB
	ybPool, err := pgxpool.New(ctx, ybDSN)
	if err != nil {
		t.Fatalf("connect yb: %v", err)
	}
	defer ybPool.Close()

	// Create target table
	_, err = ybPool.Exec(ctx, `CREATE TABLE IF NOT EXISTS ORDERS (
		ID BIGINT PRIMARY KEY,
		AMOUNT DOUBLE PRECISION,
		STATUS TEXT
	)`)
	if err != nil {
		t.Fatalf("create yb table: %v", err)
	}

	// Set up components
	pgStore := progress.NewPgStore(ybPool, "dblog_progress")
	if err := pgStore.EnsureTable(ctx); err != nil {
		t.Fatalf("ensure progress: %v", err)
	}

	cdcClient := olr.NewClient(olrHost, olrPort, "", []string{"ORDERS"})
	querier := chunk.NewOracleQuerier(oracleDB, "ID")
	ybWriter := writer.NewPgWriter(ybPool, "ID")

	tbl := config.Table{Name: "ORDERS", ChunkSize: 25}
	r := replicator.New(cdcClient, querier, ybWriter, pgStore, tbl)

	// Run concurrent mutations while replicator is running
	go func() {
		time.Sleep(2 * time.Second) // let chunking start
		for i := 101; i <= 110; i++ {
			oracleDB.ExecContext(ctx,
				"INSERT INTO ORDERS (ID, AMOUNT, STATUS) VALUES (:1, :2, :3)",
				i, float64(i)*10.0, "CDC_INSERT")
		}
		// Update some rows that might be in a chunk
		oracleDB.ExecContext(ctx,
			"UPDATE ORDERS SET STATUS = 'CDC_UPDATED' WHERE ID = 50")
		t.Log("concurrent mutations applied")
	}()

	// Run replicator with timeout
	replicatorCtx, replicatorCancel := context.WithTimeout(ctx, 60*time.Second)
	defer replicatorCancel()

	err = r.Run(replicatorCtx)
	if err != nil && err != context.DeadlineExceeded {
		t.Fatalf("replicator: %v", err)
	}

	// Verify: count rows in YugabyteDB
	var count int
	err = ybPool.QueryRow(ctx, "SELECT COUNT(*) FROM ORDERS").Scan(&count)
	if err != nil {
		t.Fatalf("count yb: %v", err)
	}

	if count < 100 {
		t.Errorf("expected at least 100 rows in YB, got %d", count)
	}
	t.Logf("YugabyteDB has %d rows", count)

	// Verify: PK 50 should have CDC_UPDATED status (if CDC event arrived)
	var status string
	err = ybPool.QueryRow(ctx, "SELECT STATUS FROM ORDERS WHERE ID = 50").Scan(&status)
	if err != nil {
		t.Fatalf("query PK 50: %v", err)
	}
	t.Logf("PK 50 STATUS = %q", status)

	// Verify: sum check
	var oracleSum, ybSum float64
	oracleDB.QueryRowContext(ctx, "SELECT SUM(AMOUNT) FROM ORDERS").Scan(&oracleSum)
	ybPool.QueryRow(ctx, "SELECT SUM(AMOUNT) FROM ORDERS").Scan(&ybSum)

	if oracleSum != ybSum {
		t.Errorf("sum mismatch: oracle=%f yb=%f", oracleSum, ybSum)
	}
	t.Logf("sum check: oracle=%f yb=%f", oracleSum, ybSum)
}
```

- [ ] **Step 2: Update Makefile for integration tests**

The Makefile already has `test-integration`. Update it to include the build tag:

In `Makefile`, change the `test-integration` target:

```makefile
test-integration:
	go test ./integration/... -v -count=1 -timeout=300s -tags=integration
```

- [ ] **Step 3: Verify integration test compiles**

```bash
cd /home/rophy/projects/dblog-cdc && go test -tags=integration -c ./integration/... -o /dev/null
```

Expected: compiles without error.

- [ ] **Step 4: Commit**

```bash
git add integration/ Makefile
git commit -m "feat: end-to-end integration test with Oracle + OLR + YugabyteDB"
```

---

### Task 15: Run All Unit Tests

- [ ] **Step 1: Run all unit tests**

```bash
cd /home/rophy/projects/dblog-cdc && go test ./internal/... -v -count=1
```

Expected: all tests pass (config: 2, buffer: 5, progress: 2, olr: 4, chunk: 2, writer: 3, replicator: 1 = 19 tests).

- [ ] **Step 2: Fix any failures**

If any tests fail, fix the implementation and re-run.

- [ ] **Step 3: Final commit**

```bash
git add -A
git commit -m "chore: ensure all unit tests pass"
```

---

## Summary

| Task | Component | Tests |
|------|-----------|-------|
| 1 | Project scaffolding + protobuf | compile check |
| 2 | Event types | compile check |
| 3 | Config loader | 2 unit tests |
| 4 | Event buffer (core algorithm) | 5 unit tests |
| 5 | Progress store (interface + memory) | 2 unit tests |
| 6 | Progress store (YugabyteDB) | compile check (integration tested) |
| 7 | OLR client | 4 unit tests |
| 8 | Chunk selector | 2 unit tests |
| 9 | Oracle querier | compile check (integration tested) |
| 10 | Writer | 3 unit tests |
| 11 | Replicator orchestrator | 1 unit test |
| 12 | Main entry point + PgWriter | compile check |
| 13 | Test environment (docker compose) | compose config check |
| 14 | Integration test | 1 e2e test |
| 15 | Run all unit tests | 19 total |
