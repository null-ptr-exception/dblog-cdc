# DBLog-CDC: Oracle → YugabyteDB Change Data Capture

**Repo:** `null-ptr-exception/dblog-cdc`
**Language:** Go
**Date:** 2026-07-01

## Overview

A Go implementation of the DBLog framework (Netflix, 2020) that replicates Oracle tables to YugabyteDB. Combines CDC streaming (via OpenLogReplicator) with chunked full-state capture to handle tables too large to snapshot atomically.

Key deviation from the paper: uses Oracle's `AS OF SCN` for consistent chunk reads instead of a watermark table, making the system fully read-only on the Oracle source.

## Architecture

Single Go binary with three goroutines per table:

```
┌─────────────────────────────────────────────────────┐
│                   dblog process                     │
│                                                     │
│  ┌──────────────┐    ┌─────────────────────────┐    │
│  │ CDC Reader    │    │ Chunk Selector          │    │
│  │ (goroutine)   │    │ (goroutine)             │    │
│  │               │    │                         │    │
│  │ OpenLog       │    │ Oracle SELECT chunks    │    │
│  │ Replicator    │    │ via AS OF SCN           │    │
│  │ protobuf      │    │ (read-only)             │    │
│  └──────┬────────┘    └────────┬────────────────┘    │
│         │                      │                     │
│         ▼                      ▼                     │
│  ┌─────────────────────────────────────────────┐    │
│  │           Event Buffer (in-memory)           │    │
│  │  SCN-based dedup: CDC wins over chunk        │    │
│  └──────────────────┬──────────────────────────┘    │
│                     ▼                                │
│  ┌─────────────────────────────────────────────┐    │
│  │           Writer (goroutine)                 │    │
│  │  batched UPSERTs → YugabyteDB (YSQL)        │    │
│  └─────────────────────────────────────────────┘    │
│                                                     │
│  State: dblog_progress table in YugabyteDB          │
└─────────────────────────────────────────────────────┘
```

### Component Responsibilities

**CDC Reader:**
- Connects to OpenLogReplicator via protobuf network socket
- Receives INSERT/UPDATE/DELETE events with per-DML SCN (`scn-type: 4`)
- Sends periodic `CONFIRM` to OLR to advance checkpoint
- On reconnect, sends `CONTINUE` with last `c_scn` + `c_idx`

**Chunk Selector:**
- Iterates configured tables, selecting rows in primary-key-ordered chunks
- Uses `AS OF SCN` for consistent reads at a known CDC stream position
- Hands chunk rows to the event buffer for dedup against CDC events

**Event Buffer:**
- In-memory buffer that interleaves CDC events and chunk rows
- Dedup logic: if a CDC event (SCN > chunk's snapshot SCN) covers a PK in the chunk, the CDC event wins and the chunk row is discarded
- Drains to the writer

**Writer:**
- Batches rows and applies via `INSERT ... ON CONFLICT DO UPDATE` (UPSERT) to YugabyteDB
- Updates `dblog_progress` atomically with each completed chunk

## SCN-Based Chunk Interleaving Algorithm

Replaces the paper's watermark table approach. No writes to Oracle.

```
For each table, repeat until all chunks are processed:

1. Note current CDC stream position → scn_before (from latest event's SCN)
2. SELECT * FROM table AS OF SCN(scn_before)
   WHERE pk > last_pk ORDER BY pk LIMIT chunk_size
   → store result set in memory as map[pk]→row
3. Note current CDC stream position → scn_after
4. CDC events with SCN <= scn_before:
   → already reflected in the chunk snapshot
   → apply to output normally (UPSERT is idempotent)
5. CDC events with scn_before < SCN <= scn_after whose PK is in the chunk:
   → CDC wins, remove that PK from chunk map
6. Output remaining chunk rows as UPSERTs
7. Save last_pk and scn_after to dblog_progress
```

**Why this works:** `AS OF SCN(scn_before)` returns a consistent snapshot at that point in the redo log. Any CDC event after that SCN is newer — if it touches a PK in the chunk, the CDC version is more recent and takes precedence. Everything else in the chunk is safe to apply.

**Correctness guarantees:**
- No data loss: CDC events are never discarded; chunk rows are only discarded when a newer CDC event exists
- No stale overwrites: chunk rows are from a known SCN; any newer CDC event for the same PK wins
- Idempotent: UPSERT semantics mean replaying events or chunks produces the same result
- History preserved: CDC events are applied in SCN order

## Data Model

### YugabyteDB (target)

```sql
CREATE TABLE dblog_progress (
    table_name  TEXT PRIMARY KEY,
    last_pk     BIGINT,          -- NULL = not started, -1 = complete
    last_lsn    BIGINT,          -- last applied CDC SCN
    updated_at  TIMESTAMPTZ DEFAULT now()
);
```

### Oracle (source)

No tables created. Fully read-only access. Requires:
- `SELECT` on source tables
- `FLASHBACK` privilege (for `AS OF SCN` queries)
- User with `SELECT ANY DICTIONARY` or LogMiner grants (for OpenLogReplicator)

### Data writes to YugabyteDB

All rows (from CDC events and chunks) are applied as:

```sql
INSERT INTO <table> (pk, col1, col2, ...)
VALUES ($1, $2, $3, ...)
ON CONFLICT (pk) DO UPDATE SET col1 = EXCLUDED.col1, col2 = EXCLUDED.col2, ...
```

## Configuration

```yaml
source:
  type: oracle
  dsn: "oracle://user:pass@oracle:1521/FREEPDB1"

target:
  type: yugabytedb
  dsn: "postgres://yugabyte:yugabyte@yugabytedb:5433/yugabyte"

cdc:
  host: "olr"
  port: 5000
  scn_type: 4
  format: protobuf

tables:
  - name: "ORDERS"
    chunk_size: 10000
  - name: "CUSTOMERS"
    chunk_size: 5000

defaults:
  chunk_size: 10000

progress:
  table: "dblog_progress"
```

**Table configuration:**
- `name`: Oracle table name (must have a single integer primary key)
- `chunk_size`: rows per chunk SELECT (default from `defaults.chunk_size`)

**Schema mapping:** First implementation assumes identical table/column names between Oracle and YugabyteDB. Type coercion layer can be added later.

## Test Environment

Docker Compose stack based on OpenLogReplicator's `tests/dbz-twin/` setup:

| Service | Image | Purpose |
|---------|-------|---------|
| oracle | `gvenzl/oracle-free:23-slim-faststart` | Source database |
| olr | `ghcr.io/rophy/openlogreplicator:v1.9.0.1` | CDC from Oracle redo logs |
| yugabytedb | `yugabytedb/yugabyte` (single-node) | Target database |
| dblog | Built from source | The system under test |

**Oracle init scripts** (based on OLR's `tests/dbz-twin/oracle-init/`):
- Enable ARCHIVELOG mode
- Enable supplemental logging
- Create test user with SELECT + FLASHBACK grants

**OLR config:**
- Format: `protobuf`
- `scn-type: 4` (per-DML SCN)
- Network output on port 5000

**Integration test flow:**

```
go test ./integration/...
  → docker compose up (Oracle, OLR, YugabyteDB)
  → wait for Oracle ARCHIVELOG + OLR ready
  → seed Oracle with test data (e.g. 100K rows)
  → start dblog
  → run concurrent INSERT/UPDATE/DELETE on Oracle
  → wait for dblog to catch up (no new events for N seconds)
  → compare source and target: row counts + checksums
  → docker compose down
```

## Error Handling and Resumability

### Crash Recovery

On restart, dblog reads `dblog_progress` from YugabyteDB:
- `last_pk`: resume chunk selection from this key
- `last_lsn`: reconnect to OpenLogReplicator with `CONTINUE` at this SCN

UPSERTs are idempotent — replaying a partial chunk after crash produces no duplicates.

### Progress Checkpointing

After each chunk is fully written, update `dblog_progress` in the same transaction as the last batch of UPSERTs. Atomic "chunk N is done" semantics.

### Failure Modes

| Failure | Behavior |
|---------|----------|
| OLR disconnect | Reconnect with `CONTINUE` using last `c_scn` + `c_idx`. UPSERTs handle replayed events. |
| Oracle disconnect during chunk | Discard partial chunk. Retry from same `last_pk`. No progress saved, nothing to roll back. |
| YugabyteDB unavailable | Back off and retry writes. CDC reader continues buffering up to configurable memory limit. If buffer fills, pause chunk selection. |
| Process crash | Resume from `dblog_progress` checkpoint. Idempotent UPSERTs handle any replayed data. |

### Completion

When a chunk SELECT returns fewer rows than `chunk_size`, the table's full state is captured. Set `last_pk = -1` in progress. The process continues as a pure CDC consumer for ongoing replication.

### Graceful Shutdown (SIGTERM)

1. Stop accepting new chunks
2. Flush current buffer to YugabyteDB
3. Save progress
4. Send `CONFIRM` to OLR with final checkpoint
5. Exit

## Scope and Non-Goals

### In Scope
- Single integer primary key tables
- INSERT, UPDATE, DELETE CDC events
- Chunked full-state capture with SCN-based dedup
- Crash recovery and resume
- Docker Compose test environment
- YAML configuration

### Not In Scope (future work)
- Composite or non-integer primary keys
- DDL replication / schema changes
- Auto-discovery of Oracle tables
- Multi-table parallelism (designed for, not implemented in v1)
- High availability / leader election
- Kafka as output
- Oracle-to-YugabyteDB type coercion beyond compatible defaults
