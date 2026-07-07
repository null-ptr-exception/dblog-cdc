# dblog-cdc

A Change-Data-Capture (CDC) replication tool that streams data from Oracle to YugabyteDB. Based on Netflix's [DBLog paper](docs/2010.12597.pdf), with a simplified watermark-free design that uses Oracle's `AS OF SCN` for point-in-time consistent chunk reads.

## How it works

The replicator runs two processes in parallel:

1. **Chunk loading** — reads the full table state in primary-key-ordered chunks using `SELECT ... AS OF SCN`. Tracks progress (last PK) so it can pause and resume.
2. **CDC streaming** — receives real-time row changes from [OpenLogReplicator](https://github.com/bersler/OpenLogReplicator) via its network protocol (protobuf handshake, JSON data).

During chunk loading, CDC events are deduplicated against chunk data: if a CDC event has a higher SCN than the chunk for the same primary key, the CDC version wins. This replaces the DBLog paper's watermark table approach — Oracle's `AS OF SCN` gives us the exact log position of each chunk read, so no watermark writes to the source database are needed.

After all chunks are loaded, the replicator switches to CDC-only mode and continuously applies changes to YugabyteDB using upserts (`INSERT ... ON CONFLICT DO UPDATE`).

### Schema-aware type mapping

Oracle and PostgreSQL/YugabyteDB have different type systems. OLR delivers all values as JSON, which loses type information (e.g. large integers lose precision as float64, timestamps use non-standard formats). At startup, the replicator queries Oracle's `ALL_TAB_COLUMNS` to build a column type map, then converts values before writing:

| Oracle type | OLR delivers | Converted to |
|-------------|-------------|--------------|
| `NUMBER` (high precision) | `json.Number` string | Passed through (pgx binds as numeric) |
| `TIMESTAMP WITH TIME ZONE` | `"epoch_nanos,+offset"` | `time.Time` |
| `RAW` | hex string (`"deadbeef"`) | `[]byte` |
| `INTERVAL DAY TO SECOND` | nanoseconds (number) | PG interval string (`"5 days 03:30:15"`) |
| `BINARY_FLOAT` | float64 with artifact | `float32` (removes promotion noise) |
| `DATE`, `TIMESTAMP` | ISO-like string | Passed through (PG accepts the format) |

No configuration needed — type mapping is automatic.

## Prerequisites

### Source: Oracle

- Table must have a **primary key** (used for chunk ordering and CDC event dedup). Both single-column and compound (multi-column) primary keys are supported. Any sortable type works (integer, string, etc.) — PK values are treated as strings internally.
- **Supplemental logging** must be enabled on the table:
  ```sql
  ALTER TABLE my_table ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
  ```
- **Archivelog mode** must be enabled (required by OpenLogReplicator):
  ```sql
  ALTER DATABASE ARCHIVELOG;
  ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;
  ```
- The replication user needs `SELECT`, `FLASHBACK`, and dictionary privileges. See [testenv/oracle-init/01-setup.sh](testenv/oracle-init/01-setup.sh) for the full grant list.

### CDC: OpenLogReplicator

- OLR must be configured with `json` format and `network` writer, pointed at the source database.
- Use `"timestamp": 12` for ISO8601 timestamps (PostgreSQL-compatible) and `"column": 2` for full column values on all operations.
- OLR reads online redo logs directly — no log switches needed for real-time streaming.
- See [testenv/olr-config.json](testenv/olr-config.json) for a working configuration.

### Target: YugabyteDB (or PostgreSQL)

- The target table must be created manually with a matching schema and the **same primary key** (including compound PKs):
  ```sql
  CREATE TABLE orders (
      id BIGINT PRIMARY KEY,
      amount DOUBLE PRECISION,
      status TEXT
  );
  -- Compound PK example:
  CREATE TABLE order_items (
      order_id BIGINT,
      item_id  BIGINT,
      qty      INTEGER,
      PRIMARY KEY (order_id, item_id)
  );
  ```
- A progress table is created automatically (default: `dblog_progress`) to track chunk position and last-seen SCN.

## Configuration

```yaml
source:
  type: oracle
  dsn: "oracle://user:pass@host:1521/service"

target:
  type: yugabytedb
  dsn: "postgres://user:pass@host:5433/db"

cdc:
  host: olr-host
  port: 5000

tables:
  - name: ORDERS
    pk_columns:          # primary key column(s) (default: [ID])
      - ID
    chunk_size: 10000    # rows per chunk (default: 10000)
  - name: ORDER_ITEMS
    pk_columns:          # compound PK example
      - ORDER_ID
      - ITEM_ID

progress:
  table: dblog_progress  # progress tracking table in target DB
```

Run with:

```bash
./dblog -config config.yaml
```

## Development

Start the full stack (Oracle, OLR, YugabyteDB, dev container):

```bash
docker compose up -d
```

Build and run tests:

```bash
make build        # build dev container
make test-unit    # unit tests
make test-e2e     # integration tests (requires running stack)
```

### Stress test

The stress test runs randomized INSERT/UPDATE/DELETE workloads against Oracle, then verifies row-by-row convergence with YugabyteDB:

```bash
make test-e2e     # runs all tests including stress

# Tune via env vars:
docker compose exec \
  -e STRESS_WORKERS=4 \
  -e STRESS_ROUND_SEC=30 \
  -e STRESS_ROUNDS=5 \
  -e STRESS_PK_RANGE=2000 \
  -e STRESS_OP_DELAY_MS=1 \
  dev go test ./integration/... -v -count=1 -timeout=600s -tags=integration -run TestReplication_Stress
```

## Architecture

```
Oracle ──────────────────────────────────────── YugabyteDB
  │                                                 ▲
  ├── chunk reads (SELECT ... AS OF SCN) ──────────┐│
  │                                                ││
  └── redo logs ──► OpenLogReplicator ──► dblog ───┘│
                     (JSON/TCP)            (dedup    │
                                          + upsert)─┘
```

Key components:

| Package | Role |
|---------|------|
| `cmd/dblog` | CLI entry point |
| `internal/replicator` | Main loop: chunk loading + CDC interleaving |
| `internal/olr` | OLR client (protobuf handshake, JSON streaming, CONFIRM) |
| `internal/chunk` | Oracle chunk querier (`AS OF SCN`) |
| `internal/buffer` | In-memory dedup buffer (CDC vs chunk events) |
| `internal/writer` | YugabyteDB upsert/delete writer |
| `internal/transform` | Schema-aware Oracle→PG type mapping |
| `internal/progress` | Chunk position + SCN tracking (stored in target DB) |

## Differences from the DBLog paper

| DBLog paper | This implementation |
|-------------|---------------------|
| Watermark table writes to bracket chunk reads | `AS OF SCN` gives exact log position — no source writes needed |
| Zookeeper for progress + leader election | Progress stored in target DB (YugabyteDB) |
| Kafka output | Direct writes to YugabyteDB |
| MySQL/PostgreSQL binlog/WAL | Oracle redo logs via OpenLogReplicator |
| Java framework | Go |
