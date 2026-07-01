# dblog-cdc

A Change-Data-Capture (CDC) replication tool that streams data from Oracle to YugabyteDB. Based on Netflix's [DBLog paper](docs/2010.12597.pdf), with a simplified watermark-free design that uses Oracle's `AS OF SCN` for point-in-time consistent chunk reads.

## How it works

The replicator runs two processes in parallel:

1. **Chunk loading** — reads the full table state in primary-key-ordered chunks using `SELECT ... AS OF SCN`. Tracks progress (last PK) so it can pause and resume.
2. **CDC streaming** — receives real-time row changes from [OpenLogReplicator](https://github.com/bersler/OpenLogReplicator) via its network protobuf protocol.

During chunk loading, CDC events are deduplicated against chunk data: if a CDC event has a higher SCN than the chunk for the same primary key, the CDC version wins. This replaces the DBLog paper's watermark table approach — Oracle's `AS OF SCN` gives us the exact log position of each chunk read, so no watermark writes to the source database are needed.

After all chunks are loaded, the replicator switches to CDC-only mode and continuously applies changes to YugabyteDB using upserts (`INSERT ... ON CONFLICT DO UPDATE`).

## Prerequisites

### Source: Oracle

- Table must have an **integer primary key** column (used for chunk ordering and CDC event dedup).
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

- OLR must be configured with `protobuf` format and `network` writer, pointed at the source database.
- OLR reads online redo logs directly — no log switches needed for real-time streaming.
- See [testenv/olr-config.json](testenv/olr-config.json) for a working configuration.

### Target: YugabyteDB (or PostgreSQL)

- The target table must be created manually with a matching schema and the **same primary key**:
  ```sql
  CREATE TABLE orders (
      id BIGINT PRIMARY KEY,
      amount DOUBLE PRECISION,
      status TEXT
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
    pk_column: ID       # primary key column name (default: ID)
    chunk_size: 10000   # rows per chunk (default: 10000)

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
                     (protobuf/TCP)       (dedup    │
                                          + upsert)─┘
```

Key components:

| Package | Role |
|---------|------|
| `cmd/dblog` | CLI entry point |
| `internal/replicator` | Main loop: chunk loading + CDC interleaving |
| `internal/olr` | OLR protobuf client (handshake, streaming, CONFIRM) |
| `internal/chunk` | Oracle chunk querier (`AS OF SCN`) |
| `internal/buffer` | In-memory dedup buffer (CDC vs chunk events) |
| `internal/writer` | YugabyteDB upsert/delete writer |
| `internal/progress` | Chunk position + SCN tracking (stored in target DB) |

## Differences from the DBLog paper

| DBLog paper | This implementation |
|-------------|---------------------|
| Watermark table writes to bracket chunk reads | `AS OF SCN` gives exact log position — no source writes needed |
| Zookeeper for progress + leader election | Progress stored in target DB (YugabyteDB) |
| Kafka output | Direct writes to YugabyteDB |
| MySQL/PostgreSQL binlog/WAL | Oracle redo logs via OpenLogReplicator |
| Java framework | Go |
