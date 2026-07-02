//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/godror/godror"

	"github.com/null-ptr-exception/dblog-cdc/internal/chunk"
	"github.com/null-ptr-exception/dblog-cdc/internal/config"
	"github.com/null-ptr-exception/dblog-cdc/internal/olr"
	"github.com/null-ptr-exception/dblog-cdc/internal/progress"
	"github.com/null-ptr-exception/dblog-cdc/internal/replicator"
	"github.com/null-ptr-exception/dblog-cdc/internal/transform"
	"github.com/null-ptr-exception/dblog-cdc/internal/writer"
)

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getOracleDSN() string {
	return envOr("ORACLE_DSN", "oracle://testuser:testuser@localhost:11521/FREEPDB1")
}

func getYBDSN() string {
	return envOr("YB_DSN", "postgres://yugabyte:yugabyte@localhost:15433/yugabyte")
}

func getOLRHost() string {
	return envOr("OLR_HOST", "localhost")
}

func getOLRPort() int {
	s := envOr("OLR_PORT", "15000")
	p, _ := strconv.Atoi(s)
	return p
}

type testEnv struct {
	t        *testing.T
	ctx      context.Context
	oracleDB *sql.DB
	ybPool   *pgxpool.Pool
}

func setupEnv(t *testing.T, ctx context.Context) *testEnv {
	t.Helper()

	oracleDB, err := sql.Open("godror", getOracleDSN())
	if err != nil {
		t.Fatalf("connect oracle: %v", err)
	}
	t.Cleanup(func() { oracleDB.Close() })

	ybPool, err := pgxpool.New(ctx, getYBDSN())
	if err != nil {
		t.Fatalf("connect yb: %v", err)
	}
	t.Cleanup(func() { ybPool.Close() })

	if _, err := ybPool.Exec(ctx, `CREATE TABLE IF NOT EXISTS ORDERS (
		ID BIGINT PRIMARY KEY,
		AMOUNT DOUBLE PRECISION,
		STATUS TEXT
	)`); err != nil {
		t.Fatalf("create yb table: %v", err)
	}

	return &testEnv{t: t, ctx: ctx, oracleDB: oracleDB, ybPool: ybPool}
}

// cleanRange deletes rows in [startPK, startPK+count-1] from both Oracle and YB.
// Uses DELETE (DML) to avoid TRUNCATE/DDL issues.
// Saves the current Oracle SCN to progress so OLR skips old redo on reconnect.
func (e *testEnv) cleanRange(startPK, count int) {
	e.t.Helper()
	endPK := startPK + count - 1
	if _, err := e.oracleDB.ExecContext(e.ctx,
		"DELETE FROM ORDERS WHERE ID BETWEEN :1 AND :2", startPK, endPK); err != nil {
		e.t.Fatalf("clean oracle range %d-%d: %v", startPK, endPK, err)
	}
	if _, err := e.ybPool.Exec(e.ctx,
		"DELETE FROM ORDERS WHERE ID BETWEEN $1 AND $2", startPK, endPK); err != nil {
		e.t.Fatalf("clean yb range %d-%d: %v", startPK, endPK, err)
	}

	var scn int64
	if err := e.oracleDB.QueryRowContext(e.ctx,
		"SELECT current_scn FROM v$database").Scan(&scn); err != nil {
		e.t.Fatalf("get current scn: %v", err)
	}

	pgStore := progress.NewPgStore(e.ybPool, "dblog_progress")
	pgStore.EnsureTable(e.ctx)
	pgStore.Save(e.ctx, "ORDERS", nil, uint64(scn))
}

// scheduleCleanup registers a post-test cleanup using a fresh context
// (since the test context may be cancelled by then).
func (e *testEnv) scheduleCleanup(startPK, count int) {
	e.t.Helper()
	e.t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		endPK := startPK + count - 1
		e.oracleDB.ExecContext(ctx, "DELETE FROM ORDERS WHERE ID BETWEEN :1 AND :2", startPK, endPK)
		e.ybPool.Exec(ctx, "DELETE FROM ORDERS WHERE ID BETWEEN $1 AND $2", startPK, endPK)
	})
}

func (e *testEnv) seedRows(startPK, count int, amountFactor float64, status string) {
	e.t.Helper()
	for i := 0; i < count; i++ {
		pk := startPK + i
		if _, err := e.oracleDB.ExecContext(e.ctx,
			"INSERT INTO ORDERS (ID, AMOUNT, STATUS) VALUES (:1, :2, :3)",
			pk, float64(pk)*amountFactor, status); err != nil {
			e.t.Fatalf("seed row %d: %v", pk, err)
		}
	}
	e.t.Logf("seeded %d rows (PK %d-%d) in Oracle", count, startPK, startPK+count-1)
}

type replicatorHandle struct {
	cancel    context.CancelFunc
	cdcClient *olr.Client
}

func (e *testEnv) startReplicator(chunkSize int) *replicatorHandle {
	e.t.Helper()

	pgStore := progress.NewPgStore(e.ybPool, "dblog_progress")
	if err := pgStore.EnsureTable(e.ctx); err != nil {
		e.t.Fatalf("ensure progress: %v", err)
	}

	cdcClient := olr.NewClient(getOLRHost(), getOLRPort(), "FREE", []string{"ORDERS"}, map[string]string{"ORDERS": "ID"})
	querier := chunk.NewOracleQuerier(e.oracleDB, "ID")
	ybWriter := writer.NewPgWriter(e.ybPool, "ID")

	tbl := config.Table{Name: "ORDERS", ChunkSize: chunkSize}
	r := replicator.New(cdcClient, querier, ybWriter, pgStore, tbl)

	rCtx, rCancel := context.WithCancel(e.ctx)
	go func() {
		if err := r.Run(rCtx); err != nil && rCtx.Err() == nil {
			e.t.Errorf("replicator error: %v", err)
		}
	}()

	return &replicatorHandle{cancel: rCancel, cdcClient: cdcClient}
}

func (e *testEnv) startReplicatorForTable(table, pkCol string, chunkSize int) *replicatorHandle {
	e.t.Helper()

	pgStore := progress.NewPgStore(e.ybPool, "dblog_progress")
	if err := pgStore.EnsureTable(e.ctx); err != nil {
		e.t.Fatalf("ensure progress: %v", err)
	}

	tables := []string{table}
	pkColumns := map[string]string{table: pkCol}
	cdcClient := olr.NewClient(getOLRHost(), getOLRPort(), "FREE", tables, pkColumns)
	querier := chunk.NewOracleQuerier(e.oracleDB, pkCol)
	ybWriter := writer.NewPgWriter(e.ybPool, pkCol)

	typeMap, err := transform.LoadTypeMap(e.ctx, e.oracleDB, tables)
	if err != nil {
		e.t.Fatalf("load type map: %v", err)
	}

	tbl := config.Table{Name: table, ChunkSize: chunkSize}
	r := replicator.New(cdcClient, querier, ybWriter, pgStore, tbl)
	r.SetTransformer(transform.New(typeMap))

	rCtx, rCancel := context.WithCancel(e.ctx)
	go func() {
		if err := r.Run(rCtx); err != nil && rCtx.Err() == nil {
			e.t.Errorf("replicator error: %v", err)
		}
	}()

	return &replicatorHandle{cancel: rCancel, cdcClient: cdcClient}
}

func (e *testEnv) ybRowCount(startPK, count int) int {
	e.t.Helper()
	endPK := startPK + count - 1
	var n int
	if err := e.ybPool.QueryRow(e.ctx,
		"SELECT COUNT(*) FROM ORDERS WHERE ID BETWEEN $1 AND $2",
		startPK, endPK).Scan(&n); err != nil {
		e.t.Fatalf("yb count: %v", err)
	}
	return n
}

func (e *testEnv) waitForYBCount(startPK, count, target int, timeout time.Duration) {
	e.t.Helper()
	deadline := time.After(timeout)
	for {
		n := e.ybRowCount(startPK, count)
		if n >= target {
			return
		}
		select {
		case <-deadline:
			e.t.Fatalf("timeout waiting for YB to have %d rows in range %d-%d (have %d)",
				target, startPK, startPK+count-1, n)
		default:
			time.Sleep(500 * time.Millisecond)
		}
	}
}

// waitForCDCReady updates an existing row's status and waits for the update
// to appear in YB via CDC. Since chunks already loaded the original value,
// the updated value can only arrive via CDC — proving OLR has caught up.
// Reverts the status afterward.
func (e *testEnv) waitForCDCReady(markerPK int, originalStatus string, timeout time.Duration) {
	e.t.Helper()
	if _, err := e.oracleDB.ExecContext(e.ctx,
		"UPDATE ORDERS SET STATUS = :1 WHERE ID = :2",
		"CDC_MARKER", markerPK); err != nil {
		e.t.Fatalf("update CDC marker: %v", err)
	}

	deadline := time.After(timeout)
	for {
		var status string
		e.ybPool.QueryRow(e.ctx, "SELECT STATUS FROM ORDERS WHERE ID = $1", markerPK).Scan(&status)
		if status == "CDC_MARKER" {
			break
		}
		select {
		case <-deadline:
			e.t.Fatalf("timeout waiting for CDC marker (PK=%d) — OLR has not caught up", markerPK)
		default:
			time.Sleep(500 * time.Millisecond)
		}
	}

	if _, err := e.oracleDB.ExecContext(e.ctx,
		"UPDATE ORDERS SET STATUS = :1 WHERE ID = :2",
		originalStatus, markerPK); err != nil {
		e.t.Fatalf("revert CDC marker: %v", err)
	}
	e.t.Logf("CDC ready (marker PK=%d delivered)", markerPK)
}

func (e *testEnv) waitForYBRowValue(id int, wantStatus string, timeout time.Duration) {
	e.t.Helper()
	deadline := time.After(timeout)
	for {
		var status string
		e.ybPool.QueryRow(e.ctx, "SELECT STATUS FROM ORDERS WHERE ID = $1", id).Scan(&status)
		if status == wantStatus {
			return
		}
		select {
		case <-deadline:
			e.t.Fatalf("timeout waiting for row %d status=%q (have %q)", id, wantStatus, status)
		default:
			time.Sleep(500 * time.Millisecond)
		}
	}
}

func (e *testEnv) assertYBRow(id int, wantAmount float64, wantStatus string) {
	e.t.Helper()
	var amount float64
	var status string
	err := e.ybPool.QueryRow(e.ctx,
		"SELECT AMOUNT, STATUS FROM ORDERS WHERE ID = $1", id).Scan(&amount, &status)
	if err != nil {
		e.t.Errorf("row %d: not found: %v", id, err)
		return
	}
	if amount != wantAmount {
		e.t.Errorf("row %d: amount = %f, want %f", id, amount, wantAmount)
	}
	if status != wantStatus {
		e.t.Errorf("row %d: status = %q, want %q", id, status, wantStatus)
	}
}

func (e *testEnv) assertYBRowAbsent(id int) {
	e.t.Helper()
	var count int
	if err := e.ybPool.QueryRow(e.ctx,
		"SELECT COUNT(*) FROM ORDERS WHERE ID = $1", id).Scan(&count); err != nil {
		e.t.Fatalf("check row %d: %v", id, err)
	}
	if count != 0 {
		e.t.Errorf("row %d: expected absent, still exists", id)
	}
}

func (e *testEnv) assertConvergence(startPK, count int) {
	e.t.Helper()
	endPK := startPK + count - 1

	var oracleCount, ybCount int
	if err := e.oracleDB.QueryRowContext(e.ctx,
		"SELECT COUNT(*) FROM ORDERS WHERE ID BETWEEN :1 AND :2",
		startPK, endPK).Scan(&oracleCount); err != nil {
		e.t.Fatalf("oracle count: %v", err)
	}
	ybCount = e.ybRowCount(startPK, count)
	if oracleCount != ybCount {
		e.t.Errorf("row count mismatch: oracle=%d yb=%d", oracleCount, ybCount)
	} else {
		e.t.Logf("row counts match: %d", oracleCount)
	}

	var oracleSum, ybSum float64
	if err := e.oracleDB.QueryRowContext(e.ctx,
		"SELECT NVL(SUM(AMOUNT),0) FROM ORDERS WHERE ID BETWEEN :1 AND :2",
		startPK, endPK).Scan(&oracleSum); err != nil {
		e.t.Fatalf("oracle sum: %v", err)
	}
	if err := e.ybPool.QueryRow(e.ctx,
		"SELECT COALESCE(SUM(AMOUNT),0) FROM ORDERS WHERE ID BETWEEN $1 AND $2",
		startPK, endPK).Scan(&ybSum); err != nil {
		e.t.Fatalf("yb sum: %v", err)
	}
	if fmt.Sprintf("%.2f", oracleSum) != fmt.Sprintf("%.2f", ybSum) {
		e.t.Errorf("sum mismatch: oracle=%.2f yb=%.2f", oracleSum, ybSum)
	} else {
		e.t.Logf("sums match: %.0f", oracleSum)
	}
}

// PK ranges: each test uses non-overlapping ranges to avoid CDC cross-contamination.
// ChunkLoading:              1000-1099
// CDC:                       2000-2099
// ConcurrentMutations:       3000-3199
// FullConvergence:           4000-4099

func TestReplication_ChunkLoading(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	defer cancel()

	const startPK, count = 1000, 100
	env := setupEnv(t, ctx)
	env.cleanRange(startPK, count)
	env.seedRows(startPK, count, 10.0, "INIT")
	env.scheduleCleanup(startPK, count)

	rh := env.startReplicator(25)
	defer rh.cancel()

	env.waitForYBCount(startPK, count, count, 30*time.Second)

	rh.cancel()
	time.Sleep(500 * time.Millisecond)

	env.assertConvergence(startPK, count)
	env.assertYBRow(1000, 10000, "INIT")
	env.assertYBRow(1050, 10500, "INIT")
	env.assertYBRow(1099, 10990, "INIT")
}

func TestReplication_CDC(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	defer cancel()

	const startPK, count = 2000, 50
	env := setupEnv(t, ctx)
	env.cleanRange(startPK, 100)
	env.seedRows(startPK, count, 100.0, "SEED")
	env.scheduleCleanup(startPK, 100)

	rh := env.startReplicator(25)
	defer rh.cancel()

	env.waitForYBCount(startPK, count, count, 30*time.Second)
	t.Log("chunk loading complete")

	if err := rh.cdcClient.WaitStreaming(ctx); err != nil {
		t.Fatalf("wait for CDC streaming: %v", err)
	}
	t.Log("CDC streaming ready")

	env.waitForCDCReady(2001, "SEED", 90*time.Second)

	// INSERT
	for _, id := range []int{2051, 2052, 2053} {
		if _, err := env.oracleDB.ExecContext(ctx,
			"INSERT INTO ORDERS (ID, AMOUNT, STATUS) VALUES (:1, :2, :3)",
			id, float64(id)*100.0, "INSERTED"); err != nil {
			t.Fatalf("insert row %d: %v", id, err)
		}
	}

	// UPDATE
	for _, id := range []int{2010, 2020, 2030} {
		if _, err := env.oracleDB.ExecContext(ctx,
			"UPDATE ORDERS SET AMOUNT = :1, STATUS = :2 WHERE ID = :3",
			9999.0, "UPDATED", id); err != nil {
			t.Fatalf("update row %d: %v", id, err)
		}
	}

	// DELETE
	for _, id := range []int{2005, 2015} {
		if _, err := env.oracleDB.ExecContext(ctx,
			"DELETE FROM ORDERS WHERE ID = :1", id); err != nil {
			t.Fatalf("delete row %d: %v", id, err)
		}
	}

	t.Log("mutations applied: 3 inserts, 3 updates, 2 deletes")

	env.waitForYBRowValue(2010, "UPDATED", 60*time.Second)
	time.Sleep(2 * time.Second)

	rh.cancel()
	time.Sleep(500 * time.Millisecond)

	env.assertConvergence(startPK, 100)

	for _, id := range []int{2051, 2052, 2053} {
		env.assertYBRow(id, float64(id)*100.0, "INSERTED")
	}
	for _, id := range []int{2010, 2020, 2030} {
		env.assertYBRow(id, 9999.0, "UPDATED")
	}
	for _, id := range []int{2005, 2015} {
		env.assertYBRowAbsent(id)
	}
	env.assertYBRow(2001, 200100, "SEED")
	env.assertYBRow(2049, 204900, "SEED")
}

// TestReplication_ConcurrentMutations verifies that mutations applied while
// chunk loading is in progress converge correctly. The replicator must handle
// the race between chunk reads (AS OF a point-in-time SCN) and concurrent DML
// — either through buffer-level watermark dedup or through CDC upserts after
// chunks complete. The test validates the end result, not which path was taken.
func TestReplication_ConcurrentMutations(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	defer cancel()

	const startPK, count = 3000, 100
	env := setupEnv(t, ctx)
	env.cleanRange(startPK, 200)
	env.seedRows(startPK, count, 100.0, "ORIGINAL")
	env.scheduleCleanup(startPK, 200)

	rh := env.startReplicator(10)
	defer rh.cancel()

	// Wait for early rows to appear — chunk loading is in progress
	env.waitForYBCount(startPK, 20, 10, 30*time.Second)
	t.Log("chunk loading in progress, applying concurrent mutations")

	// Apply mutations to rows across the PK range while chunks are loading.
	// Some of these rows may already be chunk-loaded (stale), others haven't
	// been reached yet. Either way the final state must match Oracle.
	for _, id := range []int{3005, 3025, 3045, 3065, 3085} {
		if _, err := env.oracleDB.ExecContext(ctx,
			"UPDATE ORDERS SET AMOUNT = :1, STATUS = :2 WHERE ID = :3",
			7777.0, "CONCURRENT_UPDATE", id); err != nil {
			t.Fatalf("update row %d: %v", id, err)
		}
	}
	for _, id := range []int{3010, 3050, 3090} {
		if _, err := env.oracleDB.ExecContext(ctx,
			"DELETE FROM ORDERS WHERE ID = :1", id); err != nil {
			t.Fatalf("delete row %d: %v", id, err)
		}
	}
	for _, id := range []int{3101, 3102} {
		if _, err := env.oracleDB.ExecContext(ctx,
			"INSERT INTO ORDERS (ID, AMOUNT, STATUS) VALUES (:1, :2, :3)",
			id, float64(id)*100.0, "CONCURRENT_INSERT"); err != nil {
			t.Fatalf("insert row %d: %v", id, err)
		}
	}
	t.Log("concurrent mutations applied: 5 updates, 3 deletes, 2 inserts")

	// Wait for the last inserted row (highest SCN) to appear via CDC —
	// this guarantees all prior events (updates, deletes) have also arrived.
	env.waitForYBRowValue(3102, "CONCURRENT_INSERT", 60*time.Second)

	rh.cancel()
	time.Sleep(500 * time.Millisecond)

	env.assertConvergence(startPK, 200)

	for _, id := range []int{3005, 3025, 3045, 3065, 3085} {
		env.assertYBRow(id, 7777.0, "CONCURRENT_UPDATE")
	}
	for _, id := range []int{3010, 3050, 3090} {
		env.assertYBRowAbsent(id)
	}
	for _, id := range []int{3101, 3102} {
		env.assertYBRow(id, float64(id)*100.0, "CONCURRENT_INSERT")
	}
	env.assertYBRow(3001, 300100, "ORIGINAL")
	env.assertYBRow(3099, 309900, "ORIGINAL")
}

func TestReplication_FullConvergence(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	defer cancel()

	const startPK, count = 4000, 30
	env := setupEnv(t, ctx)
	env.cleanRange(startPK, 100)
	env.seedRows(startPK, count, 100.0, "SEED")
	env.scheduleCleanup(startPK, 100)

	rh := env.startReplicator(10)
	defer rh.cancel()

	env.waitForYBCount(startPK, count, count, 30*time.Second)
	t.Log("chunk loading complete")

	if err := rh.cdcClient.WaitStreaming(ctx); err != nil {
		t.Fatalf("wait for CDC streaming: %v", err)
	}

	env.waitForCDCReady(4001, "SEED", 90*time.Second)

	// Apply mutations
	for i := startPK + count; i < startPK+count+5; i++ {
		if _, err := env.oracleDB.ExecContext(ctx,
			"INSERT INTO ORDERS (ID, AMOUNT, STATUS) VALUES (:1, :2, :3)",
			i, float64(i)*100.0, "NEW"); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}
	for _, id := range []int{4000, 4010, 4020} {
		if _, err := env.oracleDB.ExecContext(ctx,
			"UPDATE ORDERS SET AMOUNT = 1, STATUS = 'CHANGED' WHERE ID = :1", id); err != nil {
			t.Fatalf("update: %v", err)
		}
	}
	for _, id := range []int{4005, 4015, 4025} {
		if _, err := env.oracleDB.ExecContext(ctx,
			"DELETE FROM ORDERS WHERE ID = :1", id); err != nil {
			t.Fatalf("delete: %v", err)
		}
	}

	env.waitForYBRowValue(4000, "CHANGED", 60*time.Second)
	time.Sleep(3 * time.Second)

	rh.cancel()
	time.Sleep(500 * time.Millisecond)

	// Full row-by-row comparison
	type row struct {
		ID     int64
		Amount float64
		Status string
	}

	oracleRows, err := env.oracleDB.QueryContext(ctx,
		"SELECT ID, AMOUNT, STATUS FROM ORDERS WHERE ID BETWEEN :1 AND :2 ORDER BY ID",
		startPK, startPK+99)
	if err != nil {
		t.Fatalf("query oracle: %v", err)
	}
	defer oracleRows.Close()

	var expected []row
	for oracleRows.Next() {
		var r row
		if err := oracleRows.Scan(&r.ID, &r.Amount, &r.Status); err != nil {
			t.Fatalf("scan oracle: %v", err)
		}
		expected = append(expected, r)
	}

	ybRows, err := env.ybPool.Query(ctx,
		"SELECT ID, AMOUNT, STATUS FROM ORDERS WHERE ID BETWEEN $1 AND $2 ORDER BY ID",
		startPK, startPK+99)
	if err != nil {
		t.Fatalf("query yb: %v", err)
	}
	defer ybRows.Close()

	var actual []row
	for ybRows.Next() {
		var r row
		if err := ybRows.Scan(&r.ID, &r.Amount, &r.Status); err != nil {
			t.Fatalf("scan yb: %v", err)
		}
		actual = append(actual, r)
	}

	if len(expected) != len(actual) {
		t.Fatalf("row count: oracle=%d yb=%d", len(expected), len(actual))
	}

	mismatches := 0
	for i := range expected {
		if expected[i] != actual[i] {
			t.Errorf("row %d mismatch: oracle=%+v yb=%+v", i, expected[i], actual[i])
			mismatches++
		}
	}
	if mismatches == 0 {
		t.Logf("all %d rows match exactly", len(expected))
	} else {
		t.Errorf("%d/%d rows differ", mismatches, len(expected))
	}

	env.assertConvergence(startPK, 100)

	t.Logf("convergence summary: oracle=%d yb=%d mismatches=%d",
		len(expected), len(actual), mismatches)
}
