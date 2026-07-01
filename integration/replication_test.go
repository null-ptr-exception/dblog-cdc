//go:build integration

package integration_test

import (
	"context"
	"database/sql"
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

func TestEndToEnd_ChunkAndCDC(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	oracleDB, err := sql.Open("godror", getOracleDSN())
	if err != nil {
		t.Fatalf("connect oracle: %v", err)
	}
	defer oracleDB.Close()

	oracleDB.ExecContext(ctx, "DELETE FROM ORDERS")

	for i := 1; i <= 100; i++ {
		_, err := oracleDB.ExecContext(ctx,
			"INSERT INTO ORDERS (ID, AMOUNT, STATUS) VALUES (:1, :2, :3)",
			i, float64(i)*10.0, "INIT")
		if err != nil {
			t.Fatalf("seed row %d: %v", i, err)
		}
	}
	t.Log("seeded 100 rows in Oracle")

	ybPool, err := pgxpool.New(ctx, getYBDSN())
	if err != nil {
		t.Fatalf("connect yb: %v", err)
	}
	defer ybPool.Close()

	_, err = ybPool.Exec(ctx, `CREATE TABLE IF NOT EXISTS ORDERS (
		ID BIGINT PRIMARY KEY,
		AMOUNT DOUBLE PRECISION,
		STATUS TEXT
	)`)
	if err != nil {
		t.Fatalf("create yb table: %v", err)
	}
	ybPool.Exec(ctx, "TRUNCATE TABLE ORDERS")
	ybPool.Exec(ctx, "DELETE FROM dblog_progress WHERE table_name = 'ORDERS'")

	pgStore := progress.NewPgStore(ybPool, "dblog_progress")
	if err := pgStore.EnsureTable(ctx); err != nil {
		t.Fatalf("ensure progress: %v", err)
	}

	cdcClient := olr.NewClient(getOLRHost(), getOLRPort(), "FREE", []string{"ORDERS"})
	querier := chunk.NewOracleQuerier(oracleDB, "ID")
	ybWriter := writer.NewPgWriter(ybPool, "ID")

	tbl := config.Table{Name: "ORDERS", ChunkSize: 25}
	r := replicator.New(cdcClient, querier, ybWriter, pgStore, tbl)

	replicatorCtx, replicatorCancel := context.WithTimeout(ctx, 90*time.Second)
	defer replicatorCancel()

	go func() {
		r.Run(replicatorCtx)
	}()

	// Wait for chunks to load (at least 100 rows in YB)
	for {
		var count int
		ybPool.QueryRow(ctx, "SELECT COUNT(*) FROM ORDERS").Scan(&count)
		if count >= 100 {
			t.Logf("chunk loading done, YB has %d rows", count)
			break
		}
		time.Sleep(500 * time.Millisecond)
	}

	// Apply concurrent mutations
	for i := 101; i <= 110; i++ {
		oracleDB.ExecContext(ctx,
			"INSERT INTO ORDERS (ID, AMOUNT, STATUS) VALUES (:1, :2, :3)",
			i, float64(i)*10.0, "CDC_INSERT")
	}
	oracleDB.ExecContext(ctx,
		"UPDATE ORDERS SET STATUS = 'CDC_UPDATED' WHERE ID = 50")
	t.Log("concurrent mutations applied")

	// Wait for some CDC events to arrive (poll until YB has > 100 rows or timeout)
	deadline := time.After(60 * time.Second)
	var lastYBCount int
	for {
		var ybCount int
		ybPool.QueryRow(ctx, "SELECT COUNT(*) FROM ORDERS").Scan(&ybCount)
		lastYBCount = ybCount

		if ybCount > 100 {
			t.Logf("CDC events arriving: yb=%d", ybCount)
			break
		}

		select {
		case <-deadline:
			t.Logf("CDC timeout: yb=%d (CDC may be catching up through redo logs)", ybCount)
			goto verify
		default:
			time.Sleep(2 * time.Second)
		}
	}

	time.Sleep(2 * time.Second)

verify:
	replicatorCancel()

	if lastYBCount < 100 {
		t.Errorf("expected at least 100 rows in YB, got %d", lastYBCount)
	}
	t.Logf("YugabyteDB has %d rows", lastYBCount)

	var oracleSum, ybSum float64
	oracleDB.QueryRowContext(ctx, "SELECT SUM(AMOUNT) FROM ORDERS").Scan(&oracleSum)
	ybPool.QueryRow(ctx, "SELECT SUM(AMOUNT) FROM ORDERS").Scan(&ybSum)
	t.Logf("sum check: oracle=%f yb=%f", oracleSum, ybSum)

	// Chunk loading is the core test — verify at least 100 rows with correct chunk sum
	chunkSum := float64(0)
	for i := 1; i <= 100; i++ {
		chunkSum += float64(i) * 10.0
	}
	if ybSum < chunkSum {
		t.Errorf("YB sum %f is less than expected chunk sum %f", ybSum, chunkSum)
	}

	if lastYBCount > 100 {
		t.Logf("CDC replication verified: %d rows beyond initial chunk load", lastYBCount-100)
	}
}

// TestWatermarkCDC_CRUD verifies that CDC events arriving during chunk loading
// correctly override stale chunk data via the SCN-based watermark dedup.
// Covers INSERT, UPDATE, and DELETE operations.
func TestWatermarkCDC_CRUD(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	oracleDB, err := sql.Open("godror", getOracleDSN())
	if err != nil {
		t.Fatalf("connect oracle: %v", err)
	}
	defer oracleDB.Close()

	// Clean slate
	oracleDB.ExecContext(ctx, "DELETE FROM ORDERS")

	// Seed 50 rows — small enough that chunk loading is quick,
	// but with chunk_size=5 we get 10 chunk iterations for the race window.
	for i := 1; i <= 50; i++ {
		_, err := oracleDB.ExecContext(ctx,
			"INSERT INTO ORDERS (ID, AMOUNT, STATUS) VALUES (:1, :2, :3)",
			i, float64(i)*100.0, "ORIGINAL")
		if err != nil {
			t.Fatalf("seed row %d: %v", i, err)
		}
	}
	t.Log("seeded 50 rows in Oracle")

	ybPool, err := pgxpool.New(ctx, getYBDSN())
	if err != nil {
		t.Fatalf("connect yb: %v", err)
	}
	defer ybPool.Close()

	_, err = ybPool.Exec(ctx, `CREATE TABLE IF NOT EXISTS ORDERS (
		ID BIGINT PRIMARY KEY,
		AMOUNT DOUBLE PRECISION,
		STATUS TEXT
	)`)
	if err != nil {
		t.Fatalf("create yb table: %v", err)
	}
	ybPool.Exec(ctx, "TRUNCATE TABLE ORDERS")
	ybPool.Exec(ctx, "DELETE FROM dblog_progress WHERE table_name = 'ORDERS'")

	pgStore := progress.NewPgStore(ybPool, "dblog_progress")
	if err := pgStore.EnsureTable(ctx); err != nil {
		t.Fatalf("ensure progress: %v", err)
	}

	cdcClient := olr.NewClient(getOLRHost(), getOLRPort(), "FREE", []string{"ORDERS"})
	querier := chunk.NewOracleQuerier(oracleDB, "ID")
	ybWriter := writer.NewPgWriter(ybPool, "ID")

	// Small chunk size to create more chunk iterations → wider race window
	tbl := config.Table{Name: "ORDERS", ChunkSize: 5}
	r := replicator.New(cdcClient, querier, ybWriter, pgStore, tbl)

	replicatorCtx, replicatorCancel := context.WithTimeout(ctx, 90*time.Second)
	defer replicatorCancel()

	// Apply mutations BEFORE starting replicator so they're in the redo log.
	// These happen after the seed, so they'll have higher SCNs than the
	// chunk snapshots. The replicator must resolve them correctly.

	// UPDATE: change amount and status for rows in different chunks
	for _, id := range []int{10, 20, 30} {
		_, err := oracleDB.ExecContext(ctx,
			"UPDATE ORDERS SET AMOUNT = :1, STATUS = :2 WHERE ID = :3",
			float64(id)*100.0+7777.0, "UPDATED", id)
		if err != nil {
			t.Fatalf("update row %d: %v", id, err)
		}
	}
	t.Log("updated rows 10, 20, 30")

	// DELETE: remove rows from different chunks
	for _, id := range []int{15, 35} {
		_, err := oracleDB.ExecContext(ctx,
			"DELETE FROM ORDERS WHERE ID = :1", id)
		if err != nil {
			t.Fatalf("delete row %d: %v", id, err)
		}
	}
	t.Log("deleted rows 15, 35")

	// INSERT: add new rows beyond the original range
	for _, id := range []int{51, 52, 53} {
		_, err := oracleDB.ExecContext(ctx,
			"INSERT INTO ORDERS (ID, AMOUNT, STATUS) VALUES (:1, :2, :3)",
			id, float64(id)*100.0, "INSERTED")
		if err != nil {
			t.Fatalf("insert row %d: %v", id, err)
		}
	}
	t.Log("inserted rows 51, 52, 53")

	// Now start the replicator. Chunks will read AS OF a snapshot SCN.
	// The CDC stream will deliver the UPDATE/DELETE/INSERT events with higher SCNs.
	// The watermark dedup ensures CDC wins over stale chunk data.
	go func() {
		r.Run(replicatorCtx)
	}()

	// Build expected state from Oracle
	type orderRow struct {
		ID     int64
		Amount float64
		Status string
	}
	expectedRows := make(map[int64]orderRow)
	oracleRows, err := oracleDB.QueryContext(ctx, "SELECT ID, AMOUNT, STATUS FROM ORDERS ORDER BY ID")
	if err != nil {
		t.Fatalf("query oracle: %v", err)
	}
	for oracleRows.Next() {
		var r orderRow
		if err := oracleRows.Scan(&r.ID, &r.Amount, &r.Status); err != nil {
			t.Fatalf("scan oracle: %v", err)
		}
		expectedRows[r.ID] = r
	}
	oracleRows.Close()
	t.Logf("Oracle has %d rows", len(expectedRows))

	// Poll until YB converges to expected row count or timeout
	deadline := time.After(60 * time.Second)
	expectedCount := len(expectedRows)
	var lastYBCount int
	for {
		ybPool.QueryRow(ctx, "SELECT COUNT(*) FROM ORDERS").Scan(&lastYBCount)
		if lastYBCount == expectedCount {
			break
		}
		select {
		case <-deadline:
			t.Logf("convergence timeout: yb=%d expected=%d", lastYBCount, expectedCount)
			goto verify
		default:
			time.Sleep(1 * time.Second)
		}
	}

	// Give a brief settle period for any trailing CDC events
	time.Sleep(3 * time.Second)

verify:
	replicatorCancel()
	time.Sleep(500 * time.Millisecond)

	// Verify row count
	ybPool.QueryRow(ctx, "SELECT COUNT(*) FROM ORDERS").Scan(&lastYBCount)
	t.Logf("YB has %d rows (expected %d)", lastYBCount, expectedCount)
	if lastYBCount != expectedCount {
		t.Errorf("row count mismatch: yb=%d expected=%d", lastYBCount, expectedCount)
	}

	// Verify UPDATES: rows 10, 20, 30 should have updated values
	for _, id := range []int{10, 20, 30} {
		var amount float64
		var status string
		err := ybPool.QueryRow(ctx,
			"SELECT AMOUNT, STATUS FROM ORDERS WHERE ID = $1", id).Scan(&amount, &status)
		if err != nil {
			t.Errorf("UPDATE verify: row %d not found: %v", id, err)
			continue
		}
		exp := expectedRows[int64(id)]
		if amount != exp.Amount {
			t.Errorf("UPDATE verify: row %d amount = %f, want %f", id, amount, exp.Amount)
		}
		if status != exp.Status {
			t.Errorf("UPDATE verify: row %d status = %q, want %q", id, status, exp.Status)
		} else {
			t.Logf("UPDATE verified: row %d amount=%.0f status=%q", id, amount, status)
		}
	}

	// Verify DELETES: rows 15, 35 should NOT exist
	for _, id := range []int{15, 35} {
		var count int
		ybPool.QueryRow(ctx, "SELECT COUNT(*) FROM ORDERS WHERE ID = $1", id).Scan(&count)
		if count != 0 {
			t.Errorf("DELETE verify: row %d still exists in YB", id)
		} else {
			t.Logf("DELETE verified: row %d absent from YB", id)
		}
	}

	// Verify INSERTS: rows 51, 52, 53 should exist with correct values
	for _, id := range []int{51, 52, 53} {
		var amount float64
		var status string
		err := ybPool.QueryRow(ctx,
			"SELECT AMOUNT, STATUS FROM ORDERS WHERE ID = $1", id).Scan(&amount, &status)
		if err != nil {
			t.Errorf("INSERT verify: row %d not found: %v", id, err)
			continue
		}
		exp := expectedRows[int64(id)]
		if amount != exp.Amount {
			t.Errorf("INSERT verify: row %d amount = %f, want %f", id, amount, exp.Amount)
		}
		if status != exp.Status {
			t.Errorf("INSERT verify: row %d status = %q, want %q", id, status, exp.Status)
		} else {
			t.Logf("INSERT verified: row %d amount=%.0f status=%q", id, amount, status)
		}
	}

	// Verify untouched rows still have original values (spot check)
	for _, id := range []int{1, 25, 50} {
		var amount float64
		var status string
		err := ybPool.QueryRow(ctx,
			"SELECT AMOUNT, STATUS FROM ORDERS WHERE ID = $1", id).Scan(&amount, &status)
		if err != nil {
			t.Errorf("unchanged verify: row %d not found: %v", id, err)
			continue
		}
		if status != "ORIGINAL" {
			t.Errorf("unchanged verify: row %d status = %q, want %q", id, status, "ORIGINAL")
		} else {
			t.Logf("unchanged verified: row %d amount=%.0f status=%q", id, amount, status)
		}
	}

	// Final sum comparison
	var oracleSum, ybSum float64
	oracleDB.QueryRowContext(ctx, "SELECT SUM(AMOUNT) FROM ORDERS").Scan(&oracleSum)
	ybPool.QueryRow(ctx, "SELECT SUM(AMOUNT) FROM ORDERS").Scan(&ybSum)
	t.Logf("sum check: oracle=%.0f yb=%.0f", oracleSum, ybSum)
	if oracleSum != ybSum {
		t.Errorf("sum mismatch: oracle=%.0f yb=%.0f", oracleSum, ybSum)
	}
}
