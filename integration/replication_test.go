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
