//go:build integration

package integration_test

import (
	"context"
	"database/sql"
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

	oracleDB, err := sql.Open("godror", oracleDSN)
	if err != nil {
		t.Fatalf("connect oracle: %v", err)
	}
	defer oracleDB.Close()

	for i := 1; i <= 100; i++ {
		_, err := oracleDB.ExecContext(ctx,
			"INSERT INTO ORDERS (ID, AMOUNT, STATUS) VALUES (:1, :2, :3)",
			i, float64(i)*10.0, "INIT")
		if err != nil {
			t.Fatalf("seed row %d: %v", i, err)
		}
	}
	t.Log("seeded 100 rows in Oracle")

	ybPool, err := pgxpool.New(ctx, ybDSN)
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

	pgStore := progress.NewPgStore(ybPool, "dblog_progress")
	if err := pgStore.EnsureTable(ctx); err != nil {
		t.Fatalf("ensure progress: %v", err)
	}

	cdcClient := olr.NewClient(olrHost, olrPort, "", []string{"ORDERS"})
	querier := chunk.NewOracleQuerier(oracleDB, "ID")
	ybWriter := writer.NewPgWriter(ybPool, "ID")

	tbl := config.Table{Name: "ORDERS", ChunkSize: 25}
	r := replicator.New(cdcClient, querier, ybWriter, pgStore, tbl)

	go func() {
		time.Sleep(2 * time.Second)
		for i := 101; i <= 110; i++ {
			oracleDB.ExecContext(ctx,
				"INSERT INTO ORDERS (ID, AMOUNT, STATUS) VALUES (:1, :2, :3)",
				i, float64(i)*10.0, "CDC_INSERT")
		}
		oracleDB.ExecContext(ctx,
			"UPDATE ORDERS SET STATUS = 'CDC_UPDATED' WHERE ID = 50")
		t.Log("concurrent mutations applied")
	}()

	replicatorCtx, replicatorCancel := context.WithTimeout(ctx, 60*time.Second)
	defer replicatorCancel()

	err = r.Run(replicatorCtx)
	if err != nil && err != context.DeadlineExceeded {
		t.Fatalf("replicator: %v", err)
	}

	var count int
	err = ybPool.QueryRow(ctx, "SELECT COUNT(*) FROM ORDERS").Scan(&count)
	if err != nil {
		t.Fatalf("count yb: %v", err)
	}

	if count < 100 {
		t.Errorf("expected at least 100 rows in YB, got %d", count)
	}
	t.Logf("YugabyteDB has %d rows", count)

	var status string
	err = ybPool.QueryRow(ctx, "SELECT STATUS FROM ORDERS WHERE ID = 50").Scan(&status)
	if err != nil {
		t.Fatalf("query PK 50: %v", err)
	}
	t.Logf("PK 50 STATUS = %q", status)

	var oracleSum, ybSum float64
	oracleDB.QueryRowContext(ctx, "SELECT SUM(AMOUNT) FROM ORDERS").Scan(&oracleSum)
	ybPool.QueryRow(ctx, "SELECT SUM(AMOUNT) FROM ORDERS").Scan(&ybSum)

	if oracleSum != ybSum {
		t.Errorf("sum mismatch: oracle=%f yb=%f", oracleSum, ybSum)
	}
	t.Logf("sum check: oracle=%f yb=%f", oracleSum, ybSum)
}
