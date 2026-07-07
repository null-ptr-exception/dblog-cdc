//go:build integration

package integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/null-ptr-exception/dblog-cdc/internal/progress"
)

// TestReplication_CompoundPK verifies the full pipeline with a compound
// (multi-column) primary key table: chunk loading, CDC insert/update/delete.
//
// PK range: ORDER_ID 9000-9003, ITEM_ID 1-3
func TestReplication_CompoundPK(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	env := setupEnv(t, ctx)

	// Create Oracle table with compound PK
	env.oracleDB.ExecContext(ctx, `
		DECLARE
			e EXCEPTION; PRAGMA EXCEPTION_INIT(e, -955);
		BEGIN
			EXECUTE IMMEDIATE 'CREATE TABLE COMPOUND_PK_TEST (
				ORDER_ID NUMBER(10) NOT NULL,
				ITEM_ID  NUMBER(10) NOT NULL,
				QTY      NUMBER(5),
				PRICE    NUMBER(10,2),
				CONSTRAINT pk_compound PRIMARY KEY (ORDER_ID, ITEM_ID)
			)';
			EXECUTE IMMEDIATE 'ALTER TABLE COMPOUND_PK_TEST ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS';
		EXCEPTION WHEN e THEN NULL;
		END;`)

	if _, err := env.ybPool.Exec(ctx, `CREATE TABLE IF NOT EXISTS COMPOUND_PK_TEST (
		ORDER_ID BIGINT,
		ITEM_ID  BIGINT,
		QTY      INTEGER,
		PRICE    NUMERIC(10,2),
		PRIMARY KEY (ORDER_ID, ITEM_ID)
	)`); err != nil {
		t.Fatalf("create yb table: %v", err)
	}

	// Clean previous data and get a stable SCN (avoids ORA-01466 if DDL just happened)
	env.oracleDB.ExecContext(ctx, "DELETE FROM COMPOUND_PK_TEST WHERE ORDER_ID BETWEEN 9000 AND 9003")
	env.ybPool.Exec(ctx, "DELETE FROM COMPOUND_PK_TEST WHERE ORDER_ID BETWEEN 9000 AND 9003")

	t.Cleanup(func() {
		cleanCtx, c := context.WithTimeout(context.Background(), 10*time.Second)
		defer c()
		env.oracleDB.ExecContext(cleanCtx, "DELETE FROM COMPOUND_PK_TEST WHERE ORDER_ID BETWEEN 9000 AND 9003")
		env.ybPool.Exec(cleanCtx, "DELETE FROM COMPOUND_PK_TEST WHERE ORDER_ID BETWEEN 9000 AND 9003")
	})

	// Seed 9 rows: 3 orders x 3 items
	for i := 1; i <= 3; i++ {
		for j := 1; j <= 3; j++ {
			orderID := 9000 + i
			_, err := env.oracleDB.ExecContext(ctx,
				"INSERT INTO COMPOUND_PK_TEST (ORDER_ID, ITEM_ID, QTY, PRICE) VALUES (:1, :2, :3, :4)",
				orderID, j, i*j, float64(i*j)*9.99,
			)
			if err != nil {
				t.Fatalf("insert (%d,%d): %v", orderID, j, err)
			}
		}
	}
	t.Log("seeded 9 rows (3 orders x 3 items)")

	// Save current SCN to progress so chunk querier uses a post-DDL SCN
	var scn int64
	if err := env.oracleDB.QueryRowContext(ctx,
		"SELECT current_scn FROM v$database").Scan(&scn); err != nil {
		t.Fatalf("get current scn: %v", err)
	}
	pgStore := progress.NewPgStore(env.ybPool, "dblog_progress")
	pgStore.EnsureTable(ctx)
	pgStore.Save(ctx, "COMPOUND_PK_TEST", nil, uint64(scn))

	rh := env.startReplicatorForTable("COMPOUND_PK_TEST", []string{"ORDER_ID", "ITEM_ID"}, 100)
	defer rh.cancel()

	// Wait for chunk loading — all 9 rows
	deadline := time.After(30 * time.Second)
	for {
		var count int
		env.ybPool.QueryRow(ctx, "SELECT COUNT(*) FROM COMPOUND_PK_TEST").Scan(&count)
		if count == 9 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("timeout waiting for 9 rows, got %d", count)
		case <-time.After(500 * time.Millisecond):
		}
	}
	t.Log("chunk loading complete: 9 rows")

	// Verify a specific compound PK row
	var qty int
	if err := env.ybPool.QueryRow(ctx,
		"SELECT QTY FROM COMPOUND_PK_TEST WHERE ORDER_ID = 9002 AND ITEM_ID = 3",
	).Scan(&qty); err != nil {
		t.Fatalf("query compound PK: %v", err)
	}
	if qty != 6 {
		t.Errorf("QTY for (9002,3) = %d, want 6", qty)
	}

	// Wait for CDC streaming
	if err := rh.cdcClient.WaitStreaming(ctx); err != nil {
		t.Fatalf("wait for CDC streaming: %v", err)
	}
	t.Log("CDC streaming ready")

	// UPDATE via CDC
	if _, err := env.oracleDB.ExecContext(ctx,
		"UPDATE COMPOUND_PK_TEST SET QTY = 99 WHERE ORDER_ID = 9001 AND ITEM_ID = 2",
	); err != nil {
		t.Fatalf("update: %v", err)
	}

	deadline = time.After(30 * time.Second)
	for {
		env.ybPool.QueryRow(ctx,
			"SELECT QTY FROM COMPOUND_PK_TEST WHERE ORDER_ID = 9001 AND ITEM_ID = 2",
		).Scan(&qty)
		if qty == 99 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("timeout waiting for CDC update, QTY = %d", qty)
		case <-time.After(500 * time.Millisecond):
		}
	}
	t.Log("CDC UPDATE verified: (9001,2) QTY=99")

	// DELETE via CDC
	if _, err := env.oracleDB.ExecContext(ctx,
		"DELETE FROM COMPOUND_PK_TEST WHERE ORDER_ID = 9003 AND ITEM_ID = 3",
	); err != nil {
		t.Fatalf("delete: %v", err)
	}

	deadline = time.After(30 * time.Second)
	for {
		var count int
		env.ybPool.QueryRow(ctx, "SELECT COUNT(*) FROM COMPOUND_PK_TEST").Scan(&count)
		if count == 8 {
			break
		}
		select {
		case <-deadline:
			var c int
			env.ybPool.QueryRow(ctx, "SELECT COUNT(*) FROM COMPOUND_PK_TEST").Scan(&c)
			t.Fatalf("timeout waiting for DELETE, count = %d", c)
		case <-time.After(500 * time.Millisecond):
		}
	}
	t.Log("CDC DELETE verified: (9003,3) removed")

	// INSERT via CDC — add a new item
	if _, err := env.oracleDB.ExecContext(ctx,
		"INSERT INTO COMPOUND_PK_TEST (ORDER_ID, ITEM_ID, QTY, PRICE) VALUES (9001, 4, 10, 49.99)",
	); err != nil {
		t.Fatalf("insert new item: %v", err)
	}

	deadline = time.After(30 * time.Second)
	for {
		var count int
		env.ybPool.QueryRow(ctx, "SELECT COUNT(*) FROM COMPOUND_PK_TEST").Scan(&count)
		if count == 9 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("timeout waiting for INSERT")
		case <-time.After(500 * time.Millisecond):
		}
	}
	env.ybPool.QueryRow(ctx,
		"SELECT QTY FROM COMPOUND_PK_TEST WHERE ORDER_ID = 9001 AND ITEM_ID = 4",
	).Scan(&qty)
	if qty != 10 {
		t.Errorf("CDC INSERT: QTY for (9001,4) = %d, want 10", qty)
	}
	t.Log("CDC INSERT verified: (9001,4) QTY=10")

	// Final convergence check
	var oCount, yCount int
	env.oracleDB.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM COMPOUND_PK_TEST WHERE ORDER_ID BETWEEN 9000 AND 9003").Scan(&oCount)
	env.ybPool.QueryRow(ctx,
		"SELECT COUNT(*) FROM COMPOUND_PK_TEST WHERE ORDER_ID BETWEEN 9000 AND 9003").Scan(&yCount)
	if oCount != yCount {
		t.Errorf("final count mismatch: oracle=%d yb=%d", oCount, yCount)
	}

	t.Logf("compound PK test passed: chunk load=%d, UPDATE+DELETE+INSERT via CDC verified", 9)
}
