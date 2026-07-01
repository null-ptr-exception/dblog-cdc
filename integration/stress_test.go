//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestReplication_Stress runs a randomized workload against Oracle while the
// replicator is running, then stops writes, waits for convergence, and asserts
// Oracle == YB. Repeats for multiple rounds to catch intermittent issues.
//
// PK range: 5000-14999 (10,000 slots)
//
// Env vars to tune:
//
//	STRESS_WORKERS    — concurrent writer goroutines (default 4)
//	STRESS_ROUND_SEC  — seconds per round (default 15)
//	STRESS_ROUNDS     — number of verification rounds (default 3)
//	STRESS_PK_RANGE   — PK range size (default 1000)
func TestReplication_Stress(t *testing.T) {
	numWorkers := envOrInt("STRESS_WORKERS", 2)
	roundDuration := time.Duration(envOrInt("STRESS_ROUND_SEC", 10)) * time.Second
	numRounds := envOrInt("STRESS_ROUNDS", 3)
	pkRange := envOrInt("STRESS_PK_RANGE", 500)
	opDelayMs := envOrInt("STRESS_OP_DELAY_MS", 1)

	const startPK = 5000
	endPK := startPK + pkRange - 1
	sentinelPK := endPK + 1 // just outside the random write range

	totalTimeout := time.Duration(numRounds)*(roundDuration+90*time.Second) + 120*time.Second
	ctx, cancel := context.WithTimeout(context.Background(), totalTimeout)
	defer cancel()

	env := setupEnv(t, ctx)
	cleanRange := pkRange + 1 // include sentinel PK
	env.cleanRange(startPK, cleanRange)
	env.scheduleCleanup(startPK, cleanRange)

	t.Logf("stress config: workers=%d round=%v rounds=%d pk_range=%d-%d op_delay=%dms",
		numWorkers, roundDuration, numRounds, startPK, endPK, opDelayMs)

	rh := env.startReplicator(50)
	defer rh.cancel()

	// Wait for OLR to be streaming before hammering writes
	if err := rh.cdcClient.WaitStreaming(ctx); err != nil {
		t.Fatalf("wait for CDC streaming: %v", err)
	}
	t.Log("CDC streaming ready, seeding initial data")

	// Seed some rows so chunk loading and CDC have data to work with
	env.seedRows(startPK, 100, 1.0, "SEED")

	var totalInserts, totalUpdates, totalDeletes atomic.Int64

	for round := 1; round <= numRounds; round++ {
		t.Logf("=== round %d/%d ===", round, numRounds)

		stopCh := make(chan struct{})
		var wg sync.WaitGroup

		for w := 0; w < numWorkers; w++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))
				stressWorker(env, rng, startPK, pkRange, time.Duration(opDelayMs)*time.Millisecond,
					stopCh, &totalInserts, &totalUpdates, &totalDeletes)
			}(w)
		}

		select {
		case <-time.After(roundDuration):
		case <-ctx.Done():
			close(stopCh)
			wg.Wait()
			t.Fatalf("context expired during round %d", round)
		}

		close(stopCh)
		wg.Wait()

		t.Logf("round %d writers stopped — ops so far: inserts=%d updates=%d deletes=%d",
			round, totalInserts.Load(), totalUpdates.Load(), totalDeletes.Load())

		waitForConvergence(t, env, sentinelPK, round, 120*time.Second)

		assertFullMatch(t, env, startPK, endPK)
		t.Logf("round %d: PASS", round)
	}

	rh.cancel()
	t.Logf("stress test complete: total ops: inserts=%d updates=%d deletes=%d",
		totalInserts.Load(), totalUpdates.Load(), totalDeletes.Load())
}

func envOrInt(key string, fallback int) int {
	s := envOr(key, "")
	if s == "" {
		return fallback
	}
	var v int
	fmt.Sscanf(s, "%d", &v)
	return v
}

func stressWorker(
	env *testEnv,
	rng *rand.Rand,
	startPK, pkRange int,
	opDelay time.Duration,
	stop <-chan struct{},
	inserts, updates, deletes *atomic.Int64,
) {
	statuses := []string{"PENDING", "ACTIVE", "SHIPPED", "CANCELLED", "RETURNED"}

	for {
		select {
		case <-stop:
			return
		default:
		}

		if opDelay > 0 {
			time.Sleep(opDelay)
		}

		pk := startPK + rng.Intn(pkRange)
		amount := float64(rng.Intn(100000)) / 100.0
		status := statuses[rng.Intn(len(statuses))]

		roll := rng.Intn(100)
		switch {
		case roll < 40:
			// INSERT (or update if exists — use MERGE for idempotency)
			_, err := env.oracleDB.ExecContext(env.ctx,
				`MERGE INTO ORDERS o USING (SELECT :1 AS id FROM dual) s
				 ON (o.ID = s.id)
				 WHEN NOT MATCHED THEN INSERT (ID, AMOUNT, STATUS) VALUES (:2, :3, :4)
				 WHEN MATCHED THEN UPDATE SET AMOUNT = :5, STATUS = :6`,
				pk, pk, amount, status, amount, status)
			if err != nil {
				if env.ctx.Err() != nil {
					return
				}
				continue
			}
			inserts.Add(1)

		case roll < 80:
			// UPDATE
			_, err := env.oracleDB.ExecContext(env.ctx,
				"UPDATE ORDERS SET AMOUNT = :1, STATUS = :2 WHERE ID = :3",
				amount, status, pk)
			if err != nil {
				if env.ctx.Err() != nil {
					return
				}
				continue
			}
			updates.Add(1)

		default:
			// DELETE
			_, err := env.oracleDB.ExecContext(env.ctx,
				"DELETE FROM ORDERS WHERE ID = :1", pk)
			if err != nil {
				if env.ctx.Err() != nil {
					return
				}
				continue
			}
			deletes.Add(1)
		}
	}
}

// waitForConvergence inserts a sentinel row in Oracle and waits for it to
// appear in YB via CDC — proving all prior events have been delivered.
func waitForConvergence(t *testing.T, env *testEnv, sentinelPK int, round int, timeout time.Duration) {
	t.Helper()
	markerStatus := fmt.Sprintf("CONVERGE_R%d", round)

	// Upsert a sentinel row with a unique status per round
	if _, err := env.oracleDB.ExecContext(env.ctx,
		`MERGE INTO ORDERS o USING (SELECT :1 AS id FROM dual) s
		 ON (o.ID = s.id)
		 WHEN NOT MATCHED THEN INSERT (ID, AMOUNT, STATUS) VALUES (:2, 0, :3)
		 WHEN MATCHED THEN UPDATE SET STATUS = :4`,
		sentinelPK, sentinelPK, markerStatus, markerStatus); err != nil {
		t.Fatalf("write convergence marker: %v", err)
	}

	deadline := time.After(timeout)
	for {
		var status string
		env.ybPool.QueryRow(env.ctx, "SELECT STATUS FROM ORDERS WHERE ID = $1", sentinelPK).Scan(&status)
		if status == markerStatus {
			t.Logf("convergence marker (PK=%d status=%q) delivered", sentinelPK, markerStatus)
			return
		}
		select {
		case <-deadline:
			t.Fatalf("timeout waiting for convergence marker (PK=%d) — CDC lagging", sentinelPK)
		default:
			time.Sleep(500 * time.Millisecond)
		}
	}
}

// assertFullMatch does a row-by-row comparison of Oracle vs YB.
func assertFullMatch(t *testing.T, env *testEnv, startPK, endPK int) {
	t.Helper()

	type row struct {
		ID     int64
		Amount float64
		Status string
	}

	oracleRows, err := env.oracleDB.QueryContext(env.ctx,
		"SELECT ID, AMOUNT, STATUS FROM ORDERS WHERE ID BETWEEN :1 AND :2 ORDER BY ID",
		startPK, endPK)
	if err != nil {
		t.Fatalf("query oracle: %v", err)
	}
	defer oracleRows.Close()

	oracleMap := make(map[int64]row)
	for oracleRows.Next() {
		var r row
		if err := oracleRows.Scan(&r.ID, &r.Amount, &r.Status); err != nil {
			t.Fatalf("scan oracle: %v", err)
		}
		oracleMap[r.ID] = r
	}

	ybRows, err := env.ybPool.Query(env.ctx,
		"SELECT ID, AMOUNT, STATUS FROM ORDERS WHERE ID BETWEEN $1 AND $2 ORDER BY ID",
		startPK, endPK)
	if err != nil {
		t.Fatalf("query yb: %v", err)
	}
	defer ybRows.Close()

	ybMap := make(map[int64]row)
	for ybRows.Next() {
		var r row
		if err := ybRows.Scan(&r.ID, &r.Amount, &r.Status); err != nil {
			t.Fatalf("scan yb: %v", err)
		}
		ybMap[r.ID] = r
	}

	missingInYB := 0
	extraInYB := 0
	valueMismatch := 0

	for id, oRow := range oracleMap {
		yRow, ok := ybMap[id]
		if !ok {
			if missingInYB < 10 {
				t.Errorf("row %d: in Oracle but missing from YB (amount=%.2f status=%q)", id, oRow.Amount, oRow.Status)
			}
			missingInYB++
			continue
		}
		if fmt.Sprintf("%.2f", oRow.Amount) != fmt.Sprintf("%.2f", yRow.Amount) || oRow.Status != yRow.Status {
			if valueMismatch < 10 {
				t.Errorf("row %d: oracle=(%.2f, %q) yb=(%.2f, %q)",
					id, oRow.Amount, oRow.Status, yRow.Amount, yRow.Status)
			}
			valueMismatch++
		}
	}

	for id := range ybMap {
		if _, ok := oracleMap[id]; !ok {
			if extraInYB < 10 {
				t.Errorf("row %d: in YB but missing from Oracle", id)
			}
			extraInYB++
		}
	}

	if missingInYB > 0 || extraInYB > 0 || valueMismatch > 0 {
		t.Errorf("total mismatches: missing_in_yb=%d extra_in_yb=%d value_mismatch=%d (oracle=%d yb=%d)",
			missingInYB, extraInYB, valueMismatch, len(oracleMap), len(ybMap))
	} else {
		t.Logf("all %d rows match exactly", len(oracleMap))
	}
}
