//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"math"
	mrand "math/rand"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/null-ptr-exception/dblog-cdc/internal/progress"
)

// TestCompoundPK_ProductionSchema runs a randomized workload against a table
// modeled on a real production 8-column compound PK schema (from test-ddl.md).
// The PK mixes VARCHAR2, NUMBER, DATE, and TIMESTAMP types — exercising
// tuple-ordered chunk pagination and CDC dedup with diverse key types.
//
// Simplified to 18 columns (8 PK + 10 non-PK) from the original 92-column spec.
// Partitioning is omitted (not relevant to CDC correctness).
//
// Env vars to tune:
//
//	CPK_STRESS_WORKERS    — concurrent writers (default 2)
//	CPK_STRESS_ROUND_SEC  — seconds per round (default 10)
//	CPK_STRESS_ROUNDS     — verification rounds (default 2)
//	CPK_STRESS_ROWS       — initial seed rows (default 1000)
//	CPK_STRESS_CHUNK_SIZE — rows per chunk read (default 5)
func TestCompoundPK_ProductionSchema(t *testing.T) {
	numWorkers := envOrInt("CPK_STRESS_WORKERS", 2)
	roundDuration := time.Duration(envOrInt("CPK_STRESS_ROUND_SEC", 10)) * time.Second
	numRounds := envOrInt("CPK_STRESS_ROUNDS", 2)
	seedCount := envOrInt("CPK_STRESS_ROWS", 1000)
	chunkSize := envOrInt("CPK_STRESS_CHUNK_SIZE", 5)

	totalTimeout := time.Duration(numRounds)*(roundDuration+90*time.Second) + 120*time.Second
	ctx, cancel := context.WithTimeout(context.Background(), totalTimeout)
	defer cancel()

	env := setupEnv(t, ctx)

	// Oracle DDL — 8-column compound PK matching production schema types
	env.oracleDB.ExecContext(ctx, `
		DECLARE
			e EXCEPTION; PRAGMA EXCEPTION_INIT(e, -955);
		BEGIN
			EXECUTE IMMEDIATE 'CREATE TABLE CPK_STRESS (
				COL_01  VARCHAR2(64)   NOT NULL,
				COL_02  VARCHAR2(64),
				COL_03  VARCHAR2(20),
				COL_04  VARCHAR2(64),
				COL_06  VARCHAR2(64)   NOT NULL,
				COL_07  VARCHAR2(64)   NOT NULL,
				COL_08  VARCHAR2(64),
				COL_09  NUMBER(10,0)   NOT NULL,
				COL_10  VARCHAR2(64),
				COL_12  DATE           NOT NULL,
				COL_14  FLOAT(126),
				COL_17  VARCHAR2(20)   NOT NULL,
				COL_21  NUMBER(10,0),
				COL_22  NUMBER(10,0),
				COL_43  NUMBER(5,0),
				COL_45  DATE,
				COL_46  TIMESTAMP(6)   NOT NULL,
				COL_77  TIMESTAMP(6)   NOT NULL,
				CONSTRAINT pk_cpk_stress PRIMARY KEY (COL_01, COL_06, COL_07, COL_09, COL_12, COL_17, COL_46, COL_77)
			)';
			EXECUTE IMMEDIATE 'ALTER TABLE CPK_STRESS ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS';
		EXCEPTION WHEN e THEN NULL;
		END;`)

	if _, err := env.ybPool.Exec(ctx, `CREATE TABLE IF NOT EXISTS CPK_STRESS (
		COL_01  VARCHAR(64)     NOT NULL,
		COL_02  VARCHAR(64),
		COL_03  VARCHAR(20),
		COL_04  VARCHAR(64),
		COL_06  VARCHAR(64)     NOT NULL,
		COL_07  VARCHAR(64)     NOT NULL,
		COL_08  VARCHAR(64),
		COL_09  BIGINT          NOT NULL,
		COL_10  VARCHAR(64),
		COL_12  TIMESTAMP       NOT NULL,
		COL_14  DOUBLE PRECISION,
		COL_17  VARCHAR(20)     NOT NULL,
		COL_21  BIGINT,
		COL_22  BIGINT,
		COL_43  SMALLINT,
		COL_45  TIMESTAMP,
		COL_46  TIMESTAMP(6)    NOT NULL,
		COL_77  TIMESTAMP(6)    NOT NULL,
		PRIMARY KEY (COL_01, COL_06, COL_07, COL_09, COL_12, COL_17, COL_46, COL_77)
	)`); err != nil {
		t.Fatalf("create yb table: %v", err)
	}

	// Clean previous test data
	env.oracleDB.ExecContext(ctx, "DELETE FROM CPK_STRESS WHERE COL_01 LIKE 'CPKTEST%'")
	env.ybPool.Exec(ctx, "DELETE FROM CPK_STRESS WHERE COL_01 LIKE 'CPKTEST%'")

	// Wait for Oracle to advance SCN past any DDL (avoids ORA-01466)
	time.Sleep(3 * time.Second)

	t.Cleanup(func() {
		cleanCtx, c := context.WithTimeout(context.Background(), 10*time.Second)
		defer c()
		env.oracleDB.ExecContext(cleanCtx, "DELETE FROM CPK_STRESS WHERE COL_01 LIKE 'CPKTEST%'")
		env.ybPool.Exec(cleanCtx, "DELETE FROM CPK_STRESS WHERE COL_01 LIKE 'CPKTEST%'")
	})

	t.Logf("cpk stress config: workers=%d round=%v rounds=%d seed=%d chunk_size=%d",
		numWorkers, roundDuration, numRounds, seedCount, chunkSize)

	// Seed rows BEFORE saving SCN so chunk loading actually finds data.
	// With 1000 rows and chunk_size=5, chunk loading takes ~200 iterations,
	// giving CDC events plenty of time to race against chunk reads.
	rng := mrand.New(mrand.NewSource(42))
	for i := 0; i < seedCount; i++ {
		cpkInsertRow(env, rng, i)
	}
	t.Logf("seeded %d rows", seedCount)

	// Save SCN AFTER seeding — chunks will see all seeded rows via AS OF SCN
	var scn int64
	if err := env.oracleDB.QueryRowContext(ctx,
		"SELECT current_scn FROM v$database").Scan(&scn); err != nil {
		t.Fatalf("get current scn: %v", err)
	}
	pgStore := progress.NewPgStore(env.ybPool, "dblog_progress")
	pgStore.EnsureTable(ctx)
	pgStore.Save(ctx, "CPK_STRESS", nil, uint64(scn))

	pkCols := []string{"COL_01", "COL_06", "COL_07", "COL_09", "COL_12", "COL_17", "COL_46", "COL_77"}
	rh := env.startReplicatorForTable("CPK_STRESS", pkCols, chunkSize)
	defer rh.cancel()

	// Start CDC mutations IMMEDIATELY — don't wait for chunk loading to finish.
	// This creates the overlap where CDC events arrive while chunks are still
	// being loaded, exercising the buffer's SCN-based dedup:
	//   - CDC event with higher SCN overwrites chunk row (CDC wins)
	//   - Chunk row with newer data than CDC event stays (chunk wins)
	var totalOps atomic.Int64

	if err := rh.cdcClient.WaitStreaming(ctx); err != nil {
		t.Fatalf("wait for CDC streaming: %v", err)
	}
	t.Log("CDC streaming ready, starting mutations while chunks load")

	for round := 1; round <= numRounds; round++ {
		t.Logf("=== round %d/%d ===", round, numRounds)

		stopCh := make(chan struct{})
		var wg sync.WaitGroup

		for w := 0; w < numWorkers; w++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				r := mrand.New(mrand.NewSource(time.Now().UnixNano() + int64(workerID)))
				cpkStressWorker(env, r, stopCh, &totalOps)
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

		t.Logf("round %d writers stopped — total ops: %d", round, totalOps.Load())

		// Convergence: insert a sentinel row and wait for it in YB
		cpkWaitConvergence(t, env, round, 120*time.Second)

		// Wait for row count to stabilize — ensures all in-flight CDC events
		// (which may have lower SCN than the sentinel) finish writing.
		cpkWaitStable(t, env, 2*time.Second, 30*time.Second)

		// Full comparison
		cpkAssertMatch(t, env)
		t.Logf("round %d: PASS", round)
	}

	rh.cancel()
	t.Logf("cpk stress complete: total ops=%d", totalOps.Load())
}

// cpkGenPK generates a deterministic compound PK from a seed integer.
// This gives us a fixed set of PK "slots" that workers can INSERT/UPDATE/DELETE.
func cpkGenPK(seed int) (col01, col06, col07 string, col09 int, col12 time.Time, col17 string, col46, col77 time.Time) {
	group := seed / 10
	item := seed % 10

	col01 = fmt.Sprintf("CPKTEST_%04d", seed)
	col06 = fmt.Sprintf("GRP_%03d", group)
	col07 = fmt.Sprintf("ITEM_%d", item)
	col09 = 1000 + seed
	col12 = time.Date(2025, 3, 1+group%28, 0, 0, 0, 0, time.UTC)
	col17 = fmt.Sprintf("CAT_%d", seed%5)
	col46 = time.Date(2025, 6, 15, 10, 0, 0, seed*1000000, time.UTC)
	col77 = time.Date(2025, 9, 20, 14, 30, 0, seed*2000000, time.UTC)
	return
}

func cpkInsertRow(env *testEnv, rng *mrand.Rand, seed int) {
	col01, col06, col07, col09, col12, col17, col46, col77 := cpkGenPK(seed)

	col02 := fmt.Sprintf("desc_%d_%d", seed, rng.Intn(10000))
	col03 := fmt.Sprintf("T%d", rng.Intn(100))
	col04 := fmt.Sprintf("ref_%d", rng.Intn(10000))
	col08 := fmt.Sprintf("extra_%d", rng.Intn(10000))
	col10 := fmt.Sprintf("note_%d", rng.Intn(10000))
	col14 := rng.Float64() * 100000
	col21 := rng.Intn(1000000)
	col22 := rng.Intn(1000000)
	col43 := rng.Intn(30000)
	days := rng.Intn(365)
	col45 := time.Date(2025, 1, 1, rng.Intn(24), rng.Intn(60), rng.Intn(60), 0, time.UTC).AddDate(0, 0, days)

	_, err := env.oracleDB.ExecContext(env.ctx,
		`MERGE INTO CPK_STRESS t USING (
			SELECT :1 AS c01, :2 AS c06, :3 AS c07, :4 AS c09,
			       TO_DATE(:5, 'YYYY-MM-DD') AS c12, :6 AS c17,
			       TO_TIMESTAMP(:7, 'YYYY-MM-DD HH24:MI:SS.FF6') AS c46,
			       TO_TIMESTAMP(:8, 'YYYY-MM-DD HH24:MI:SS.FF6') AS c77
			FROM dual
		) s ON (t.COL_01=s.c01 AND t.COL_06=s.c06 AND t.COL_07=s.c07 AND t.COL_09=s.c09
		        AND t.COL_12=s.c12 AND t.COL_17=s.c17 AND t.COL_46=s.c46 AND t.COL_77=s.c77)
		WHEN NOT MATCHED THEN INSERT
			(COL_01, COL_02, COL_03, COL_04, COL_06, COL_07, COL_08, COL_09, COL_10,
			 COL_12, COL_14, COL_17, COL_21, COL_22, COL_43, COL_45, COL_46, COL_77)
		VALUES (:9, :10, :11, :12, :13, :14, :15, :16, :17,
		        TO_DATE(:18, 'YYYY-MM-DD'), :19, :20, :21, :22, :23,
		        TO_DATE(:24, 'YYYY-MM-DD HH24:MI:SS'), TO_TIMESTAMP(:25, 'YYYY-MM-DD HH24:MI:SS.FF6'),
		        TO_TIMESTAMP(:26, 'YYYY-MM-DD HH24:MI:SS.FF6'))
		WHEN MATCHED THEN UPDATE SET
			COL_02=:27, COL_03=:28, COL_04=:29, COL_08=:30, COL_10=:31,
			COL_14=:32, COL_21=:33, COL_22=:34, COL_43=:35,
			COL_45=TO_DATE(:36, 'YYYY-MM-DD HH24:MI:SS')`,
		// ON clause PK values
		col01, col06, col07, col09,
		col12.Format("2006-01-02"), col17,
		col46.Format("2006-01-02 15:04:05.000000"),
		col77.Format("2006-01-02 15:04:05.000000"),
		// INSERT values
		col01, col02, col03, col04, col06, col07, col08, col09, col10,
		col12.Format("2006-01-02"), col14, col17, col21, col22, col43,
		col45.Format("2006-01-02 15:04:05"),
		col46.Format("2006-01-02 15:04:05.000000"),
		col77.Format("2006-01-02 15:04:05.000000"),
		// UPDATE values
		col02, col03, col04, col08, col10,
		col14, col21, col22, col43,
		col45.Format("2006-01-02 15:04:05"),
	)
	if err != nil && env.ctx.Err() == nil {
		env.t.Logf("cpk insert seed %d: %v", seed, err)
	}
}

func cpkStressWorker(
	env *testEnv,
	rng *mrand.Rand,
	stop <-chan struct{},
	ops *atomic.Int64,
) {
	const slotCount = 1000

	for {
		select {
		case <-stop:
			return
		default:
		}

		time.Sleep(2 * time.Millisecond)

		seed := rng.Intn(slotCount)

		roll := rng.Intn(100)
		switch {
		case roll < 50:
			cpkInsertRow(env, rng, seed)
			ops.Add(1)
		case roll < 85:
			// UPDATE non-PK columns
			col01, col06, col07, col09, col12, col17, col46, col77 := cpkGenPK(seed)
			newDesc := fmt.Sprintf("upd_%d_%d", seed, rng.Intn(10000))
			newVal := rng.Intn(999999)
			_, err := env.oracleDB.ExecContext(env.ctx,
				`UPDATE CPK_STRESS SET COL_02=:1, COL_21=:2
				 WHERE COL_01=:3 AND COL_06=:4 AND COL_07=:5 AND COL_09=:6
				   AND COL_12=TO_DATE(:7, 'YYYY-MM-DD') AND COL_17=:8
				   AND COL_46=TO_TIMESTAMP(:9, 'YYYY-MM-DD HH24:MI:SS.FF6')
				   AND COL_77=TO_TIMESTAMP(:10, 'YYYY-MM-DD HH24:MI:SS.FF6')`,
				newDesc, newVal,
				col01, col06, col07, col09,
				col12.Format("2006-01-02"), col17,
				col46.Format("2006-01-02 15:04:05.000000"),
				col77.Format("2006-01-02 15:04:05.000000"),
			)
			if err != nil {
				if env.ctx.Err() != nil {
					return
				}
				continue
			}
			ops.Add(1)
		default:
			// DELETE
			col01, col06, col07, col09, col12, col17, col46, col77 := cpkGenPK(seed)
			_, err := env.oracleDB.ExecContext(env.ctx,
				`DELETE FROM CPK_STRESS
				 WHERE COL_01=:1 AND COL_06=:2 AND COL_07=:3 AND COL_09=:4
				   AND COL_12=TO_DATE(:5, 'YYYY-MM-DD') AND COL_17=:6
				   AND COL_46=TO_TIMESTAMP(:7, 'YYYY-MM-DD HH24:MI:SS.FF6')
				   AND COL_77=TO_TIMESTAMP(:8, 'YYYY-MM-DD HH24:MI:SS.FF6')`,
				col01, col06, col07, col09,
				col12.Format("2006-01-02"), col17,
				col46.Format("2006-01-02 15:04:05.000000"),
				col77.Format("2006-01-02 15:04:05.000000"),
			)
			if err != nil {
				if env.ctx.Err() != nil {
					return
				}
				continue
			}
			ops.Add(1)
		}
	}
}

func cpkWaitConvergence(t *testing.T, env *testEnv, round int, timeout time.Duration) {
	t.Helper()
	// Use a sentinel row with a unique marker in COL_02
	marker := fmt.Sprintf("CONV_R%d", round)
	seed := 9999 // sentinel slot outside normal range

	col01, col06, col07, col09, col12, col17, col46, col77 := cpkGenPK(seed)

	_, err := env.oracleDB.ExecContext(env.ctx,
		`MERGE INTO CPK_STRESS t USING (
			SELECT :1 AS c01, :2 AS c06, :3 AS c07, :4 AS c09,
			       TO_DATE(:5, 'YYYY-MM-DD') AS c12, :6 AS c17,
			       TO_TIMESTAMP(:7, 'YYYY-MM-DD HH24:MI:SS.FF6') AS c46,
			       TO_TIMESTAMP(:8, 'YYYY-MM-DD HH24:MI:SS.FF6') AS c77
			FROM dual
		) s ON (t.COL_01=s.c01 AND t.COL_06=s.c06 AND t.COL_07=s.c07 AND t.COL_09=s.c09
		        AND t.COL_12=s.c12 AND t.COL_17=s.c17 AND t.COL_46=s.c46 AND t.COL_77=s.c77)
		WHEN NOT MATCHED THEN INSERT
			(COL_01, COL_02, COL_06, COL_07, COL_09, COL_12, COL_17, COL_46, COL_77)
		VALUES (:9, :10, :11, :12, :13, TO_DATE(:14, 'YYYY-MM-DD'), :15,
		        TO_TIMESTAMP(:16, 'YYYY-MM-DD HH24:MI:SS.FF6'),
		        TO_TIMESTAMP(:17, 'YYYY-MM-DD HH24:MI:SS.FF6'))
		WHEN MATCHED THEN UPDATE SET COL_02=:18`,
		col01, col06, col07, col09,
		col12.Format("2006-01-02"), col17,
		col46.Format("2006-01-02 15:04:05.000000"),
		col77.Format("2006-01-02 15:04:05.000000"),
		col01, marker, col06, col07, col09,
		col12.Format("2006-01-02"), col17,
		col46.Format("2006-01-02 15:04:05.000000"),
		col77.Format("2006-01-02 15:04:05.000000"),
		marker,
	)
	if err != nil {
		t.Fatalf("write convergence marker: %v", err)
	}

	deadline := time.After(timeout)
	for {
		var col02 *string
		env.ybPool.QueryRow(env.ctx,
			"SELECT COL_02 FROM CPK_STRESS WHERE COL_01 = $1", col01).Scan(&col02)
		if col02 != nil && *col02 == marker {
			t.Logf("convergence marker delivered (COL_01=%s)", col01)
			return
		}
		select {
		case <-deadline:
			t.Fatalf("timeout waiting for convergence marker (COL_01=%s)", col01)
		default:
			time.Sleep(500 * time.Millisecond)
		}
	}
}

func cpkWaitStable(t *testing.T, env *testEnv, stableDur, timeout time.Duration) {
	t.Helper()
	var lastCount int
	stableSince := time.Now()
	deadline := time.After(timeout)

	for {
		var count int
		env.ybPool.QueryRow(env.ctx,
			"SELECT COUNT(*) FROM CPK_STRESS WHERE COL_01 LIKE 'CPKTEST%'").Scan(&count)
		if count != lastCount {
			lastCount = count
			stableSince = time.Now()
		}
		if time.Since(stableSince) >= stableDur {
			t.Logf("row count stable at %d for %v", count, stableDur)
			return
		}
		select {
		case <-deadline:
			t.Logf("stability timeout, count=%d", lastCount)
			return
		default:
			time.Sleep(200 * time.Millisecond)
		}
	}
}

func cpkAssertMatch(t *testing.T, env *testEnv) {
	t.Helper()

	// Exclude sentinel row (seed 9999) from comparison — it has NULLs for non-PK columns
	oracleRows, err := env.oracleDB.QueryContext(env.ctx,
		`SELECT COL_01, COL_02, COL_03, COL_04, COL_06, COL_07, COL_08,
		        TO_CHAR(COL_09), COL_10,
		        TO_CHAR(COL_12, 'YYYY-MM-DD'),
		        TO_CHAR(COL_14),
		        COL_17,
		        TO_CHAR(COL_21), TO_CHAR(COL_22), TO_CHAR(COL_43),
		        TO_CHAR(COL_45, 'YYYY-MM-DD HH24:MI:SS'),
		        TO_CHAR(COL_46, 'YYYY-MM-DD HH24:MI:SS.FF6'),
		        TO_CHAR(COL_77, 'YYYY-MM-DD HH24:MI:SS.FF6')
		 FROM CPK_STRESS WHERE COL_01 LIKE 'CPKTEST%' AND COL_01 != 'CPKTEST_9999'
		 ORDER BY COL_01, COL_06, COL_07`)
	if err != nil {
		t.Fatalf("query oracle: %v", err)
	}
	defer oracleRows.Close()

	type cpkRow struct {
		cols [18]*string
	}

	makeKey := func(r cpkRow) string {
		parts := make([]string, 8)
		for i, idx := range []int{0, 4, 5, 7, 9, 11, 16, 17} {
			parts[i] = ptrStr(r.cols[idx])
		}
		return strings.Join(parts, "|")
	}

	oracleMap := make(map[string]cpkRow)
	for oracleRows.Next() {
		var r cpkRow
		ptrs := make([]any, 18)
		for i := range ptrs {
			ptrs[i] = &r.cols[i]
		}
		if err := oracleRows.Scan(ptrs...); err != nil {
			t.Fatalf("scan oracle: %v", err)
		}
		oracleMap[makeKey(r)] = r
	}

	ybRows, err := env.ybPool.Query(env.ctx,
		`SELECT COL_01, COL_02, COL_03, COL_04, COL_06, COL_07, COL_08,
		        COL_09::TEXT,
		        COL_10,
		        TO_CHAR(COL_12, 'YYYY-MM-DD'),
		        COL_14::TEXT,
		        COL_17,
		        COL_21::TEXT, COL_22::TEXT, COL_43::TEXT,
		        TO_CHAR(COL_45, 'YYYY-MM-DD HH24:MI:SS'),
		        TO_CHAR(COL_46, 'YYYY-MM-DD HH24:MI:SS.US'),
		        TO_CHAR(COL_77, 'YYYY-MM-DD HH24:MI:SS.US')
		 FROM CPK_STRESS WHERE COL_01 LIKE 'CPKTEST%' AND COL_01 != 'CPKTEST_9999'
		 ORDER BY COL_01, COL_06, COL_07`)
	if err != nil {
		t.Fatalf("query yb: %v", err)
	}
	defer ybRows.Close()

	ybMap := make(map[string]cpkRow)
	for ybRows.Next() {
		var r cpkRow
		ptrs := make([]any, 18)
		for i := range ptrs {
			ptrs[i] = &r.cols[i]
		}
		if err := ybRows.Scan(ptrs...); err != nil {
			t.Fatalf("scan yb: %v", err)
		}
		ybMap[makeKey(r)] = r
	}

	colNames := []string{
		"COL_01", "COL_02", "COL_03", "COL_04", "COL_06", "COL_07", "COL_08",
		"COL_09", "COL_10", "COL_12", "COL_14", "COL_17",
		"COL_21", "COL_22", "COL_43", "COL_45", "COL_46", "COL_77",
	}

	missingInYB := 0
	extraInYB := 0
	mismatch := 0

	for key, oRow := range oracleMap {
		yRow, ok := ybMap[key]
		if !ok {
			if missingInYB < 5 {
				t.Errorf("key %s: in Oracle but missing from YB", key)
			}
			missingInYB++
			continue
		}

		var diffs []string
		for i := range oRow.cols {
			ov, yv := ptrStr(oRow.cols[i]), ptrStr(yRow.cols[i])
			if i == 10 { // COL_14 (FLOAT) — compare with relative tolerance
				if floatClose(ov, yv, 1e-12) {
					continue
				}
			}
			if i == 12 || i == 13 || i == 14 { // COL_21, COL_22, COL_43
				ov = normalizeDecimal(ov)
				yv = normalizeDecimal(yv)
			}
			if ov != yv {
				diffs = append(diffs, fmt.Sprintf("%s: oracle=%q yb=%q", colNames[i], ov, yv))
			}
		}
		if len(diffs) > 0 {
			if mismatch < 5 {
				t.Errorf("key %s mismatch: %v", key, diffs)
			}
			mismatch++
		}
	}

	for key := range ybMap {
		if _, ok := oracleMap[key]; !ok {
			if extraInYB < 5 {
				t.Errorf("key %s: in YB but missing from Oracle", key)
			}
			extraInYB++
		}
	}

	if missingInYB > 0 || extraInYB > 0 || mismatch > 0 {
		t.Errorf("total: missing=%d extra=%d value_mismatch=%d (oracle=%d yb=%d)",
			missingInYB, extraInYB, mismatch, len(oracleMap), len(ybMap))
	} else {
		t.Logf("all %d rows match across all columns", len(oracleMap))
	}
}

func floatClose(a, b string, relTol float64) bool {
	if a == b {
		return true
	}
	fa, errA := strconv.ParseFloat(a, 64)
	fb, errB := strconv.ParseFloat(b, 64)
	if errA != nil || errB != nil {
		return false
	}
	diff := math.Abs(fa - fb)
	max := math.Max(math.Abs(fa), math.Abs(fb))
	if max == 0 {
		return diff == 0
	}
	return diff/max < relTol
}
