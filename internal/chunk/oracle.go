package chunk

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"

	"github.com/null-ptr-exception/dblog-cdc/internal/event"
)

type OracleQuerier struct {
	db    *sql.DB
	pkCol string
}

func NewOracleQuerier(db *sql.DB, pkCol string) *OracleQuerier {
	return &OracleQuerier{db: db, pkCol: pkCol}
}

func (o *OracleQuerier) CurrentSCN(ctx context.Context) (uint64, error) {
	var scn uint64
	err := o.db.QueryRowContext(ctx, "SELECT current_scn FROM v$database").Scan(&scn)
	return scn, err
}

func (o *OracleQuerier) QueryChunk(ctx context.Context, table string, afterPK int64, limit int, scn uint64) (*event.ChunkResult, error) {
	query := fmt.Sprintf(
		"SELECT * FROM %s AS OF SCN %d WHERE %s > :1 ORDER BY %s ASC FETCH FIRST :2 ROWS ONLY",
		table, scn, o.pkCol, o.pkCol,
	)

	rows, err := o.db.QueryContext(ctx, query, afterPK, limit)
	if err != nil {
		return nil, fmt.Errorf("chunk query: %w", err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("columns: %w", err)
	}

	result := &event.ChunkResult{
		Table: table,
		SCN:   scn,
		Rows:  make(map[int64]map[string]any),
	}

	count := 0
	for rows.Next() {
		values := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range values {
			ptrs[i] = &values[i]
		}

		if err := rows.Scan(ptrs...); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}

		rowMap := make(map[string]any, len(cols))
		var pk int64
		for i, col := range cols {
			rowMap[col] = values[i]
			if col == o.pkCol {
				switch v := values[i].(type) {
				case int64:
					pk = v
				case float64:
					pk = int64(v)
				case string:
					pk, _ = strconv.ParseInt(v, 10, 64)
				default:
					pk, _ = strconv.ParseInt(fmt.Sprint(v), 10, 64)
				}
			}
		}

		result.Rows[pk] = rowMap
		result.LastPK = pk
		count++
	}

	result.Complete = count < limit
	return result, rows.Err()
}
