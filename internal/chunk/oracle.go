package chunk

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/null-ptr-exception/dblog-cdc/internal/event"
)

type OracleQuerier struct {
	db     *sql.DB
	pkCols []string
}

func NewOracleQuerier(db *sql.DB, pkCols []string) *OracleQuerier {
	return &OracleQuerier{db: db, pkCols: pkCols}
}

func (o *OracleQuerier) CurrentSCN(ctx context.Context) (uint64, error) {
	var scn uint64
	err := o.db.QueryRowContext(ctx, "SELECT current_scn FROM v$database").Scan(&scn)
	return scn, err
}

func (o *OracleQuerier) QueryChunk(ctx context.Context, table string, afterPK []string, limit int, scn uint64) (*event.ChunkResult, error) {
	var whereClause string
	var args []any

	if len(afterPK) > 0 {
		// Row-value comparison: (col1, col2) > (:1, :2)
		colList := strings.Join(o.pkCols, ", ")
		placeholders := make([]string, len(o.pkCols))
		for i := range o.pkCols {
			placeholders[i] = fmt.Sprintf(":%d", i+1)
			args = append(args, afterPK[i])
		}
		whereClause = fmt.Sprintf("(%s) > (%s)", colList, strings.Join(placeholders, ", "))
	} else {
		whereClause = "1=1"
	}

	orderBy := strings.Join(o.pkCols, ", ")
	limitPlaceholder := fmt.Sprintf(":%d", len(args)+1)
	args = append(args, limit)

	query := fmt.Sprintf(
		"SELECT * FROM %s AS OF SCN %d WHERE %s ORDER BY %s ASC FETCH FIRST %s ROWS ONLY",
		table, scn, whereClause, orderBy, limitPlaceholder,
	)

	rows, err := o.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("chunk query: %w", err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("columns: %w", err)
	}

	// Build a set of PK column names for fast lookup
	pkSet := make(map[string]bool, len(o.pkCols))
	for _, c := range o.pkCols {
		pkSet[c] = true
	}

	result := &event.ChunkResult{
		Table: table,
		SCN:   scn,
		Rows:  make(map[string]event.ChunkRow),
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
		pk := make([]string, 0, len(o.pkCols))
		for i, col := range cols {
			rowMap[col] = values[i]
		}
		// Extract PK values in pkCols order
		for _, pkc := range o.pkCols {
			pk = append(pk, fmt.Sprint(rowMap[pkc]))
		}

		key := event.EncodePK(pk)
		result.Rows[key] = event.ChunkRow{PK: pk, Columns: rowMap}
		result.LastPK = pk
		count++
	}

	result.Complete = count < limit
	return result, rows.Err()
}
