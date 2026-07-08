package chunk

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/null-ptr-exception/dblog-cdc/internal/event"
	"github.com/null-ptr-exception/dblog-cdc/internal/transform"
)

type OracleQuerier struct {
	db     *sql.DB
	pkCols []string
	// pkExprs are SQL expressions for each PK column, wrapping DATE/TIMESTAMP
	// with TO_CHAR for consistent string comparison and ordering.
	pkExprs []string
}

func NewOracleQuerier(db *sql.DB, pkCols []string) *OracleQuerier {
	exprs := make([]string, len(pkCols))
	for i, col := range pkCols {
		exprs[i] = col
	}
	return &OracleQuerier{db: db, pkCols: pkCols, pkExprs: exprs}
}

// SetPKTypes configures type-aware expressions for PK columns that are
// DATE or TIMESTAMP, so row-value comparisons use sortable string representations.
func (o *OracleQuerier) SetPKTypes(colTypes map[string]transform.ColumnType) {
	for i, col := range o.pkCols {
		ct, ok := colTypes[col]
		if !ok {
			continue
		}
		dt := strings.ToUpper(ct.DataType)
		switch {
		case dt == "DATE":
			o.pkExprs[i] = fmt.Sprintf("TO_CHAR(%s, 'YYYY-MM-DD HH24:MI:SS')", col)
		case strings.HasPrefix(dt, "TIMESTAMP"):
			o.pkExprs[i] = fmt.Sprintf("TO_CHAR(%s, 'YYYY-MM-DD HH24:MI:SS.FF6')", col)
		case dt == "NUMBER" || dt == "FLOAT":
			// Leave as-is — Oracle handles implicit string→number conversion
			// for bind params, and native numeric ordering is correct.
			// TO_CHAR(number) would produce lexicographic ordering (100 < 5 < 9).
		}
	}
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
		// Row-value comparison using type-aware expressions
		exprList := strings.Join(o.pkExprs, ", ")
		placeholders := make([]string, len(o.pkCols))
		for i := range o.pkCols {
			placeholders[i] = fmt.Sprintf(":%d", i+1)
			args = append(args, afterPK[i])
		}
		whereClause = fmt.Sprintf("(%s) > (%s)", exprList, strings.Join(placeholders, ", "))
	} else {
		whereClause = "1=1"
	}

	orderBy := strings.Join(o.pkExprs, ", ")
	limitPlaceholder := fmt.Sprintf(":%d", len(args)+1)
	args = append(args, limit)

	// Build PK extraction expressions for SELECT
	pkSelects := make([]string, len(o.pkCols))
	for i, expr := range o.pkExprs {
		if expr != o.pkCols[i] {
			pkSelects[i] = fmt.Sprintf("%s AS PK_%s", expr, o.pkCols[i])
		} else {
			pkSelects[i] = fmt.Sprintf("TO_CHAR(%s) AS PK_%s", o.pkCols[i], o.pkCols[i])
		}
	}

	query := fmt.Sprintf(
		"SELECT %s, t.* FROM %s AS OF SCN %d t WHERE %s ORDER BY %s ASC FETCH FIRST %s ROWS ONLY",
		strings.Join(pkSelects, ", "), table, scn, whereClause, orderBy, limitPlaceholder,
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

	numPKExtras := len(o.pkCols)

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

		// First numPKExtras columns are the PK string extractions
		pk := make([]string, numPKExtras)
		for i := 0; i < numPKExtras; i++ {
			pk[i] = fmt.Sprint(values[i])
		}

		// Remaining columns are the actual table columns (from t.*)
		rowMap := make(map[string]any, len(cols)-numPKExtras)
		for i := numPKExtras; i < len(cols); i++ {
			rowMap[cols[i]] = values[i]
		}

		key := event.EncodePK(pk)
		result.Rows[key] = event.ChunkRow{PK: pk, Columns: rowMap}
		result.LastPK = pk
		count++
	}

	result.Complete = count < limit
	return result, rows.Err()
}
