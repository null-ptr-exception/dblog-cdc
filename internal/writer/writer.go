package writer

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/null-ptr-exception/dblog-cdc/internal/event"
)

type Executor interface {
	ExecContext(ctx context.Context, query string, args ...any) error
}

type Writer struct {
	db     Executor
	pkCols []string
}

func New(db Executor, pkCols []string) *Writer {
	return &Writer{db: db, pkCols: pkCols}
}

func BuildUpsertSQL(table string, pkCols []string, e event.Event) (string, []any) {
	colNames := make([]string, 0, len(e.Columns))
	for name := range e.Columns {
		colNames = append(colNames, name)
	}
	sort.Strings(colNames)

	// Build a set of PK columns for exclusion from UPDATE
	pkSet := make(map[string]bool, len(pkCols))
	for _, pk := range pkCols {
		pkSet[pk] = true
	}

	placeholders := make([]string, len(colNames))
	args := make([]any, len(colNames))
	updateClauses := make([]string, 0, len(colNames))

	for i, name := range colNames {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		args[i] = e.Columns[name]
		if !pkSet[name] {
			updateClauses = append(updateClauses, fmt.Sprintf("%s = EXCLUDED.%s", name, name))
		}
	}

	// Sort pkCols for deterministic ON CONFLICT clause
	sortedPKs := make([]string, len(pkCols))
	copy(sortedPKs, pkCols)
	sort.Strings(sortedPKs)

	sql := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		table,
		strings.Join(colNames, ", "),
		strings.Join(placeholders, ", "),
	)

	if len(updateClauses) > 0 {
		sql += fmt.Sprintf(" ON CONFLICT (%s) DO UPDATE SET %s",
			strings.Join(sortedPKs, ", "),
			strings.Join(updateClauses, ", "),
		)
	}

	return sql, args
}

func BuildDeleteSQL(table string, pkCols []string, pk []string) (string, []any) {
	clauses := make([]string, len(pkCols))
	args := make([]any, len(pkCols))
	for i, col := range pkCols {
		clauses[i] = fmt.Sprintf("%s = $%d", col, i+1)
		args[i] = pk[i]
	}
	return fmt.Sprintf("DELETE FROM %s WHERE %s", table, strings.Join(clauses, " AND ")), args
}

func (w *Writer) WriteBatch(ctx context.Context, events []event.Event) error {
	for _, e := range events {
		var sql string
		var args []any

		if e.Op == event.OpDelete {
			sql, args = BuildDeleteSQL(e.Table, w.pkCols, e.PK)
		} else {
			sql, args = BuildUpsertSQL(e.Table, w.pkCols, e)
		}

		if err := w.db.ExecContext(ctx, sql, args...); err != nil {
			return fmt.Errorf("write %s PK=%v: %w", e.Table, e.PK, err)
		}
	}
	return nil
}
