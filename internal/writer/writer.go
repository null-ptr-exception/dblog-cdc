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
	db    Executor
	pkCol string
}

func New(db Executor, pkCol string) *Writer {
	return &Writer{db: db, pkCol: pkCol}
}

func BuildUpsertSQL(table, pkCol string, e event.Event) (string, []any) {
	colNames := make([]string, 0, len(e.Columns))
	for name := range e.Columns {
		colNames = append(colNames, name)
	}
	sort.Strings(colNames)

	placeholders := make([]string, len(colNames))
	args := make([]any, len(colNames))
	updateClauses := make([]string, 0, len(colNames))

	for i, name := range colNames {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		args[i] = e.Columns[name]
		if name != pkCol {
			updateClauses = append(updateClauses, fmt.Sprintf("%s = EXCLUDED.%s", name, name))
		}
	}

	sql := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		table,
		strings.Join(colNames, ", "),
		strings.Join(placeholders, ", "),
	)

	if len(updateClauses) > 0 {
		sql += fmt.Sprintf(" ON CONFLICT (%s) DO UPDATE SET %s",
			pkCol,
			strings.Join(updateClauses, ", "),
		)
	}

	return sql, args
}

func BuildDeleteSQL(table, pkCol string, pk int64) (string, []any) {
	return fmt.Sprintf("DELETE FROM %s WHERE %s = $1", table, pkCol), []any{pk}
}

func (w *Writer) WriteBatch(ctx context.Context, events []event.Event) error {
	for _, e := range events {
		var sql string
		var args []any

		if e.Op == event.OpDelete {
			sql, args = BuildDeleteSQL(e.Table, w.pkCol, e.PK)
		} else {
			sql, args = BuildUpsertSQL(e.Table, w.pkCol, e)
		}

		if err := w.db.ExecContext(ctx, sql, args...); err != nil {
			return fmt.Errorf("write %s PK=%d: %w", e.Table, e.PK, err)
		}
	}
	return nil
}
