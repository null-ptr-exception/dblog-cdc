package writer_test

import (
	"context"
	"testing"

	"github.com/null-ptr-exception/dblog-cdc/internal/event"
	"github.com/null-ptr-exception/dblog-cdc/internal/writer"
)

func TestBuildUpsertSQL(t *testing.T) {
	sql, args := writer.BuildUpsertSQL("ORDERS", "ID", event.Event{
		Table: "ORDERS",
		Op:    event.OpInsert,
		PK:    42,
		Columns: map[string]any{
			"ID":     int64(42),
			"AMOUNT": 99.95,
			"STATUS": "NEW",
		},
	})

	if len(args) != 3 {
		t.Fatalf("expected 3 args, got %d", len(args))
	}

	if sql == "" {
		t.Error("SQL should not be empty")
	}
	t.Logf("SQL: %s", sql)
	t.Logf("Args: %v", args)
}

func TestBuildDeleteSQL(t *testing.T) {
	sql, args := writer.BuildDeleteSQL("ORDERS", "ID", 42)

	if len(args) != 1 {
		t.Fatalf("expected 1 arg, got %d", len(args))
	}
	if args[0] != int64(42) {
		t.Errorf("expected PK 42, got %v", args[0])
	}
	if sql == "" {
		t.Error("SQL should not be empty")
	}
	t.Logf("SQL: %s", sql)
}

type mockDB struct {
	executed []string
}

func (m *mockDB) ExecContext(_ context.Context, query string, args ...any) error {
	m.executed = append(m.executed, query)
	return nil
}

func TestWriter_WriteBatch(t *testing.T) {
	db := &mockDB{}
	w := writer.New(db, "ID")

	events := []event.Event{
		{Table: "T", Op: event.OpInsert, PK: 1, Columns: map[string]any{"ID": int64(1), "V": "a"}},
		{Table: "T", Op: event.OpUpdate, PK: 2, Columns: map[string]any{"ID": int64(2), "V": "b"}},
		{Table: "T", Op: event.OpDelete, PK: 3},
	}

	err := w.WriteBatch(context.Background(), events)
	if err != nil {
		t.Fatalf("WriteBatch() error: %v", err)
	}

	if len(db.executed) != 3 {
		t.Errorf("expected 3 queries, got %d", len(db.executed))
	}
}
