package writer_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/null-ptr-exception/dblog-cdc/internal/event"
	"github.com/null-ptr-exception/dblog-cdc/internal/writer"
)

func TestBuildUpsertSQL(t *testing.T) {
	sql, args := writer.BuildUpsertSQL("ORDERS", []string{"ID"}, event.Event{
		Table: "ORDERS",
		Op:    event.OpInsert,
		PK:    []string{"42"},
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
	sql, args := writer.BuildDeleteSQL("ORDERS", []string{"ID"}, []string{"42"})

	if len(args) != 1 {
		t.Fatalf("expected 1 arg, got %d", len(args))
	}
	if args[0] != "42" {
		t.Errorf("expected PK 42, got %v", args[0])
	}
	if sql == "" {
		t.Error("SQL should not be empty")
	}
	t.Logf("SQL: %s", sql)
}

func TestBuildDeleteSQL_Compound(t *testing.T) {
	sql, args := writer.BuildDeleteSQL("ORDER_ITEMS", []string{"ORDER_ID", "LINE_NUM"}, []string{"10", "2"})

	if len(args) != 2 {
		t.Fatalf("expected 2 args, got %d", len(args))
	}
	if args[0] != "10" || args[1] != "2" {
		t.Errorf("args = %v, want [10, 2]", args)
	}
	if sql == "" {
		t.Error("SQL should not be empty")
	}
	t.Logf("SQL: %s", sql)
}

type mockDB struct {
	executed []string
	failAt   int // fail at this call index (-1 = never)
}

func (m *mockDB) ExecContext(_ context.Context, query string, args ...any) error {
	if m.failAt >= 0 && len(m.executed) == m.failAt {
		m.executed = append(m.executed, query)
		return fmt.Errorf("simulated DB error")
	}
	m.executed = append(m.executed, query)
	return nil
}

func TestWriter_WriteBatch(t *testing.T) {
	db := &mockDB{failAt: -1}
	w := writer.New(db, []string{"ID"})

	events := []event.Event{
		{Table: "T", Op: event.OpInsert, PK: []string{"1"}, Columns: map[string]any{"ID": int64(1), "V": "a"}},
		{Table: "T", Op: event.OpUpdate, PK: []string{"2"}, Columns: map[string]any{"ID": int64(2), "V": "b"}},
		{Table: "T", Op: event.OpDelete, PK: []string{"3"}},
	}

	err := w.WriteBatch(context.Background(), events)
	if err != nil {
		t.Fatalf("WriteBatch() error: %v", err)
	}

	if len(db.executed) != 3 {
		t.Errorf("expected 3 queries, got %d", len(db.executed))
	}
}

func TestBuildUpsertSQL_AllPKColumns(t *testing.T) {
	// When every column is a PK, there are no non-PK columns to UPDATE.
	sql, args := writer.BuildUpsertSQL("LOOKUP", []string{"K1", "K2"}, event.Event{
		Table: "LOOKUP",
		Op:    event.OpInsert,
		PK:    []string{"a", "b"},
		Columns: map[string]any{
			"K1": "a",
			"K2": "b",
		},
	})

	if len(args) != 2 {
		t.Fatalf("expected 2 args, got %d", len(args))
	}

	// Should be a plain INSERT with no ON CONFLICT clause
	if strings.Contains(sql, "ON CONFLICT") {
		t.Errorf("all-PK table should not have ON CONFLICT clause, got: %s", sql)
	}
	t.Logf("SQL: %s", sql)
}

func TestWriter_WriteBatch_PartialFailure(t *testing.T) {
	db := &mockDB{failAt: 1} // fail on second event
	w := writer.New(db, []string{"ID"})

	events := []event.Event{
		{Table: "T", Op: event.OpInsert, PK: []string{"1"}, Columns: map[string]any{"ID": int64(1), "V": "a"}},
		{Table: "T", Op: event.OpInsert, PK: []string{"2"}, Columns: map[string]any{"ID": int64(2), "V": "b"}},
		{Table: "T", Op: event.OpInsert, PK: []string{"3"}, Columns: map[string]any{"ID": int64(3), "V": "c"}},
	}

	err := w.WriteBatch(context.Background(), events)
	if err == nil {
		t.Fatal("expected error from partial failure")
	}

	// First event was written, second failed, third never attempted
	if len(db.executed) != 2 {
		t.Errorf("expected 2 queries executed (1 success + 1 fail), got %d", len(db.executed))
	}
}

func TestBuildUpsertSQL_CompoundPK(t *testing.T) {
	sql, args := writer.BuildUpsertSQL("ITEMS", []string{"ORDER_ID", "LINE"}, event.Event{
		Table: "ITEMS",
		Op:    event.OpInsert,
		PK:    []string{"10", "2"},
		Columns: map[string]any{
			"ORDER_ID": int64(10),
			"LINE":     int64(2),
			"QTY":      int64(5),
			"PRICE":    12.50,
		},
	})

	if len(args) != 4 {
		t.Fatalf("expected 4 args, got %d", len(args))
	}

	if !strings.Contains(sql, "ON CONFLICT") {
		t.Error("compound PK upsert should have ON CONFLICT clause")
	}
	// Non-PK columns (QTY, PRICE) should be in UPDATE SET
	if !strings.Contains(sql, "PRICE = EXCLUDED.PRICE") {
		t.Error("should update non-PK column PRICE")
	}
	if !strings.Contains(sql, "QTY = EXCLUDED.QTY") {
		t.Error("should update non-PK column QTY")
	}
	// PK columns should NOT be in UPDATE SET
	if strings.Contains(sql, "ORDER_ID = EXCLUDED.ORDER_ID") {
		t.Error("should not update PK column ORDER_ID")
	}
	t.Logf("SQL: %s", sql)
}
