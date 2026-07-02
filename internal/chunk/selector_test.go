package chunk_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/null-ptr-exception/dblog-cdc/internal/chunk"
	"github.com/null-ptr-exception/dblog-cdc/internal/event"
)

type mockQuerier struct {
	rows  []map[string]any
	pkCol string
	scn   uint64
}

func (m *mockQuerier) QueryChunk(_ context.Context, table string, afterPK string, limit int, scn uint64) (*event.ChunkResult, error) {
	m.scn = scn
	result := &event.ChunkResult{
		Table: table,
		SCN:   scn,
		Rows:  make(map[string]map[string]any),
	}

	count := 0
	for _, row := range m.rows {
		pk := fmt.Sprint(row[m.pkCol])
		if pk > afterPK {
			result.Rows[pk] = row
			result.LastPK = pk
			count++
			if count >= limit {
				break
			}
		}
	}

	result.Complete = count < limit
	return result, nil
}

func (m *mockQuerier) CurrentSCN(_ context.Context) (uint64, error) {
	return m.scn, nil
}

func TestSelector_SingleChunk(t *testing.T) {
	q := &mockQuerier{
		pkCol: "ID",
		scn:   100,
		rows: []map[string]any{
			{"ID": int64(1), "NAME": "a"},
			{"ID": int64(2), "NAME": "b"},
		},
	}

	s := chunk.NewSelector(q)
	result, err := s.Next(context.Background(), "ORDERS", "", 10, 100)
	if err != nil {
		t.Fatalf("Next() error: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Errorf("got %d rows, want 2", len(result.Rows))
	}
	if !result.Complete {
		t.Error("should be complete (2 rows < limit 10)")
	}
	if result.LastPK != "2" {
		t.Errorf("LastPK = %s, want 2", result.LastPK)
	}
}

func TestSelector_MultipleChunks(t *testing.T) {
	rows := make([]map[string]any, 5)
	for i := range rows {
		rows[i] = map[string]any{"ID": int64(i + 1), "V": fmt.Sprintf("val%d", i+1)}
	}

	q := &mockQuerier{pkCol: "ID", scn: 100, rows: rows}
	s := chunk.NewSelector(q)

	r1, err := s.Next(context.Background(), "T", "", 2, 100)
	if err != nil {
		t.Fatalf("chunk 1 error: %v", err)
	}
	if len(r1.Rows) != 2 || r1.Complete {
		t.Errorf("chunk 1: %d rows, complete=%v", len(r1.Rows), r1.Complete)
	}

	r2, err := s.Next(context.Background(), "T", r1.LastPK, 2, 100)
	if err != nil {
		t.Fatalf("chunk 2 error: %v", err)
	}
	if len(r2.Rows) != 2 || r2.Complete {
		t.Errorf("chunk 2: %d rows, complete=%v", len(r2.Rows), r2.Complete)
	}

	r3, err := s.Next(context.Background(), "T", r2.LastPK, 2, 100)
	if err != nil {
		t.Fatalf("chunk 3 error: %v", err)
	}
	if len(r3.Rows) != 1 || !r3.Complete {
		t.Errorf("chunk 3: %d rows, complete=%v", len(r3.Rows), r3.Complete)
	}
}
