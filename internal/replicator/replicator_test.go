package replicator_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/null-ptr-exception/dblog-cdc/internal/config"
	"github.com/null-ptr-exception/dblog-cdc/internal/event"
	"github.com/null-ptr-exception/dblog-cdc/internal/progress"
	"github.com/null-ptr-exception/dblog-cdc/internal/replicator"
)

type mockCDCSource struct {
	events []event.Event
}

func (m *mockCDCSource) Stream(ctx context.Context, startSCN uint64, ch chan<- event.Event) error {
	for _, e := range m.events {
		select {
		case ch <- e:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	<-ctx.Done()
	return ctx.Err()
}

func (m *mockCDCSource) LastSCN() uint64 {
	if len(m.events) == 0 {
		return 100
	}
	return m.events[len(m.events)-1].SCN
}

type mockChunkQuerier struct {
	rows   []map[string]any
	pkCols []string
}

func (m *mockChunkQuerier) QueryChunk(_ context.Context, table string, afterPK []string, limit int, scn uint64) (*event.ChunkResult, error) {
	result := &event.ChunkResult{
		Table: table,
		SCN:   scn,
		Rows:  make(map[string]event.ChunkRow),
	}

	afterKey := ""
	if len(afterPK) > 0 {
		afterKey = event.EncodePK(afterPK)
	}

	count := 0
	for _, row := range m.rows {
		pk := make([]string, len(m.pkCols))
		for i, col := range m.pkCols {
			pk[i] = fmt.Sprint(row[col])
		}
		key := event.EncodePK(pk)
		if key > afterKey {
			result.Rows[key] = event.ChunkRow{PK: pk, Columns: row}
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

func (m *mockChunkQuerier) CurrentSCN(_ context.Context) (uint64, error) {
	return 100, nil
}

type captureWriter struct {
	events []event.Event
}

func (w *captureWriter) WriteBatch(_ context.Context, events []event.Event) error {
	w.events = append(w.events, events...)
	return nil
}

func TestReplicator_ChunksAndCDC(t *testing.T) {
	cdc := &mockCDCSource{
		events: []event.Event{
			{Table: "T", Op: event.OpUpdate, SCN: 105, PK: []string{"2"}, Columns: map[string]any{"ID": int64(2), "V": "cdc_updated"}},
		},
	}

	chunks := &mockChunkQuerier{
		pkCols: []string{"ID"},
		rows: []map[string]any{
			{"ID": int64(1), "V": "chunk_1"},
			{"ID": int64(2), "V": "chunk_2"},
			{"ID": int64(3), "V": "chunk_3"},
		},
	}

	writer := &captureWriter{}
	store := progress.NewMemoryStore()

	cfg := config.Table{Name: "T", ChunkSize: 10}
	r := replicator.New(cdc, chunks, writer, store, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := r.Run(ctx)
	if err != nil && err != context.DeadlineExceeded {
		t.Fatalf("Run() error: %v", err)
	}

	found := map[string]event.Event{}
	for _, e := range writer.events {
		found[event.EncodePK(e.PK)] = e
	}

	if len(found) < 3 {
		t.Fatalf("expected at least 3 written events, got %d", len(found))
	}

	if found["2"].Columns["V"] != "cdc_updated" {
		t.Errorf("PK 2 should be from CDC, got %v", found["2"].Columns["V"])
	}
}
