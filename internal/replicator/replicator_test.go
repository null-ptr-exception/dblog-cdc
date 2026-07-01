package replicator_test

import (
	"context"
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
	rows []map[string]any
}

func (m *mockChunkQuerier) QueryChunk(_ context.Context, table string, afterPK int64, limit int, scn uint64) (*event.ChunkResult, error) {
	result := &event.ChunkResult{
		Table: table,
		SCN:   scn,
		Rows:  make(map[int64]map[string]any),
	}
	count := 0
	for _, row := range m.rows {
		pk := row["ID"].(int64)
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
			{Table: "T", Op: event.OpUpdate, SCN: 105, PK: 2, Columns: map[string]any{"ID": int64(2), "V": "cdc_updated"}},
		},
	}

	chunks := &mockChunkQuerier{
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

	found := map[int64]event.Event{}
	for _, e := range writer.events {
		found[e.PK] = e
	}

	if len(found) < 3 {
		t.Fatalf("expected at least 3 written events, got %d", len(found))
	}

	if found[2].Columns["V"] != "cdc_updated" {
		t.Errorf("PK 2 should be from CDC, got %v", found[2].Columns["V"])
	}
}
