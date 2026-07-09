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
	events   []event.Event
	startSCN uint64 // captured from Stream call
	streamFn func(ctx context.Context, startSCN uint64, ch chan<- event.Event) error
}

func (m *mockCDCSource) Stream(ctx context.Context, startSCN uint64, ch chan<- event.Event) error {
	m.startSCN = startSCN
	if m.streamFn != nil {
		return m.streamFn(ctx, startSCN, ch)
	}
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
	rows    []map[string]any
	pkCols  []string
	scn     uint64        // 0 defaults to 100
	onQuery func()        // called before returning chunk results (for synchronization)
}

func (m *mockChunkQuerier) QueryChunk(_ context.Context, table string, afterPK []string, limit int, scn uint64) (*event.ChunkResult, error) {
	if m.onQuery != nil {
		m.onQuery()
	}
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
	if m.scn > 0 {
		return m.scn, nil
	}
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

func TestReplicator_ResumeFromLastPK(t *testing.T) {
	cdc := &mockCDCSource{}
	chunks := &mockChunkQuerier{
		pkCols: []string{"ID"},
		rows: []map[string]any{
			{"ID": int64(1), "V": "v1"},
			{"ID": int64(2), "V": "v2"},
			{"ID": int64(3), "V": "v3"},
			{"ID": int64(4), "V": "v4"},
			{"ID": int64(5), "V": "v5"},
		},
	}

	writer := &captureWriter{}
	store := progress.NewMemoryStore()

	// Pre-populate progress: already processed up to PK "2" at SCN 50
	store.Save(context.Background(), "T", []string{"2"}, 50)

	cfg := config.Table{Name: "T", ChunkSize: 10}
	r := replicator.New(cdc, chunks, writer, store, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := r.Run(ctx)
	if err != nil && err != context.DeadlineExceeded {
		t.Fatalf("Run() error: %v", err)
	}

	// CDC should start from saved SCN (50)
	if cdc.startSCN != 50 {
		t.Errorf("CDC should start from saved SCN 50, got %d", cdc.startSCN)
	}

	// Should only write PKs after "2": 3, 4, 5
	found := map[string]bool{}
	for _, e := range writer.events {
		found[event.EncodePK(e.PK)] = true
	}

	if found["1"] || found["2"] {
		t.Error("should not re-process PKs 1 or 2 (already completed)")
	}
	if !found["3"] || !found["4"] || !found["5"] {
		t.Errorf("should process PKs 3, 4, 5 — found: %v", found)
	}
}

func TestReplicator_ResumeChunksComplete(t *testing.T) {
	cdcEvents := make(chan struct{})
	cdc := &mockCDCSource{
		streamFn: func(ctx context.Context, startSCN uint64, ch chan<- event.Event) error {
			ch <- event.Event{Table: "T", Op: event.OpInsert, SCN: 200, PK: []string{"99"}, Columns: map[string]any{"ID": int64(99), "V": "from_cdc"}}
			close(cdcEvents)
			<-ctx.Done()
			return ctx.Err()
		},
	}

	chunks := &mockChunkQuerier{
		pkCols: []string{"ID"},
		rows:   []map[string]any{}, // empty — shouldn't be queried
	}

	writer := &captureWriter{}
	store := progress.NewMemoryStore()

	// Pre-populate progress: chunks already complete
	store.Save(context.Background(), "T", []string{"__COMPLETE__"}, 150)

	cfg := config.Table{Name: "T", ChunkSize: 10}
	r := replicator.New(cdc, chunks, writer, store, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	go func() {
		<-cdcEvents
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	err := r.Run(ctx)
	if err != nil && err != context.Canceled {
		t.Fatalf("Run() error: %v", err)
	}

	// CDC should start from saved SCN (150)
	if cdc.startSCN != 150 {
		t.Errorf("CDC should start from saved SCN 150, got %d", cdc.startSCN)
	}

	// Should process the CDC event directly (no chunk loading)
	found := map[string]event.Event{}
	for _, e := range writer.events {
		found[event.EncodePK(e.PK)] = e
	}

	if _, ok := found["99"]; !ok {
		t.Error("should have written CDC event for PK 99")
	}
}

func TestReplicator_CDCErrorDuringChunks(t *testing.T) {
	cdc := &mockCDCSource{
		streamFn: func(ctx context.Context, startSCN uint64, ch chan<- event.Event) error {
			return fmt.Errorf("CDC connection lost")
		},
	}

	chunks := &mockChunkQuerier{
		pkCols: []string{"ID"},
		rows: []map[string]any{
			{"ID": int64(1), "V": "v1"},
		},
	}

	writer := &captureWriter{}
	store := progress.NewMemoryStore()

	cfg := config.Table{Name: "T", ChunkSize: 10}
	r := replicator.New(cdc, chunks, writer, store, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := r.Run(ctx)

	// The error should eventually surface — either as the CDC error
	// or as a context timeout. Currently the CDC error sits unread
	// until chunks complete.
	if err == nil {
		t.Fatal("expected error from CDC failure")
	}
	t.Logf("got error (expected): %v", err)
}

// TestReplicator_CDCDiesDuringSlowChunks verifies behavior when CDC dies
// while chunk loading is still in progress across multiple iterations.
// The replicator continues loading chunks without CDC dedup — any mutations
// that occurred after CDC died are silently lost. The error only surfaces
// after all chunks complete.
func TestReplicator_CDCDiesDuringSlowChunks(t *testing.T) {
	cdcDied := make(chan struct{})
	cdc := &mockCDCSource{
		streamFn: func(ctx context.Context, startSCN uint64, ch chan<- event.Event) error {
			// Send one CDC event then die
			ch <- event.Event{Table: "T", Op: event.OpUpdate, SCN: 105, PK: []string{"1"},
				Columns: map[string]any{"ID": int64(1), "V": "cdc_v1"}}
			close(cdcDied)
			return fmt.Errorf("CDC connection lost")
		},
	}

	chunks := &mockChunkQuerier{
		pkCols: []string{"ID"},
		rows: []map[string]any{
			{"ID": int64(1), "V": "chunk_1"},
			{"ID": int64(2), "V": "chunk_2"},
			{"ID": int64(3), "V": "chunk_3"},
			{"ID": int64(4), "V": "chunk_4"},
			{"ID": int64(5), "V": "chunk_5"},
		},
		onQuery: func() { <-cdcDied }, // ensure CDC has died before first chunk returns
	}

	writer := &captureWriter{}
	store := progress.NewMemoryStore()

	// chunkSize=1: 5 iterations, CDC dies before first chunk returns
	cfg := config.Table{Name: "T", ChunkSize: 1}
	r := replicator.New(cdc, chunks, writer, store, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := r.Run(ctx)

	// Error should surface after chunks complete
	if err == nil {
		t.Fatal("expected CDC error to surface")
	}
	if err.Error() != "CDC connection lost" {
		t.Errorf("expected 'CDC connection lost', got: %v", err)
	}

	// All 5 chunks should have been written (replicator didn't abort early)
	found := map[string]bool{}
	for _, e := range writer.events {
		found[event.EncodePK(e.PK)] = true
	}
	for i := 1; i <= 5; i++ {
		pk := fmt.Sprint(i)
		if !found[pk] {
			t.Errorf("PK %s should have been written (chunks continue after CDC death)", pk)
		}
	}

	// Progress should be saved as __COMPLETE__ (chunks finished)
	state, _ := store.Get(context.Background(), "T")
	if len(state.LastPK) != 1 || state.LastPK[0] != "__COMPLETE__" {
		t.Errorf("chunks should have completed despite CDC death, got %v", state.LastPK)
	}

	t.Log("BUG: replicator continued all 5 chunk iterations after CDC died — no dedup for those chunks")
}

func TestReplicator_EmptyTable(t *testing.T) {
	cdc := &mockCDCSource{}
	chunks := &mockChunkQuerier{
		pkCols: []string{"ID"},
		rows:   []map[string]any{}, // no rows
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

	// No rows should be written
	if len(writer.events) != 0 {
		t.Errorf("expected 0 events for empty table, got %d", len(writer.events))
	}

	// Progress should still be saved as __COMPLETE__
	state, _ := store.Get(context.Background(), "T")
	if len(state.LastPK) != 1 || state.LastPK[0] != "__COMPLETE__" {
		t.Errorf("empty table should still mark chunks complete, got %v", state.LastPK)
	}
}
