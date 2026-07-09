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

// Pairwise combinatorial tests covering:
//   Factor A: Replicator state   — fresh, resume-mid-chunk, resume-complete
//   Factor B: PK cardinality     — single, compound
//   Factor C: CDC operation       — INSERT, UPDATE, DELETE
//   Factor D: CDC-vs-chunk timing — before SCN, after SCN, equal SCN
//   Factor E: Chunk position      — first, middle, last
//
// Each test covers a unique combination of pairs not covered by other tests.

func runReplicator(t *testing.T, cdc *mockCDCSource, chunks *mockChunkQuerier, store *progress.MemoryStore, cfg config.Table, timeout time.Duration) *captureWriter {
	t.Helper()
	w := &captureWriter{}
	r := replicator.New(cdc, chunks, w, store, cfg)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	err := r.Run(ctx)
	if err != nil && err != context.DeadlineExceeded && err != context.Canceled {
		t.Fatalf("Run() error: %v", err)
	}
	return w
}

// Test 1: resume-mid + single PK + DELETE + after SCN + last chunk
// Resume skips rows 1-3, chunk returns 4,5 (last/partial), CDC deletes PK 4.
func TestCombo_ResumeMid_Single_Delete_AfterSCN_LastChunk(t *testing.T) {
	cdc := &mockCDCSource{
		events: []event.Event{
			{Table: "T", Op: event.OpDelete, SCN: 105, PK: []string{"4"}},
		},
	}

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

	store := progress.NewMemoryStore()
	store.Save(context.Background(), "T", []string{"3"}, 50)

	w := runReplicator(t, cdc, chunks, store, config.Table{Name: "T", ChunkSize: 10}, 2*time.Second)

	found := map[string]event.Event{}
	for _, e := range w.events {
		found[event.EncodePK(e.PK)] = e
	}

	if _, ok := found["1"]; ok {
		t.Error("PK 1 should be skipped (resume past it)")
	}
	if _, ok := found["3"]; ok {
		t.Error("PK 3 should be skipped (resume past it)")
	}
	if e, ok := found["4"]; !ok {
		t.Error("PK 4 should have a DELETE event")
	} else if e.Op != event.OpDelete {
		t.Errorf("PK 4 should be DELETE, got %v", e.Op)
	}
	if e, ok := found["5"]; !ok {
		t.Error("PK 5 should be from chunk")
	} else if e.Columns["V"] != "v5" {
		t.Errorf("PK 5 value mismatch: got %v", e.Columns["V"])
	}

	// Progress should be saved as __COMPLETE__
	state, _ := store.Get(context.Background(), "T")
	if len(state.LastPK) != 1 || state.LastPK[0] != "__COMPLETE__" {
		t.Errorf("expected __COMPLETE__ marker, got %v", state.LastPK)
	}
}

// Test 2: resume-complete + compound PK + INSERT then UPDATE + CDC-only
// Chunks already done. CDC sends insert then update for same compound PK.
func TestCombo_ResumeComplete_Compound_InsertUpdate_CDCOnly(t *testing.T) {
	cdc := &mockCDCSource{
		streamFn: func(ctx context.Context, startSCN uint64, ch chan<- event.Event) error {
			ch <- event.Event{Table: "T", Op: event.OpInsert, SCN: 200, PK: []string{"A", "1"},
				Columns: map[string]any{"K1": "A", "K2": "1", "V": "inserted"}}
			ch <- event.Event{Table: "T", Op: event.OpUpdate, SCN: 201, PK: []string{"A", "1"},
				Columns: map[string]any{"K1": "A", "K2": "1", "V": "updated"}}
			<-ctx.Done()
			return ctx.Err()
		},
	}

	chunks := &mockChunkQuerier{pkCols: []string{"K1", "K2"}}

	store := progress.NewMemoryStore()
	store.Save(context.Background(), "T", []string{"__COMPLETE__"}, 150)

	w := runReplicator(t, cdc, chunks, store, config.Table{Name: "T", ChunkSize: 10}, 2*time.Second)

	if cdc.startSCN != 150 {
		t.Errorf("CDC should start from saved SCN 150, got %d", cdc.startSCN)
	}

	// Both events should be written in order
	pk := event.EncodePK([]string{"A", "1"})
	var ops []event.OpType
	for _, e := range w.events {
		if event.EncodePK(e.PK) == pk {
			ops = append(ops, e.Op)
		}
	}
	if len(ops) != 2 {
		t.Fatalf("expected 2 events for PK [A,1], got %d", len(ops))
	}
	if ops[0] != event.OpInsert || ops[1] != event.OpUpdate {
		t.Errorf("expected INSERT then UPDATE, got %v", ops)
	}
}

// Test 3: fresh + single PK + UPDATE + equal SCN + first chunk
// CDC event at same SCN as chunk for a PK in the chunk.
// The chunk snapshot already reflects this change — CDC event should be dropped.
// Only the chunk row should be written.
func TestCombo_Fresh_Single_Update_EqualSCN_FirstChunk(t *testing.T) {
	cdcSent := make(chan struct{})
	cdc := &mockCDCSource{
		streamFn: func(ctx context.Context, startSCN uint64, ch chan<- event.Event) error {
			ch <- event.Event{Table: "T", Op: event.OpUpdate, SCN: 100, PK: []string{"1"},
				Columns: map[string]any{"ID": int64(1), "V": "cdc_version"}}
			close(cdcSent)
			<-ctx.Done()
			return ctx.Err()
		},
	}

	chunks := &mockChunkQuerier{
		pkCols: []string{"ID"},
		rows: []map[string]any{
			{"ID": int64(1), "V": "chunk_version"},
			{"ID": int64(2), "V": "v2"},
		},
		onQuery: func() { <-cdcSent },
	}

	store := progress.NewMemoryStore()
	w := runReplicator(t, cdc, chunks, store, config.Table{Name: "T", ChunkSize: 10}, 2*time.Second)

	// Stale CDC (SCN == chunk SCN) should be dropped. Only chunk row written for PK 1.
	var pk1Events []event.Event
	for _, e := range w.events {
		if event.EncodePK(e.PK) == "1" {
			pk1Events = append(pk1Events, e)
		}
	}

	if len(pk1Events) != 1 {
		t.Fatalf("expected 1 event for PK 1 (chunk only, stale CDC dropped), got %d", len(pk1Events))
	}
	if pk1Events[0].Columns["V"] != "chunk_version" {
		t.Errorf("PK 1 should be chunk version, got %v", pk1Events[0].Columns["V"])
	}
}

// Test 4: fresh + compound PK + UPDATE + after SCN + middle chunk
// Multi-chunk loading with compound PKs. CDC update arrives for a row
// in the second chunk batch; dedup removes chunk row, CDC wins.
func TestCombo_Fresh_Compound_Update_AfterSCN_MiddleChunk(t *testing.T) {
	cdc := &mockCDCSource{
		events: []event.Event{
			{Table: "T", Op: event.OpUpdate, SCN: 105, PK: []string{"B", "1"},
				Columns: map[string]any{"K1": "B", "K2": "1", "V": "cdc_updated"}},
		},
	}

	chunks := &mockChunkQuerier{
		pkCols: []string{"K1", "K2"},
		rows: []map[string]any{
			{"K1": "A", "K2": "1", "V": "a1"},
			{"K1": "A", "K2": "2", "V": "a2"},
			{"K1": "B", "K2": "1", "V": "b1_chunk"},
			{"K1": "B", "K2": "2", "V": "b2"},
		},
	}

	store := progress.NewMemoryStore()
	// chunkSize=2 forces 2 chunk batches: [A1,A2] then [B1,B2]
	w := runReplicator(t, cdc, chunks, store, config.Table{Name: "T", ChunkSize: 2}, 2*time.Second)

	found := map[string]event.Event{}
	for _, e := range w.events {
		found[event.EncodePK(e.PK)] = e
	}

	b1Key := event.EncodePK([]string{"B", "1"})
	if e, ok := found[b1Key]; !ok {
		t.Error("PK [B,1] should be present")
	} else if e.Columns["V"] != "cdc_updated" {
		t.Errorf("PK [B,1] should be from CDC (dedup), got %v", e.Columns["V"])
	}

	a1Key := event.EncodePK([]string{"A", "1"})
	if e, ok := found[a1Key]; !ok {
		t.Error("PK [A,1] should be from chunk")
	} else if e.Columns["V"] != "a1" {
		t.Errorf("PK [A,1] value mismatch: got %v", e.Columns["V"])
	}
}

// Test 5: resume-mid + compound PK + INSERT + after SCN + first chunk
// Resume from compound PK, new row inserted via CDC during first post-resume chunk.
func TestCombo_ResumeMid_Compound_Insert_AfterSCN_FirstChunk(t *testing.T) {
	cdc := &mockCDCSource{
		events: []event.Event{
			{Table: "T", Op: event.OpInsert, SCN: 105, PK: []string{"C", "1"},
				Columns: map[string]any{"K1": "C", "K2": "1", "V": "cdc_new"}},
		},
	}

	chunks := &mockChunkQuerier{
		pkCols: []string{"K1", "K2"},
		rows: []map[string]any{
			{"K1": "A", "K2": "1", "V": "a1"},
			{"K1": "B", "K2": "1", "V": "b1"},
			{"K1": "C", "K2": "1", "V": "c1_chunk"},
			{"K1": "D", "K2": "1", "V": "d1"},
		},
	}

	store := progress.NewMemoryStore()
	// Resume past [B,1]
	store.Save(context.Background(), "T", []string{"B", "1"}, 50)

	w := runReplicator(t, cdc, chunks, store, config.Table{Name: "T", ChunkSize: 10}, 2*time.Second)

	if cdc.startSCN != 50 {
		t.Errorf("CDC should start from saved SCN 50, got %d", cdc.startSCN)
	}

	found := map[string]event.Event{}
	for _, e := range w.events {
		found[event.EncodePK(e.PK)] = e
	}

	a1Key := event.EncodePK([]string{"A", "1"})
	if _, ok := found[a1Key]; ok {
		t.Error("PK [A,1] should be skipped (resume past it)")
	}

	c1Key := event.EncodePK([]string{"C", "1"})
	if e, ok := found[c1Key]; !ok {
		t.Error("PK [C,1] should be present")
	} else if e.Columns["V"] != "cdc_new" {
		t.Errorf("PK [C,1] should be from CDC (dedup), got %v", e.Columns["V"])
	}

	d1Key := event.EncodePK([]string{"D", "1"})
	if _, ok := found[d1Key]; !ok {
		t.Error("PK [D,1] should be from chunk")
	}
}

// Test 6: fresh + single PK + DELETE + before SCN + middle chunk
// CDC delete at SCN 90 for a PK that exists in the chunk at SCN 100.
// The chunk is authoritative — the stale CDC DELETE should be dropped.
// The chunk row should be the only event for PK 2.
func TestCombo_Fresh_Single_Delete_BeforeSCN_MiddleChunk(t *testing.T) {
	cdcSent := make(chan struct{})
	cdc := &mockCDCSource{
		streamFn: func(ctx context.Context, startSCN uint64, ch chan<- event.Event) error {
			ch <- event.Event{Table: "T", Op: event.OpDelete, SCN: 90, PK: []string{"2"}}
			close(cdcSent)
			<-ctx.Done()
			return ctx.Err()
		},
	}

	chunks := &mockChunkQuerier{
		pkCols: []string{"ID"},
		rows: []map[string]any{
			{"ID": int64(1), "V": "v1"},
			{"ID": int64(2), "V": "v2"},
			{"ID": int64(3), "V": "v3"},
		},
		onQuery: func() { <-cdcSent },
	}

	store := progress.NewMemoryStore()
	w := runReplicator(t, cdc, chunks, store, config.Table{Name: "T", ChunkSize: 10}, 2*time.Second)

	// Stale CDC DELETE (SCN < chunk SCN) for a PK in chunk should be dropped.
	var pk2Events []event.Event
	for _, e := range w.events {
		if event.EncodePK(e.PK) == "2" {
			pk2Events = append(pk2Events, e)
		}
	}

	if len(pk2Events) != 1 {
		t.Fatalf("expected 1 event for PK 2 (chunk only, stale CDC dropped), got %d", len(pk2Events))
	}
	if pk2Events[0].Op != event.OpInsert {
		t.Errorf("PK 2 should be INSERT from chunk, got %v", pk2Events[0].Op)
	}
	if pk2Events[0].Columns["V"] != "v2" {
		t.Errorf("PK 2 should have V=v2 from chunk, got %v", pk2Events[0].Columns["V"])
	}
}
