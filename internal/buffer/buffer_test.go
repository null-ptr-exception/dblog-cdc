package buffer_test

import (
	"testing"

	"github.com/null-ptr-exception/dblog-cdc/internal/buffer"
	"github.com/null-ptr-exception/dblog-cdc/internal/event"
)

func TestBuffer_CDCOnly(t *testing.T) {
	b := buffer.New()

	b.PushCDC(event.Event{Table: "T", Op: event.OpInsert, SCN: 100, PK: []string{"1"}, Columns: map[string]any{"v": "a"}})
	b.PushCDC(event.Event{Table: "T", Op: event.OpUpdate, SCN: 101, PK: []string{"2"}, Columns: map[string]any{"v": "b"}})

	out := b.Drain()
	if len(out) != 2 {
		t.Fatalf("got %d events, want 2", len(out))
	}
	if event.EncodePK(out[0].PK) != "1" || event.EncodePK(out[1].PK) != "2" {
		t.Errorf("wrong PKs: %v, %v", out[0].PK, out[1].PK)
	}
}

func TestBuffer_ChunkOnly(t *testing.T) {
	b := buffer.New()

	chunk := event.ChunkResult{
		Table: "T",
		SCN:   100,
		Rows: map[string]event.ChunkRow{
			"1": {PK: []string{"1"}, Columns: map[string]any{"v": "a"}},
			"2": {PK: []string{"2"}, Columns: map[string]any{"v": "b"}},
			"3": {PK: []string{"3"}, Columns: map[string]any{"v": "c"}},
		},
		LastPK: []string{"3"},
	}
	b.PushChunk(chunk)

	out := b.Drain()
	if len(out) != 3 {
		t.Fatalf("got %d events, want 3", len(out))
	}
}

func TestBuffer_CDCWinsOverChunk(t *testing.T) {
	b := buffer.New()

	chunk := event.ChunkResult{
		Table: "T",
		SCN:   100,
		Rows: map[string]event.ChunkRow{
			"1": {PK: []string{"1"}, Columns: map[string]any{"v": "chunk_1"}},
			"2": {PK: []string{"2"}, Columns: map[string]any{"v": "chunk_2"}},
			"3": {PK: []string{"3"}, Columns: map[string]any{"v": "chunk_3"}},
		},
		LastPK: []string{"3"},
	}
	b.PushChunk(chunk)

	b.ApplyCDCDedup(event.Event{Table: "T", Op: event.OpUpdate, SCN: 105, PK: []string{"2"}, Columns: map[string]any{"v": "cdc_2"}})

	out := b.Drain()
	if len(out) != 3 {
		t.Fatalf("got %d events, want 3", len(out))
	}

	byPK := map[string]event.Event{}
	for _, e := range out {
		byPK[event.EncodePK(e.PK)] = e
	}

	if byPK["1"].Columns["v"] != "chunk_1" {
		t.Errorf("PK 1 should be from chunk, got %v", byPK["1"].Columns["v"])
	}
	if byPK["2"].Columns["v"] != "cdc_2" {
		t.Errorf("PK 2 should be from CDC, got %v", byPK["2"].Columns["v"])
	}
	if byPK["3"].Columns["v"] != "chunk_3" {
		t.Errorf("PK 3 should be from chunk, got %v", byPK["3"].Columns["v"])
	}
}

func TestBuffer_CDCBeforeChunkSCN_Dropped(t *testing.T) {
	b := buffer.New()

	chunk := event.ChunkResult{
		Table: "T",
		SCN:   100,
		Rows: map[string]event.ChunkRow{
			"1": {PK: []string{"1"}, Columns: map[string]any{"v": "chunk_1"}},
		},
		LastPK: []string{"1"},
	}
	b.PushChunk(chunk)

	// CDC event with SCN < chunk SCN for a PK that exists in the chunk.
	// The chunk snapshot is authoritative — this stale CDC event should be dropped.
	b.ApplyCDCDedup(event.Event{Table: "T", Op: event.OpUpdate, SCN: 90, PK: []string{"1"}, Columns: map[string]any{"v": "old"}})

	out := b.Drain()
	if len(out) != 1 {
		t.Fatalf("stale CDC event should be dropped, got %d events (want 1 chunk row)", len(out))
	}
	if out[0].Columns["v"] != "chunk_1" {
		t.Errorf("only event should be chunk row, got %v", out[0].Columns["v"])
	}
}

func TestBuffer_CDCDeleteRemovesChunkRow(t *testing.T) {
	b := buffer.New()

	chunk := event.ChunkResult{
		Table: "T",
		SCN:   100,
		Rows: map[string]event.ChunkRow{
			"1": {PK: []string{"1"}, Columns: map[string]any{"v": "chunk_1"}},
			"2": {PK: []string{"2"}, Columns: map[string]any{"v": "chunk_2"}},
		},
		LastPK: []string{"2"},
	}
	b.PushChunk(chunk)

	b.ApplyCDCDedup(event.Event{Table: "T", Op: event.OpDelete, SCN: 105, PK: []string{"1"}})

	out := b.Drain()
	if len(out) != 2 {
		t.Fatalf("got %d events, want 2 (1 delete + 1 chunk row)", len(out))
	}

	byPK := map[string]event.Event{}
	for _, e := range out {
		byPK[event.EncodePK(e.PK)] = e
	}
	if byPK["1"].Op != event.OpDelete {
		t.Errorf("PK 1 should be DELETE, got %v", byPK["1"].Op)
	}
	if byPK["2"].Columns["v"] != "chunk_2" {
		t.Errorf("PK 2 should be from chunk, got %v", byPK["2"].Columns["v"])
	}
}

func TestBuffer_NoChunk_DrainReturnsEmpty(t *testing.T) {
	b := buffer.New()
	out := b.Drain()
	if len(out) != 0 {
		t.Errorf("expected empty drain, got %d", len(out))
	}
}

func TestBuffer_CDCAtExactChunkSCN_Dropped(t *testing.T) {
	b := buffer.New()

	chunk := event.ChunkResult{
		Table: "T",
		SCN:   100,
		Rows: map[string]event.ChunkRow{
			"1": {PK: []string{"1"}, Columns: map[string]any{"v": "chunk_1"}},
		},
		LastPK: []string{"1"},
	}
	b.PushChunk(chunk)

	// CDC event with SCN == chunk SCN for a PK that exists in the chunk.
	// The chunk snapshot already reflects this change — drop the CDC event.
	b.ApplyCDCDedup(event.Event{Table: "T", Op: event.OpUpdate, SCN: 100, PK: []string{"1"}, Columns: map[string]any{"v": "cdc_same_scn"}})

	out := b.Drain()
	if len(out) != 1 {
		t.Fatalf("CDC at equal SCN should be dropped, got %d events (want 1 chunk row)", len(out))
	}
	if out[0].Columns["v"] != "chunk_1" {
		t.Errorf("only event should be chunk row, got %v", out[0].Columns["v"])
	}
}

func TestBuffer_MultipleCDCEventsForSamePK(t *testing.T) {
	b := buffer.New()

	chunk := event.ChunkResult{
		Table: "T",
		SCN:   100,
		Rows: map[string]event.ChunkRow{
			"1": {PK: []string{"1"}, Columns: map[string]any{"v": "chunk_1"}},
			"2": {PK: []string{"2"}, Columns: map[string]any{"v": "chunk_2"}},
		},
		LastPK: []string{"2"},
	}
	b.PushChunk(chunk)

	// Two CDC events for same PK (both SCN > chunk): update then delete.
	// First CDC dedupes chunk row. Second CDC: PK no longer in chunk, kept as regular CDC.
	b.ApplyCDCDedup(event.Event{Table: "T", Op: event.OpUpdate, SCN: 105, PK: []string{"1"}, Columns: map[string]any{"v": "updated"}})
	b.ApplyCDCDedup(event.Event{Table: "T", Op: event.OpDelete, SCN: 106, PK: []string{"1"}})

	out := b.Drain()

	// Expect: 2 CDC events for PK 1 (update + delete) + 1 chunk row for PK 2
	if len(out) != 3 {
		t.Fatalf("got %d events, want 3", len(out))
	}

	var pk1Events []event.Event
	for _, e := range out {
		if event.EncodePK(e.PK) == "1" {
			pk1Events = append(pk1Events, e)
		}
	}
	if len(pk1Events) != 2 {
		t.Fatalf("expected 2 CDC events for PK 1, got %d", len(pk1Events))
	}
	if pk1Events[0].Op != event.OpUpdate {
		t.Errorf("first CDC event for PK 1 should be UPDATE, got %v", pk1Events[0].Op)
	}
	if pk1Events[1].Op != event.OpDelete {
		t.Errorf("second CDC event for PK 1 should be DELETE, got %v", pk1Events[1].Op)
	}
}

func TestBuffer_StaleCDCForNonChunkPK_Kept(t *testing.T) {
	b := buffer.New()

	chunk := event.ChunkResult{
		Table: "T",
		SCN:   100,
		Rows: map[string]event.ChunkRow{
			"1": {PK: []string{"1"}, Columns: map[string]any{"v": "chunk_1"}},
		},
		LastPK: []string{"1"},
	}
	b.PushChunk(chunk)

	// CDC event with SCN < chunk SCN but for a PK NOT in the chunk.
	// This is a valid event (could be for a future chunk or out-of-range PK).
	b.ApplyCDCDedup(event.Event{Table: "T", Op: event.OpUpdate, SCN: 90, PK: []string{"5"}, Columns: map[string]any{"v": "other"}})

	out := b.Drain()
	if len(out) != 2 {
		t.Fatalf("got %d events, want 2 (chunk PK 1 + CDC PK 5)", len(out))
	}
}

func TestBuffer_CDCBeforeChunkPushed(t *testing.T) {
	b := buffer.New()

	// CDC arrives before any chunk is loaded
	b.ApplyCDCDedup(event.Event{Table: "T", Op: event.OpInsert, SCN: 95, PK: []string{"1"}, Columns: map[string]any{"v": "early_cdc"}})

	// Now push chunk
	chunk := event.ChunkResult{
		Table: "T",
		SCN:   100,
		Rows: map[string]event.ChunkRow{
			"1": {PK: []string{"1"}, Columns: map[string]any{"v": "chunk_1"}},
		},
		LastPK: []string{"1"},
	}
	b.PushChunk(chunk)

	out := b.Drain()
	if len(out) != 2 {
		t.Fatalf("got %d events, want 2 (early CDC + chunk)", len(out))
	}
}
