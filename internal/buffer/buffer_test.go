package buffer_test

import (
	"testing"

	"github.com/null-ptr-exception/dblog-cdc/internal/buffer"
	"github.com/null-ptr-exception/dblog-cdc/internal/event"
)

func TestBuffer_CDCOnly(t *testing.T) {
	b := buffer.New()

	b.PushCDC(event.Event{Table: "T", Op: event.OpInsert, SCN: 100, PK: 1, Columns: map[string]any{"v": "a"}})
	b.PushCDC(event.Event{Table: "T", Op: event.OpUpdate, SCN: 101, PK: 2, Columns: map[string]any{"v": "b"}})

	out := b.Drain()
	if len(out) != 2 {
		t.Fatalf("got %d events, want 2", len(out))
	}
	if out[0].PK != 1 || out[1].PK != 2 {
		t.Errorf("wrong PKs: %d, %d", out[0].PK, out[1].PK)
	}
}

func TestBuffer_ChunkOnly(t *testing.T) {
	b := buffer.New()

	chunk := event.ChunkResult{
		Table: "T",
		SCN:   100,
		Rows: map[int64]map[string]any{
			1: {"v": "a"},
			2: {"v": "b"},
			3: {"v": "c"},
		},
		LastPK: 3,
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
		Rows: map[int64]map[string]any{
			1: {"v": "chunk_1"},
			2: {"v": "chunk_2"},
			3: {"v": "chunk_3"},
		},
		LastPK: 3,
	}
	b.PushChunk(chunk)

	b.ApplyCDCDedup(event.Event{Table: "T", Op: event.OpUpdate, SCN: 105, PK: 2, Columns: map[string]any{"v": "cdc_2"}})

	out := b.Drain()
	if len(out) != 3 {
		t.Fatalf("got %d events, want 3", len(out))
	}

	byPK := map[int64]event.Event{}
	for _, e := range out {
		byPK[e.PK] = e
	}

	if byPK[1].Columns["v"] != "chunk_1" {
		t.Errorf("PK 1 should be from chunk, got %v", byPK[1].Columns["v"])
	}
	if byPK[2].Columns["v"] != "cdc_2" {
		t.Errorf("PK 2 should be from CDC, got %v", byPK[2].Columns["v"])
	}
	if byPK[3].Columns["v"] != "chunk_3" {
		t.Errorf("PK 3 should be from chunk, got %v", byPK[3].Columns["v"])
	}
}

func TestBuffer_CDCBeforeChunkSCN_NoDedup(t *testing.T) {
	b := buffer.New()

	chunk := event.ChunkResult{
		Table: "T",
		SCN:   100,
		Rows: map[int64]map[string]any{
			1: {"v": "chunk_1"},
		},
		LastPK: 1,
	}
	b.PushChunk(chunk)

	b.ApplyCDCDedup(event.Event{Table: "T", Op: event.OpUpdate, SCN: 90, PK: 1, Columns: map[string]any{"v": "old"}})

	out := b.Drain()
	byPK := map[int64]event.Event{}
	for _, e := range out {
		byPK[e.PK] = e
	}
	if byPK[1].Columns["v"] != "chunk_1" {
		t.Errorf("PK 1 should be from chunk (newer), got %v", byPK[1].Columns["v"])
	}
}

func TestBuffer_CDCDeleteRemovesChunkRow(t *testing.T) {
	b := buffer.New()

	chunk := event.ChunkResult{
		Table: "T",
		SCN:   100,
		Rows: map[int64]map[string]any{
			1: {"v": "chunk_1"},
			2: {"v": "chunk_2"},
		},
		LastPK: 2,
	}
	b.PushChunk(chunk)

	b.ApplyCDCDedup(event.Event{Table: "T", Op: event.OpDelete, SCN: 105, PK: 1})

	out := b.Drain()
	if len(out) != 2 {
		t.Fatalf("got %d events, want 2 (1 delete + 1 chunk row)", len(out))
	}

	byPK := map[int64]event.Event{}
	for _, e := range out {
		byPK[e.PK] = e
	}
	if byPK[1].Op != event.OpDelete {
		t.Errorf("PK 1 should be DELETE, got %v", byPK[1].Op)
	}
	if byPK[2].Columns["v"] != "chunk_2" {
		t.Errorf("PK 2 should be from chunk, got %v", byPK[2].Columns["v"])
	}
}

func TestBuffer_NoChunk_DrainReturnsEmpty(t *testing.T) {
	b := buffer.New()
	out := b.Drain()
	if len(out) != 0 {
		t.Errorf("expected empty drain, got %d", len(out))
	}
}
