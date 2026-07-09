package buffer

import (
	"github.com/null-ptr-exception/dblog-cdc/internal/event"
)

type Buffer struct {
	chunk    *event.ChunkResult
	cdcDedup []event.Event
}

func New() *Buffer {
	return &Buffer{}
}

func (b *Buffer) PushCDC(e event.Event) {
	b.cdcDedup = append(b.cdcDedup, e)
}

func (b *Buffer) PushChunk(c event.ChunkResult) {
	b.chunk = &c
}

func (b *Buffer) ApplyCDCDedup(e event.Event) {
	if b.chunk == nil {
		b.cdcDedup = append(b.cdcDedup, e)
		return
	}

	key := event.EncodePK(e.PK)
	if _, exists := b.chunk.Rows[key]; exists {
		if e.SCN > b.chunk.SCN {
			delete(b.chunk.Rows, key)
			b.cdcDedup = append(b.cdcDedup, e)
		}
		// SCN <= chunk SCN: chunk is authoritative, drop stale CDC event
		return
	}

	b.cdcDedup = append(b.cdcDedup, e)
}

func (b *Buffer) Drain() []event.Event {
	var out []event.Event

	out = append(out, b.cdcDedup...)

	if b.chunk != nil {
		for _, row := range b.chunk.Rows {
			out = append(out, event.Event{
				Table:   b.chunk.Table,
				Op:      event.OpInsert,
				SCN:     b.chunk.SCN,
				PK:      row.PK,
				Columns: row.Columns,
			})
		}
	}

	b.chunk = nil
	b.cdcDedup = nil
	return out
}
