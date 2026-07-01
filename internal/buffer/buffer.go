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

	if e.SCN > b.chunk.SCN {
		if _, exists := b.chunk.Rows[e.PK]; exists {
			delete(b.chunk.Rows, e.PK)
			b.cdcDedup = append(b.cdcDedup, e)
			return
		}
	}

	b.cdcDedup = append(b.cdcDedup, e)
}

func (b *Buffer) Drain() []event.Event {
	var out []event.Event

	out = append(out, b.cdcDedup...)

	if b.chunk != nil {
		for pk, cols := range b.chunk.Rows {
			out = append(out, event.Event{
				Table:   b.chunk.Table,
				Op:      event.OpInsert,
				SCN:     b.chunk.SCN,
				PK:      pk,
				Columns: cols,
			})
		}
	}

	b.chunk = nil
	b.cdcDedup = nil
	return out
}
