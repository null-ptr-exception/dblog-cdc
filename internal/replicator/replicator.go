package replicator

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/null-ptr-exception/dblog-cdc/internal/buffer"
	"github.com/null-ptr-exception/dblog-cdc/internal/chunk"
	"github.com/null-ptr-exception/dblog-cdc/internal/config"
	"github.com/null-ptr-exception/dblog-cdc/internal/event"
	"github.com/null-ptr-exception/dblog-cdc/internal/progress"
	"github.com/null-ptr-exception/dblog-cdc/internal/transform"
)

const chunksCompleteMarker = "__COMPLETE__"

type CDCSource interface {
	Stream(ctx context.Context, startSCN uint64, events chan<- event.Event) error
	LastSCN() uint64
}

type BatchWriter interface {
	WriteBatch(ctx context.Context, events []event.Event) error
}

type Replicator struct {
	cdc         CDCSource
	chunks      chunk.Querier
	writer      BatchWriter
	store       progress.Store
	table       config.Table
	transformer *transform.Transformer
}

func New(cdc CDCSource, chunks chunk.Querier, writer BatchWriter, store progress.Store, table config.Table) *Replicator {
	return &Replicator{
		cdc:    cdc,
		chunks: chunks,
		writer: writer,
		store:  store,
		table:  table,
	}
}

func (r *Replicator) SetTransformer(t *transform.Transformer) {
	r.transformer = t
}

func (r *Replicator) transformEvents(events []event.Event) {
	if r.transformer == nil {
		return
	}
	for i := range events {
		r.transformer.TransformEvent(&events[i])
	}
}

func (r *Replicator) Run(ctx context.Context) error {
	state, err := r.store.Get(ctx, r.table.Name)
	if err != nil {
		return err
	}

	var lastPK string
	chunksComplete := false
	if state.LastPK != nil {
		if *state.LastPK == chunksCompleteMarker {
			chunksComplete = true
		} else {
			lastPK = *state.LastPK
		}
	}

	cdcCh := make(chan event.Event, 1000)
	cdcErrCh := make(chan error, 1)

	go func() {
		cdcErrCh <- r.cdc.Stream(ctx, state.LastSCN, cdcCh)
	}()

	buf := buffer.New()
	selector := chunk.NewSelector(r.chunks)

	for {
	drainLoop:
		for {
			select {
			case ev := <-cdcCh:
				if !chunksComplete {
					buf.ApplyCDCDedup(ev)
				} else {
					buf.PushCDC(ev)
				}
			default:
				break drainLoop
			}
		}

		if chunksComplete {
			events := buf.Drain()
			if len(events) > 0 {
				r.transformEvents(events)
				if err := r.writer.WriteBatch(ctx, events); err != nil {
					return err
				}
			}
		}

		if !chunksComplete {
			scnBefore, err := r.chunks.CurrentSCN(ctx)
			if err != nil {
				return fmt.Errorf("current SCN: %w", err)
			}

			chunkResult, err := selector.Next(ctx, r.table.Name, lastPK, r.table.ChunkSize, scnBefore)
			if err != nil {
				return err
			}

			buf.PushChunk(*chunkResult)

		drainAfterChunk:
			for {
				select {
				case ev := <-cdcCh:
					buf.ApplyCDCDedup(ev)
				default:
					break drainAfterChunk
				}
			}

			events := buf.Drain()
			if len(events) > 0 {
				r.transformEvents(events)
				if err := r.writer.WriteBatch(ctx, events); err != nil {
					return err
				}
			}

			lastPK = chunkResult.LastPK
			pk := lastPK
			if chunkResult.Complete {
				pk = chunksCompleteMarker
				chunksComplete = true
			}
			if err := r.store.Save(ctx, r.table.Name, &pk, scnBefore); err != nil {
				return err
			}

			if chunkResult.Complete {
				slog.Info("chunk loading complete", "table", r.table.Name, "last_pk", lastPK)
			}

			continue
		}

		select {
		case ev := <-cdcCh:
			buf.PushCDC(ev)
			events := buf.Drain()
			if len(events) > 0 {
				r.transformEvents(events)
				if err := r.writer.WriteBatch(ctx, events); err != nil {
					return err
				}
			}
		case err := <-cdcErrCh:
			return err
		case <-ctx.Done():
			events := buf.Drain()
			if len(events) > 0 {
				r.transformEvents(events)
				r.writer.WriteBatch(ctx, events)
			}
			return ctx.Err()
		}
	}
}
