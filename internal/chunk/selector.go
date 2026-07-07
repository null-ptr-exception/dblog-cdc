package chunk

import (
	"context"

	"github.com/null-ptr-exception/dblog-cdc/internal/event"
)

type Querier interface {
	QueryChunk(ctx context.Context, table string, afterPK []string, limit int, scn uint64) (*event.ChunkResult, error)
	CurrentSCN(ctx context.Context) (uint64, error)
}

type Selector struct {
	querier Querier
}

func NewSelector(q Querier) *Selector {
	return &Selector{querier: q}
}

func (s *Selector) Next(ctx context.Context, table string, afterPK []string, chunkSize int, scn uint64) (*event.ChunkResult, error) {
	return s.querier.QueryChunk(ctx, table, afterPK, chunkSize, scn)
}
