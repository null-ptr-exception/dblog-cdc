package writer

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

type pgExecutor struct {
	pool *pgxpool.Pool
}

func (p *pgExecutor) ExecContext(ctx context.Context, query string, args ...any) error {
	_, err := p.pool.Exec(ctx, query, args...)
	return err
}

func NewPgWriter(pool *pgxpool.Pool, pkCol string) *Writer {
	return New(&pgExecutor{pool: pool}, pkCol)
}
