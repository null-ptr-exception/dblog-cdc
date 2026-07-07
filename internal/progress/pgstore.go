package progress

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

type PgStore struct {
	pool      *pgxpool.Pool
	tableName string
}

func NewPgStore(pool *pgxpool.Pool, tableName string) *PgStore {
	return &PgStore{pool: pool, tableName: tableName}
}

func (s *PgStore) EnsureTable(ctx context.Context) error {
	query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		table_name  TEXT PRIMARY KEY,
		last_pk     TEXT,
		last_lsn    BIGINT,
		updated_at  TIMESTAMPTZ DEFAULT now()
	)`, s.tableName)
	_, err := s.pool.Exec(ctx, query)
	return err
}

func (s *PgStore) Get(ctx context.Context, table string) (State, error) {
	var state State
	var lastPKStr *string
	var lastSCN *int64

	query := fmt.Sprintf("SELECT last_pk, last_lsn FROM %s WHERE table_name = $1", s.tableName)
	err := s.pool.QueryRow(ctx, query, table).Scan(&lastPKStr, &lastSCN)
	if err != nil {
		if err.Error() == "no rows in result set" {
			return State{}, nil
		}
		return State{}, fmt.Errorf("get progress: %w", err)
	}

	if lastPKStr != nil {
		// Try JSON array first, fall back to plain string for backwards compat
		var pk []string
		if err := json.Unmarshal([]byte(*lastPKStr), &pk); err != nil {
			// Plain string value from old format
			pk = []string{*lastPKStr}
		}
		state.LastPK = pk
	}
	if lastSCN != nil {
		state.LastSCN = uint64(*lastSCN)
	}
	return state, nil
}

func (s *PgStore) Save(ctx context.Context, table string, lastPK []string, lastSCN uint64) error {
	var lastPKStr *string
	if lastPK != nil {
		data, err := json.Marshal(lastPK)
		if err != nil {
			return fmt.Errorf("marshal lastPK: %w", err)
		}
		s := string(data)
		lastPKStr = &s
	}

	query := fmt.Sprintf(`INSERT INTO %s (table_name, last_pk, last_lsn, updated_at)
		VALUES ($1, $2, $3, now())
		ON CONFLICT (table_name) DO UPDATE SET
			last_pk = EXCLUDED.last_pk,
			last_lsn = EXCLUDED.last_lsn,
			updated_at = now()`, s.tableName)

	scn := int64(lastSCN)
	_, err := s.pool.Exec(ctx, query, table, lastPKStr, &scn)
	return err
}
