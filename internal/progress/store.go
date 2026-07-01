package progress

import (
	"context"
	"sync"
)

type State struct {
	LastPK  *int64
	LastSCN uint64
}

type Store interface {
	Get(ctx context.Context, table string) (State, error)
	Save(ctx context.Context, table string, lastPK *int64, lastSCN uint64) error
}

type MemoryStore struct {
	mu    sync.Mutex
	state map[string]State
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{state: make(map[string]State)}
}

func (m *MemoryStore) Get(_ context.Context, table string) (State, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.state[table], nil
}

func (m *MemoryStore) Save(_ context.Context, table string, lastPK *int64, lastSCN uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state[table] = State{LastPK: lastPK, LastSCN: lastSCN}
	return nil
}
