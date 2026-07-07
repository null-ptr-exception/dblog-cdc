package progress_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/null-ptr-exception/dblog-cdc/internal/progress"
)

func TestMemoryStore_GetSet(t *testing.T) {
	s := progress.NewMemoryStore()
	ctx := context.Background()

	state, err := s.Get(ctx, "ORDERS")
	if err != nil {
		t.Fatalf("Get() error: %v", err)
	}
	if state.LastPK != nil {
		t.Errorf("expected nil LastPK, got %v", state.LastPK)
	}

	pk := []string{"500"}
	scn := uint64(12345)
	err = s.Save(ctx, "ORDERS", pk, scn)
	if err != nil {
		t.Fatalf("Save() error: %v", err)
	}

	state, err = s.Get(ctx, "ORDERS")
	if err != nil {
		t.Fatalf("Get() error: %v", err)
	}
	if !reflect.DeepEqual(state.LastPK, []string{"500"}) {
		t.Errorf("LastPK = %v, want [500]", state.LastPK)
	}
	if state.LastSCN != 12345 {
		t.Errorf("LastSCN = %d, want 12345", state.LastSCN)
	}
}

func TestMemoryStore_MarkComplete(t *testing.T) {
	s := progress.NewMemoryStore()
	ctx := context.Background()

	complete := []string{"__COMPLETE__"}
	err := s.Save(ctx, "ORDERS", complete, 99999)
	if err != nil {
		t.Fatalf("Save() error: %v", err)
	}

	state, err := s.Get(ctx, "ORDERS")
	if err != nil {
		t.Fatalf("Get() error: %v", err)
	}
	if !reflect.DeepEqual(state.LastPK, []string{"__COMPLETE__"}) {
		t.Errorf("LastPK = %v, want [__COMPLETE__]", state.LastPK)
	}
}
