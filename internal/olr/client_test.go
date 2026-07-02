package olr

import (
	"testing"

	"github.com/null-ptr-exception/dblog-cdc/internal/event"
)

func TestConvertJSONPayload_Insert(t *testing.T) {
	p := jsonPayload{
		Op:     "c",
		Schema: jsonSchema{Owner: "TEST", Table: "ORDERS"},
		After: map[string]any{
			"ID":     float64(42),
			"AMOUNT": float64(99.95),
			"STATUS": "NEW",
		},
	}

	ev, err := convertJSONPayload(p, 12345, "ID")
	if err != nil {
		t.Fatalf("convertJSONPayload() error: %v", err)
	}
	if ev.Table != "ORDERS" {
		t.Errorf("Table = %q", ev.Table)
	}
	if ev.Op != event.OpInsert {
		t.Errorf("Op = %v", ev.Op)
	}
	if ev.SCN != 12345 {
		t.Errorf("SCN = %d", ev.SCN)
	}
	if ev.PK != "42" {
		t.Errorf("PK = %s", ev.PK)
	}
	if ev.Columns["AMOUNT"] != float64(99.95) {
		t.Errorf("AMOUNT = %v", ev.Columns["AMOUNT"])
	}
}

func TestConvertJSONPayload_Update(t *testing.T) {
	p := jsonPayload{
		Op:     "u",
		Schema: jsonSchema{Table: "ORDERS"},
		After: map[string]any{
			"ID":     float64(7),
			"STATUS": "SHIPPED",
		},
	}

	ev, err := convertJSONPayload(p, 200, "ID")
	if err != nil {
		t.Fatalf("convertJSONPayload() error: %v", err)
	}
	if ev.Op != event.OpUpdate {
		t.Errorf("Op = %v", ev.Op)
	}
	if ev.PK != "7" {
		t.Errorf("PK = %s", ev.PK)
	}
}

func TestConvertJSONPayload_Delete(t *testing.T) {
	p := jsonPayload{
		Op:     "d",
		Schema: jsonSchema{Table: "ORDERS"},
		Before: map[string]any{
			"ID": float64(3),
		},
	}

	ev, err := convertJSONPayload(p, 300, "ID")
	if err != nil {
		t.Fatalf("convertJSONPayload() error: %v", err)
	}
	if ev.Op != event.OpDelete {
		t.Errorf("Op = %v", ev.Op)
	}
	if ev.PK != "3" {
		t.Errorf("PK = %s", ev.PK)
	}
}

func TestConvertJSONPayload_SkipNonDML(t *testing.T) {
	for _, op := range []string{"begin", "commit", "ddl", "chkpt"} {
		p := jsonPayload{Op: op}
		_, err := convertJSONPayload(p, 100, "ID")
		if err != ErrSkipEvent {
			t.Errorf("Op %q should return ErrSkipEvent, got %v", op, err)
		}
	}
}

func TestConvertJSONPayload_StringPK(t *testing.T) {
	p := jsonPayload{
		Op:     "c",
		Schema: jsonSchema{Table: "BIG_TABLE"},
		After: map[string]any{
			"ID": "12345678901234567",
			"V":  "test",
		},
	}

	ev, err := convertJSONPayload(p, 100, "ID")
	if err != nil {
		t.Fatalf("convertJSONPayload() error: %v", err)
	}
	if ev.PK != "12345678901234567" {
		t.Errorf("PK = %s, want 12345678901234567", ev.PK)
	}
}

func TestConvertJSONPayload_NullColumn(t *testing.T) {
	p := jsonPayload{
		Op:     "c",
		Schema: jsonSchema{Table: "ORDERS"},
		After: map[string]any{
			"ID":     float64(1),
			"STATUS": nil,
		},
	}

	ev, err := convertJSONPayload(p, 100, "ID")
	if err != nil {
		t.Fatalf("convertJSONPayload() error: %v", err)
	}
	if ev.Columns["STATUS"] != nil {
		t.Errorf("STATUS = %v, want nil", ev.Columns["STATUS"])
	}
}
