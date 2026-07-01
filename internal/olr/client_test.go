package olr_test

import (
	"testing"

	"github.com/null-ptr-exception/dblog-cdc/internal/event"
	"github.com/null-ptr-exception/dblog-cdc/internal/olr"
	pb "github.com/null-ptr-exception/dblog-cdc/pb"
)

func TestConvertPayload_Insert(t *testing.T) {
	payload := &pb.Payload{
		Op: pb.Op_INSERT,
		Schema: &pb.Schema{
			Owner: "TEST",
			Name:  "ORDERS",
		},
		After: []*pb.Value{
			{Name: "ID", Datum: &pb.Value_ValueInt{ValueInt: 42}},
			{Name: "AMOUNT", Datum: &pb.Value_ValueDouble{ValueDouble: 99.95}},
			{Name: "STATUS", Datum: &pb.Value_ValueString{ValueString: "NEW"}},
		},
	}

	ev, err := olr.ConvertPayload(payload, 12345)
	if err != nil {
		t.Fatalf("ConvertPayload() error: %v", err)
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
	if ev.PK != 42 {
		t.Errorf("PK = %d", ev.PK)
	}
	if ev.Columns["AMOUNT"] != 99.95 {
		t.Errorf("AMOUNT = %v", ev.Columns["AMOUNT"])
	}
}

func TestConvertPayload_Update(t *testing.T) {
	payload := &pb.Payload{
		Op:     pb.Op_UPDATE,
		Schema: &pb.Schema{Name: "ORDERS"},
		After: []*pb.Value{
			{Name: "ID", Datum: &pb.Value_ValueInt{ValueInt: 7}},
			{Name: "STATUS", Datum: &pb.Value_ValueString{ValueString: "SHIPPED"}},
		},
	}

	ev, err := olr.ConvertPayload(payload, 200)
	if err != nil {
		t.Fatalf("ConvertPayload() error: %v", err)
	}
	if ev.Op != event.OpUpdate {
		t.Errorf("Op = %v", ev.Op)
	}
	if ev.PK != 7 {
		t.Errorf("PK = %d", ev.PK)
	}
}

func TestConvertPayload_Delete(t *testing.T) {
	payload := &pb.Payload{
		Op:     pb.Op_DELETE,
		Schema: &pb.Schema{Name: "ORDERS"},
		Before: []*pb.Value{
			{Name: "ID", Datum: &pb.Value_ValueInt{ValueInt: 3}},
		},
	}

	ev, err := olr.ConvertPayload(payload, 300)
	if err != nil {
		t.Fatalf("ConvertPayload() error: %v", err)
	}
	if ev.Op != event.OpDelete {
		t.Errorf("Op = %v", ev.Op)
	}
	if ev.PK != 3 {
		t.Errorf("PK = %d", ev.PK)
	}
}

func TestConvertPayload_SkipBeginCommit(t *testing.T) {
	for _, op := range []pb.Op{pb.Op_BEGIN, pb.Op_COMMIT, pb.Op_DDL, pb.Op_CHKPT} {
		payload := &pb.Payload{Op: op}
		_, err := olr.ConvertPayload(payload, 100)
		if err != olr.ErrSkipEvent {
			t.Errorf("Op %v should return ErrSkipEvent, got %v", op, err)
		}
	}
}
