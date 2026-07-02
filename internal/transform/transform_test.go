package transform

import (
	"encoding/json"
	"fmt"
	"math"
	"testing"
	"time"
)

func TestConvertRaw(t *testing.T) {
	got := convertRaw("deadbeef")
	b, ok := got.([]byte)
	if !ok {
		t.Fatalf("expected []byte, got %T", got)
	}
	if fmt.Sprintf("%x", b) != "deadbeef" {
		t.Errorf("got %x, want deadbeef", b)
	}
}

func TestConvertTimestampTZ(t *testing.T) {
	// 2024-03-15 14:30:00.123456 +05:30
	// epoch nanos at UTC: 2024-03-15 09:00:00.123456 UTC
	got := convertTimestampTZ("1710493200123456000,+05:30")
	ts, ok := got.(time.Time)
	if !ok {
		t.Fatalf("expected time.Time, got %T: %v", got, got)
	}
	wantUTC := time.Date(2024, 3, 15, 9, 0, 0, 123456000, time.UTC)
	if ts.UTC().Sub(wantUTC).Abs() > time.Microsecond {
		t.Errorf("got %v, want %v", ts.UTC(), wantUTC)
	}
	_, offset := ts.Zone()
	if offset != 5*3600+30*60 {
		t.Errorf("tz offset = %d, want %d", offset, 5*3600+30*60)
	}
}

func TestConvertInterval(t *testing.T) {
	// 5 days 3:30:15.123456 = 444615.123456s
	// OLR sends as nanoseconds: 444615123456000
	got := convertInterval(float64(444615123456000))
	s, ok := got.(string)
	if !ok {
		t.Fatalf("expected string, got %T: %v", got, got)
	}
	if s != "5 days 03:30:15.123456" {
		t.Errorf("got %q, want %q", s, "5 days 03:30:15.123456")
	}
}

func TestConvertInterval_JsonNumber(t *testing.T) {
	got := convertInterval(json.Number("444615123456000"))
	s, ok := got.(string)
	if !ok {
		t.Fatalf("expected string, got %T: %v", got, got)
	}
	if s != "5 days 03:30:15.123456" {
		t.Errorf("got %q, want %q", s, "5 days 03:30:15.123456")
	}
}

func TestConvertNumber_JsonNumber(t *testing.T) {
	ct := ColumnType{DataType: "NUMBER", Precision: 19, Scale: 0}
	n := json.Number("1234567890123456789")
	got := convertNumber(n, ct)
	// Precision 19 > 18, so should stay as json.Number
	if _, ok := got.(json.Number); !ok {
		t.Errorf("expected json.Number, got %T: %v", got, got)
	}
}

func TestConvertNumber_SmallInt(t *testing.T) {
	ct := ColumnType{DataType: "NUMBER", Precision: 10, Scale: 0}
	n := json.Number("42")
	got := convertNumber(n, ct)
	v, ok := got.(int64)
	if !ok {
		t.Fatalf("expected int64, got %T: %v", got, got)
	}
	if v != 42 {
		t.Errorf("got %d, want 42", v)
	}
}

func TestConvertBinaryFloat(t *testing.T) {
	got := convertBinaryFloat(float64(3.1400001))
	f, ok := got.(float32)
	if !ok {
		t.Fatalf("expected float32, got %T: %v", got, got)
	}
	if math.Abs(float64(f)-3.14) > 0.0001 {
		t.Errorf("got %v, want ~3.14", f)
	}
}
