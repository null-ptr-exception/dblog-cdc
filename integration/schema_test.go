//go:build integration

package integration_test

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/null-ptr-exception/dblog-cdc/internal/event"
	"github.com/null-ptr-exception/dblog-cdc/internal/transform"
	"github.com/null-ptr-exception/dblog-cdc/internal/writer"
)

// TestSchema_OLRTypeCompatibility verifies that values as delivered by OLR's
// JSON format (after json.Unmarshal in Go) can be correctly written to
// YugabyteDB. OLR sends all numbers as JSON numbers (→ float64 in Go),
// timestamps as strings, and uses custom formats for some types.
//
// Each subtest simulates OLR's JSON output for a specific Oracle type and
// attempts to write it to YB. Tests that FAIL indicate types that need
// schema-aware mapping.
func TestSchema_OLRTypeCompatibility(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	env := setupEnv(t, ctx)
	ybWriter := writer.NewPgWriter(env.ybPool, "ID")

	type typeCase struct {
		name    string
		ybDDL   string
		ybTable string
		// columns as OLR would deliver them (post json.Unmarshal with UseNumber)
		olrColumns map[string]any
		// Oracle column types for the transformer
		oracleTypes map[string]transform.ColumnType
		verify      func(t *testing.T)
	}

	cases := []typeCase{
		{
			name:    "BigInt_Precision",
			ybDDL:   "CREATE TABLE IF NOT EXISTS OLR_BIGINT (ID BIGINT PRIMARY KEY, VAL BIGINT)",
			ybTable: "OLR_BIGINT",
			// With UseNumber, OLR sends NUMBER(19,0) as json.Number preserving digits
			olrColumns: map[string]any{
				"ID":  json.Number("9001"),
				"VAL": json.Number("1234567890123456789"),
			},
			oracleTypes: map[string]transform.ColumnType{
				"ID":  {DataType: "NUMBER", Precision: 10, Scale: 0},
				"VAL": {DataType: "NUMBER", Precision: 19, Scale: 0},
			},
			verify: func(t *testing.T) {
				var val int64
				err := env.ybPool.QueryRow(ctx, "SELECT VAL FROM OLR_BIGINT WHERE ID = $1", 9001).Scan(&val)
				if err != nil {
					t.Fatalf("scan: %v", err)
				}
				want := int64(1234567890123456789)
				if val != want {
					t.Errorf("precision loss: got %d, want %d (diff=%d)",
						val, want, val-want)
				}
			},
		},
		{
			name:    "PreciseNumeric_Precision",
			ybDDL:   "CREATE TABLE IF NOT EXISTS OLR_NUMERIC (ID BIGINT PRIMARY KEY, VAL NUMERIC(38,10))",
			ybTable: "OLR_NUMERIC",
			// With UseNumber, full precision preserved as json.Number string
			olrColumns: map[string]any{
				"ID":  json.Number("9001"),
				"VAL": json.Number("12345678901234567890.1234567890"),
			},
			oracleTypes: map[string]transform.ColumnType{
				"ID":  {DataType: "NUMBER", Precision: 10, Scale: 0},
				"VAL": {DataType: "NUMBER", Precision: 38, Scale: 10},
			},
			verify: func(t *testing.T) {
				var val string
				err := env.ybPool.QueryRow(ctx, "SELECT VAL::TEXT FROM OLR_NUMERIC WHERE ID = $1", 9001).Scan(&val)
				if err != nil {
					t.Fatalf("scan: %v", err)
				}
				want := "12345678901234567890.1234567890"
				if val != want {
					t.Errorf("precision loss: got %s, want %s", val, want)
				}
			},
		},
		{
			name:    "BinaryFloat_Rounding",
			ybDDL:   "CREATE TABLE IF NOT EXISTS OLR_BFLOAT (ID BIGINT PRIMARY KEY, VAL REAL)",
			ybTable: "OLR_BFLOAT",
			// OLR sends BINARY_FLOAT 3.14 as 3.1400001 (single→double promotion)
			// Transformer converts to float32 to remove artifact
			olrColumns: map[string]any{
				"ID":  json.Number("9001"),
				"VAL": json.Number("3.1400001"),
			},
			oracleTypes: map[string]transform.ColumnType{
				"ID":  {DataType: "NUMBER", Precision: 10, Scale: 0},
				"VAL": {DataType: "BINARY_FLOAT"},
			},
			verify: func(t *testing.T) {
				var val float32
				err := env.ybPool.QueryRow(ctx, "SELECT VAL FROM OLR_BFLOAT WHERE ID = $1", 9001).Scan(&val)
				if err != nil {
					t.Fatalf("scan: %v", err)
				}
				if math.Abs(float64(val)-3.14) > 0.0001 {
					t.Errorf("rounding error: got %.10f, want ~3.14", val)
				}
			},
		},
		{
			name:    "TimestampTZ_OLRFormat",
			ybDDL:   "CREATE TABLE IF NOT EXISTS OLR_TSTZ (ID BIGINT PRIMARY KEY, VAL TIMESTAMPTZ)",
			ybTable: "OLR_TSTZ",
			// OLR sends TIMESTAMP WITH TIME ZONE as "epoch_nanos,+offset"
			// Transformer converts to time.Time
			olrColumns: map[string]any{
				"ID":  json.Number("9001"),
				"VAL": "1710493200123456000,+05:30",
			},
			oracleTypes: map[string]transform.ColumnType{
				"ID":  {DataType: "NUMBER", Precision: 10, Scale: 0},
				"VAL": {DataType: "TIMESTAMP(6) WITH TIME ZONE"},
			},
			verify: func(t *testing.T) {
				var val time.Time
				err := env.ybPool.QueryRow(ctx, "SELECT VAL FROM OLR_TSTZ WHERE ID = $1", 9001).Scan(&val)
				if err != nil {
					t.Fatalf("scan: %v", err)
				}
				wantUTC := time.Date(2024, 3, 15, 9, 0, 0, 123456000, time.UTC)
				if val.UTC().Sub(wantUTC).Abs() > time.Second {
					t.Errorf("wrong value: got %v, want %v", val.UTC(), wantUTC)
				}
			},
		},
		{
			name:    "Raw_HexString",
			ybDDL:   "CREATE TABLE IF NOT EXISTS OLR_RAW (ID BIGINT PRIMARY KEY, VAL BYTEA)",
			ybTable: "OLR_RAW",
			// OLR sends RAW as lowercase hex string without \x prefix
			// Transformer hex-decodes to []byte
			olrColumns: map[string]any{
				"ID":  json.Number("9001"),
				"VAL": "deadbeef",
			},
			oracleTypes: map[string]transform.ColumnType{
				"ID":  {DataType: "NUMBER", Precision: 10, Scale: 0},
				"VAL": {DataType: "RAW"},
			},
			verify: func(t *testing.T) {
				var val []byte
				err := env.ybPool.QueryRow(ctx, "SELECT VAL FROM OLR_RAW WHERE ID = $1", 9001).Scan(&val)
				if err != nil {
					t.Fatalf("scan: %v", err)
				}
				want, _ := hex.DecodeString("deadbeef")
				if fmt.Sprintf("%x", val) != fmt.Sprintf("%x", want) {
					t.Errorf("got %x, want %x", val, want)
				}
			},
		},
		{
			name:    "IntervalDS_Microseconds",
			ybDDL:   "CREATE TABLE IF NOT EXISTS OLR_INTERVAL (ID BIGINT PRIMARY KEY, VAL INTERVAL)",
			ybTable: "OLR_INTERVAL",
			// OLR sends INTERVAL DAY TO SECOND as nanoseconds
			// Transformer converts to PG interval string
			olrColumns: map[string]any{
				"ID":  json.Number("9001"),
				"VAL": json.Number("444615123456000"),
			},
			oracleTypes: map[string]transform.ColumnType{
				"ID":  {DataType: "NUMBER", Precision: 10, Scale: 0},
				"VAL": {DataType: "INTERVAL DAY(3) TO SECOND(6)"},
			},
			verify: func(t *testing.T) {
				var val string
				err := env.ybPool.QueryRow(ctx, "SELECT VAL::TEXT FROM OLR_INTERVAL WHERE ID = $1", 9001).Scan(&val)
				if err != nil {
					t.Fatalf("scan: %v", err)
				}
				t.Logf("INTERVAL in YB: %s (want: 5 days 03:30:15.123456)", val)
			},
		},
		{
			name:    "Timestamp_StringFormat",
			ybDDL:   "CREATE TABLE IF NOT EXISTS OLR_TS (ID BIGINT PRIMARY KEY, VAL TIMESTAMP)",
			ybTable: "OLR_TS",
			// OLR sends TIMESTAMP as "2024-03-15 14:30:00.123456000" (9 fractional digits)
			olrColumns: map[string]any{
				"ID":  json.Number("9001"),
				"VAL": "2024-03-15 14:30:00.123456000",
			},
			oracleTypes: map[string]transform.ColumnType{
				"ID":  {DataType: "NUMBER", Precision: 10, Scale: 0},
				"VAL": {DataType: "TIMESTAMP(6)"},
			},
			verify: func(t *testing.T) {
				var val time.Time
				err := env.ybPool.QueryRow(ctx, "SELECT VAL FROM OLR_TS WHERE ID = $1", 9001).Scan(&val)
				if err != nil {
					t.Fatalf("scan: %v", err)
				}
				want := time.Date(2024, 3, 15, 14, 30, 0, 123456000, time.UTC)
				if val.Sub(want).Abs() > time.Microsecond {
					t.Errorf("wrong value: got %v, want %v", val, want)
				}
			},
		},
		{
			name:    "OracleDate_StringFormat",
			ybDDL:   "CREATE TABLE IF NOT EXISTS OLR_DATE (ID BIGINT PRIMARY KEY, VAL TIMESTAMP)",
			ybTable: "OLR_DATE",
			// OLR sends Oracle DATE as "2024-03-15 14:30:00.000000000"
			olrColumns: map[string]any{
				"ID":  json.Number("9001"),
				"VAL": "2024-03-15 14:30:00.000000000",
			},
			oracleTypes: map[string]transform.ColumnType{
				"ID":  {DataType: "NUMBER", Precision: 10, Scale: 0},
				"VAL": {DataType: "DATE"},
			},
			verify: func(t *testing.T) {
				var val time.Time
				err := env.ybPool.QueryRow(ctx, "SELECT VAL FROM OLR_DATE WHERE ID = $1", 9001).Scan(&val)
				if err != nil {
					t.Fatalf("scan: %v", err)
				}
				want := time.Date(2024, 3, 15, 14, 30, 0, 0, time.UTC)
				if val.Sub(want).Abs() > time.Second {
					t.Errorf("wrong value: got %v, want %v", val, want)
				}
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			// Create YB table
			if _, err := env.ybPool.Exec(ctx, tc.ybDDL); err != nil {
				t.Fatalf("create yb table: %v", err)
			}
			t.Cleanup(func() {
				cleanCtx, c := context.WithTimeout(context.Background(), 5*time.Second)
				defer c()
				env.ybPool.Exec(cleanCtx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tc.ybTable))
			})

			// Apply transformer (schema-aware type mapping)
			typeMap := transform.TypeMap{tc.ybTable: tc.oracleTypes}
			xformer := transform.New(typeMap)

			ev := event.Event{
				Table:   tc.ybTable,
				Op:      event.OpInsert,
				PK:      fmt.Sprint(tc.olrColumns["ID"]),
				Columns: tc.olrColumns,
			}
			xformer.TransformEvent(&ev)

			// Write to YB
			err := ybWriter.WriteBatch(ctx, []event.Event{ev})
			if err != nil {
				t.Fatalf("write to YB failed (OLR value incompatible with PG type): %v", err)
			}

			tc.verify(t)
		})
	}
}
