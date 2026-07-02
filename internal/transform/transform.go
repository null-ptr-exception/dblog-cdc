package transform

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/null-ptr-exception/dblog-cdc/internal/event"
)

type ColumnType struct {
	DataType  string
	Precision int
	Scale     int
}

type TypeMap map[string]map[string]ColumnType // table → column → type

func LoadTypeMap(ctx context.Context, db *sql.DB, tables []string) (TypeMap, error) {
	tm := make(TypeMap, len(tables))
	for _, table := range tables {
		cols, err := queryColumnTypes(ctx, db, table)
		if err != nil {
			return nil, fmt.Errorf("load types for %s: %w", table, err)
		}
		tm[table] = cols
	}
	return tm, nil
}

func queryColumnTypes(ctx context.Context, db *sql.DB, table string) (map[string]ColumnType, error) {
	rows, err := db.QueryContext(ctx,
		`SELECT COLUMN_NAME, DATA_TYPE, NVL(DATA_PRECISION, 0), NVL(DATA_SCALE, 0)
		 FROM ALL_TAB_COLUMNS WHERE TABLE_NAME = :1 ORDER BY COLUMN_ID`, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols := make(map[string]ColumnType)
	for rows.Next() {
		var name, dataType string
		var precision, scale int
		if err := rows.Scan(&name, &dataType, &precision, &scale); err != nil {
			return nil, err
		}
		cols[name] = ColumnType{DataType: dataType, Precision: precision, Scale: scale}
	}
	return cols, rows.Err()
}

type Transformer struct {
	types TypeMap
}

func New(types TypeMap) *Transformer {
	return &Transformer{types: types}
}

func (t *Transformer) TransformEvent(ev *event.Event) {
	tableTypes, ok := t.types[ev.Table]
	if !ok {
		return
	}
	for col, val := range ev.Columns {
		if val == nil {
			continue
		}
		ct, ok := tableTypes[col]
		if !ok {
			continue
		}
		ev.Columns[col] = convertValue(val, ct)
	}
}

func convertValue(val any, ct ColumnType) any {
	dt := strings.ToUpper(ct.DataType)

	switch {
	case dt == "RAW":
		return convertRaw(val)

	case strings.Contains(dt, "TIMESTAMP") && strings.Contains(dt, "TIME ZONE"):
		return convertTimestampTZ(val)

	case strings.HasPrefix(dt, "INTERVAL"):
		return convertInterval(val)

	case dt == "NUMBER" || dt == "FLOAT":
		return convertNumber(val, ct)

	case dt == "BINARY_FLOAT":
		return convertBinaryFloat(val)

	default:
		return val
	}
}

func convertRaw(val any) any {
	s, ok := val.(string)
	if !ok {
		return val
	}
	b, err := hex.DecodeString(s)
	if err != nil {
		return val
	}
	return b
}

func convertTimestampTZ(val any) any {
	s, ok := val.(string)
	if !ok {
		return val
	}
	// OLR format: "epoch_nanos,+HH:MM" or "epoch_nanos,-HH:MM"
	parts := strings.SplitN(s, ",", 2)
	if len(parts) != 2 {
		return val
	}
	epochNanos, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return val
	}
	tzStr := strings.TrimSpace(parts[1])

	loc, err := parseTZOffset(tzStr)
	if err != nil {
		return time.Unix(0, epochNanos).UTC()
	}
	return time.Unix(0, epochNanos).In(loc)
}

func parseTZOffset(s string) (*time.Location, error) {
	if s == "" || s == "UTC" || s == "+00:00" {
		return time.UTC, nil
	}
	sign := 1
	if s[0] == '-' {
		sign = -1
		s = s[1:]
	} else if s[0] == '+' {
		s = s[1:]
	}
	parts := strings.SplitN(s, ":", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid tz offset: %s", s)
	}
	hours, err := strconv.Atoi(parts[0])
	if err != nil {
		return nil, err
	}
	mins, err := strconv.Atoi(parts[1])
	if err != nil {
		return nil, err
	}
	offset := sign * (hours*3600 + mins*60)
	return time.FixedZone(fmt.Sprintf("UTC%+d", offset/3600), offset), nil
}

func convertInterval(val any) any {
	// OLR sends interval as microseconds (float64 or json.Number)
	var us int64
	switch v := val.(type) {
	case float64:
		us = int64(v)
	case json.Number:
		n, err := v.Int64()
		if err != nil {
			return val
		}
		us = n
	default:
		return val
	}

	// OLR value appears to be microseconds
	// 5 days 3:30:15.123456 = 444615.123456s = 444615123456µs
	// But OLR sends 444615123456000 which is 1000x → nanoseconds
	// Detect: if value > reasonable microsecond range, treat as nanoseconds
	if us > 365*24*3600*1e6 {
		us = us / 1000 // nanoseconds → microseconds
	}

	negative := us < 0
	if negative {
		us = -us
	}

	totalSec := us / 1_000_000
	fracUs := us % 1_000_000
	days := totalSec / 86400
	rem := totalSec % 86400
	hours := rem / 3600
	rem %= 3600
	mins := rem / 60
	secs := rem % 60

	var b strings.Builder
	if negative {
		b.WriteString("-")
	}
	if days > 0 {
		fmt.Fprintf(&b, "%d days ", days)
	}
	fmt.Fprintf(&b, "%02d:%02d:%02d", hours, mins, secs)
	if fracUs > 0 {
		fmt.Fprintf(&b, ".%06d", fracUs)
	}
	return b.String()
}

func convertNumber(val any, ct ColumnType) any {
	// json.Number preserves precision — pass through as string for pgx
	if n, ok := val.(json.Number); ok {
		if ct.Scale == 0 && ct.Precision > 0 && ct.Precision <= 18 {
			if v, err := n.Int64(); err == nil {
				return v
			}
		}
		return n
	}
	return val
}

func convertBinaryFloat(val any) any {
	// OLR promotes BINARY_FLOAT (32-bit) to float64, causing artifacts like
	// 3.14 → 3.1400001. Convert back to float32 to round-trip correctly.
	switch v := val.(type) {
	case float64:
		return float32(v)
	case json.Number:
		f, err := v.Float64()
		if err != nil {
			return val
		}
		return float32(f)
	default:
		return val
	}
}
