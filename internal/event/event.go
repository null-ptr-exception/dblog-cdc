package event

import (
	"fmt"
	"strings"
)

type OpType int

const (
	OpInsert OpType = iota
	OpUpdate
	OpDelete
)

func (o OpType) String() string {
	switch o {
	case OpInsert:
		return "INSERT"
	case OpUpdate:
		return "UPDATE"
	case OpDelete:
		return "DELETE"
	default:
		return "UNKNOWN"
	}
}

type Event struct {
	Table   string
	Op      OpType
	SCN     uint64
	PK      []string
	Columns map[string]any
}

type ChunkRow struct {
	PK      []string
	Columns map[string]any
}

type ChunkResult struct {
	Table    string
	SCN      uint64
	Rows     map[string]ChunkRow
	LastPK   []string
	Complete bool
}

func EncodePK(pk []string) string {
	if len(pk) == 1 {
		return pk[0]
	}
	parts := make([]string, len(pk))
	for i, v := range pk {
		parts[i] = fmt.Sprintf("%d:%s", len(v), v)
	}
	return strings.Join(parts, ",")
}
