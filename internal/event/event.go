package event

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
	PK      string
	Columns map[string]any
}

type ChunkResult struct {
	Table    string
	SCN      uint64
	Rows     map[string]map[string]any
	LastPK   string
	Complete bool
}
