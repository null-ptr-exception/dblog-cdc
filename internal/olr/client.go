package olr

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"

	"github.com/null-ptr-exception/dblog-cdc/internal/event"
	pb "github.com/null-ptr-exception/dblog-cdc/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var ErrSkipEvent = errors.New("skip non-DML event")

func ConvertPayload(p *pb.Payload, scn uint64) (event.Event, error) {
	switch p.Op {
	case pb.Op_INSERT, pb.Op_UPDATE, pb.Op_DELETE:
	default:
		return event.Event{}, ErrSkipEvent
	}

	var op event.OpType
	switch p.Op {
	case pb.Op_INSERT:
		op = event.OpInsert
	case pb.Op_UPDATE:
		op = event.OpUpdate
	case pb.Op_DELETE:
		op = event.OpDelete
	}

	tableName := ""
	if p.Schema != nil {
		tableName = p.Schema.Name
	}

	values := p.After
	if op == event.OpDelete {
		values = p.Before
	}

	var pk int64
	var pkFound bool
	columns := make(map[string]any)

	for _, v := range values {
		val := extractValue(v)
		columns[v.Name] = val

		if !pkFound {
			if intVal, ok := v.Datum.(*pb.Value_ValueInt); ok {
				pk = intVal.ValueInt
				pkFound = true
			}
		}
	}

	if !pkFound {
		return event.Event{}, fmt.Errorf("no integer PK found in event for table %s", tableName)
	}

	return event.Event{
		Table:   tableName,
		Op:      op,
		SCN:     scn,
		PK:      pk,
		Columns: columns,
	}, nil
}

func extractValue(v *pb.Value) any {
	switch d := v.Datum.(type) {
	case *pb.Value_ValueInt:
		return d.ValueInt
	case *pb.Value_ValueFloat:
		return d.ValueFloat
	case *pb.Value_ValueDouble:
		return d.ValueDouble
	case *pb.Value_ValueString:
		return d.ValueString
	case *pb.Value_ValueBytes:
		return d.ValueBytes
	default:
		return nil
	}
}

type Client struct {
	addr     string
	dbName   string
	conn     *grpc.ClientConn
	tables   map[string]bool

	mu       sync.Mutex
	lastSCN  uint64
	lastCSCN uint64
	lastCIdx uint64
}

func NewClient(host string, port int, dbName string, tables []string) *Client {
	tableSet := make(map[string]bool, len(tables))
	for _, t := range tables {
		tableSet[t] = true
	}
	return &Client{
		addr:   fmt.Sprintf("%s:%d", host, port),
		dbName: dbName,
		tables: tableSet,
	}
}

func (c *Client) LastSCN() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastSCN
}

func (c *Client) Stream(ctx context.Context, startSCN uint64, events chan<- event.Event) error {
	var err error
	c.conn, err = grpc.NewClient(c.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("grpc dial: %w", err)
	}
	defer c.conn.Close()

	client := pb.NewOpenLogReplicatorClient(c.conn)
	stream, err := client.Redo(ctx)
	if err != nil {
		return fmt.Errorf("start redo stream: %w", err)
	}

	req := &pb.RedoRequest{
		DatabaseName: c.dbName,
	}
	if startSCN > 0 {
		req.Code = pb.RequestCode_CONTINUE
		cscn := startSCN
		cidx := uint64(0)
		req.CScn = &cscn
		req.CIdx = &cidx
	} else {
		req.Code = pb.RequestCode_START
	}

	if err := stream.Send(req); err != nil {
		return fmt.Errorf("send start: %w", err)
	}

	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("recv: %w", err)
		}

		c.mu.Lock()
		if scn := resp.GetScn(); scn > 0 {
			c.lastSCN = scn
		}
		c.lastCSCN = resp.CScn
		c.lastCIdx = resp.CIdx
		c.mu.Unlock()

		scn := resp.GetScn()

		for _, p := range resp.Payload {
			ev, err := ConvertPayload(p, scn)
			if errors.Is(err, ErrSkipEvent) {
				continue
			}
			if err != nil {
				slog.Warn("skip event", "error", err)
				continue
			}

			if !c.tables[ev.Table] {
				continue
			}

			select {
			case events <- ev:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}

func (c *Client) Confirm(stream pb.OpenLogReplicator_RedoClient) error {
	c.mu.Lock()
	cscn := c.lastCSCN
	cidx := c.lastCIdx
	c.mu.Unlock()

	return stream.Send(&pb.RedoRequest{
		Code: pb.RequestCode_CONFIRM,
		CScn: &cscn,
		CIdx: &cidx,
	})
}
