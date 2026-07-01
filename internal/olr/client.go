package olr

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"

	"github.com/null-ptr-exception/dblog-cdc/internal/event"
	pb "github.com/null-ptr-exception/dblog-cdc/pb"
	"google.golang.org/protobuf/proto"
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
	addr   string
	dbName string
	tables map[string]bool

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

func sendMsg(conn net.Conn, msg proto.Message) error {
	data, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	if err := binary.Write(conn, binary.LittleEndian, uint32(len(data))); err != nil {
		return fmt.Errorf("write length: %w", err)
	}
	if _, err := conn.Write(data); err != nil {
		return fmt.Errorf("write payload: %w", err)
	}
	return nil
}

func recvMsg(conn net.Conn, msg proto.Message) error {
	var length uint32
	if err := binary.Read(conn, binary.LittleEndian, &length); err != nil {
		return fmt.Errorf("read length: %w", err)
	}
	if length == 0xFFFFFFFF {
		var bigLen uint64
		if err := binary.Read(conn, binary.LittleEndian, &bigLen); err != nil {
			return fmt.Errorf("read big length: %w", err)
		}
		length = uint32(bigLen)
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(conn, buf); err != nil {
		return fmt.Errorf("read payload: %w", err)
	}
	return proto.Unmarshal(buf, msg)
}

func (c *Client) Stream(ctx context.Context, startSCN uint64, events chan<- event.Event) error {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", c.addr)
	if err != nil {
		return fmt.Errorf("dial %s: %w", c.addr, err)
	}
	defer conn.Close()

	go func() {
		<-ctx.Done()
		conn.Close()
	}()

	infoReq := &pb.RedoRequest{
		Code:         pb.RequestCode_INFO,
		DatabaseName: c.dbName,
	}
	if err := sendMsg(conn, infoReq); err != nil {
		return fmt.Errorf("send info: %w", err)
	}

	infoResp := &pb.RedoResponse{}
	if err := recvMsg(conn, infoResp); err != nil {
		return fmt.Errorf("recv info: %w", err)
	}
	slog.Info("OLR info response", "code", infoResp.Code, "scn", infoResp.GetScn())

	if infoResp.Code != pb.ResponseCode_REPLICATE {
		startReq := &pb.RedoRequest{
			DatabaseName: c.dbName,
		}
		if startSCN > 0 {
			startReq.Code = pb.RequestCode_CONTINUE
			startReq.CScn = &startSCN
			cidx := uint64(0)
			startReq.CIdx = &cidx
		} else {
			startReq.Code = pb.RequestCode_START
			startReq.TmVal = &pb.RedoRequest_Scn{Scn: 0}
		}

		if err := sendMsg(conn, startReq); err != nil {
			return fmt.Errorf("send start: %w", err)
		}

		startResp := &pb.RedoResponse{}
		if err := recvMsg(conn, startResp); err != nil {
			return fmt.Errorf("recv start: %w", err)
		}
		slog.Info("OLR start response", "code", startResp.Code)

		if startResp.Code != pb.ResponseCode_REPLICATE {
			return fmt.Errorf("unexpected start response: %s", startResp.Code)
		}
	} else {
		slog.Info("OLR already streaming, skipping START")
	}

	for {
		resp := &pb.RedoResponse{}
		if err := recvMsg(conn, resp); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
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
