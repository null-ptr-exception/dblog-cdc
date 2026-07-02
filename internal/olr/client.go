package olr

import (
	"context"
	"encoding/binary"
	"encoding/json"
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

// jsonMessage is the top-level JSON message from OLR after handshake.
type jsonMessage struct {
	SCN     uint64        `json:"scn"`
	CSCN    uint64        `json:"c_scn"`
	CIdx    uint64        `json:"c_idx"`
	Payload []jsonPayload `json:"payload"`
}

type jsonPayload struct {
	Op     string            `json:"op"`
	Schema jsonSchema        `json:"schema"`
	Before map[string]any    `json:"before"`
	After  map[string]any    `json:"after"`
}

type jsonSchema struct {
	Owner string `json:"owner"`
	Table string `json:"table"`
}

func convertJSONPayload(p jsonPayload, scn uint64, pkCol string) (event.Event, error) {
	var op event.OpType
	switch p.Op {
	case "c":
		op = event.OpInsert
	case "u":
		op = event.OpUpdate
	case "d":
		op = event.OpDelete
	default:
		return event.Event{}, ErrSkipEvent
	}

	columns := p.After
	if op == event.OpDelete {
		columns = p.Before
	}
	if columns == nil {
		return event.Event{}, fmt.Errorf("no column data in %s event for table %s", p.Op, p.Schema.Table)
	}

	pkVal, ok := columns[pkCol]
	if !ok || pkVal == nil {
		return event.Event{}, fmt.Errorf("PK column %q not found in event for table %s", pkCol, p.Schema.Table)
	}
	pk := fmt.Sprint(pkVal)

	return event.Event{
		Table:   p.Schema.Table,
		Op:      op,
		SCN:     scn,
		PK:      pk,
		Columns: columns,
	}, nil
}

type Client struct {
	addr      string
	dbName    string
	tables    map[string]bool
	pkColumns map[string]string

	mu        sync.Mutex
	lastSCN   uint64
	lastCSCN  uint64
	lastCIdx  uint64
	streaming chan struct{}
}

func NewClient(host string, port int, dbName string, tables []string, pkColumns map[string]string) *Client {
	tableSet := make(map[string]bool, len(tables))
	for _, t := range tables {
		tableSet[t] = true
	}
	return &Client{
		addr:      fmt.Sprintf("%s:%d", host, port),
		dbName:    dbName,
		tables:    tableSet,
		pkColumns: pkColumns,
		streaming: make(chan struct{}),
	}
}

func (c *Client) LastSCN() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastSCN
}

func (c *Client) WaitStreaming(ctx context.Context) error {
	select {
	case <-c.streaming:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
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

func recvProto(conn net.Conn, msg proto.Message) error {
	buf, err := recvRaw(conn)
	if err != nil {
		return err
	}
	return proto.Unmarshal(buf, msg)
}

func recvRaw(conn net.Conn) ([]byte, error) {
	var length uint32
	if err := binary.Read(conn, binary.LittleEndian, &length); err != nil {
		return nil, fmt.Errorf("read length: %w", err)
	}
	if length == 0xFFFFFFFF {
		var bigLen uint64
		if err := binary.Read(conn, binary.LittleEndian, &bigLen); err != nil {
			return nil, fmt.Errorf("read big length: %w", err)
		}
		length = uint32(bigLen)
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(conn, buf); err != nil {
		return nil, fmt.Errorf("read payload: %w", err)
	}
	return buf, nil
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

	// Handshake is always protobuf
	infoReq := &pb.RedoRequest{
		Code:         pb.RequestCode_INFO,
		DatabaseName: c.dbName,
	}
	if err := sendMsg(conn, infoReq); err != nil {
		return fmt.Errorf("send info: %w", err)
	}

	infoResp := &pb.RedoResponse{}
	if err := recvProto(conn, infoResp); err != nil {
		return fmt.Errorf("recv info: %w", err)
	}
	slog.Info("OLR info response", "code", infoResp.Code, "scn", infoResp.GetScn())

	resumeSCN := startSCN
	if resumeSCN == 0 && infoResp.GetScn() > 0 {
		resumeSCN = infoResp.GetScn()
	}

	var req *pb.RedoRequest
	switch infoResp.Code {
	case pb.ResponseCode_REPLICATE:
		req = &pb.RedoRequest{
			Code:         pb.RequestCode_CONTINUE,
			DatabaseName: c.dbName,
			CScn:         &resumeSCN,
			CIdx:         func() *uint64 { v := uint64(0); return &v }(),
		}
	case pb.ResponseCode_READY:
		startFrom := uint64(0xFFFFFFFFFFFFFFFF)
		if startSCN > 0 {
			startFrom = startSCN
		}
		req = &pb.RedoRequest{
			Code:         pb.RequestCode_START,
			DatabaseName: c.dbName,
			TmVal:        &pb.RedoRequest_Scn{Scn: startFrom},
		}
	default:
		return fmt.Errorf("unexpected info response: %s", infoResp.Code)
	}

	if err := sendMsg(conn, req); err != nil {
		return fmt.Errorf("send %s: %w", req.Code, err)
	}

	startResp := &pb.RedoResponse{}
	if err := recvProto(conn, startResp); err != nil {
		return fmt.Errorf("recv %s response: %w", req.Code, err)
	}
	slog.Info("OLR handshake complete", "sent", req.Code, "response", startResp.Code)

	if startResp.Code != pb.ResponseCode_REPLICATE {
		return fmt.Errorf("unexpected response to %s: %s", req.Code, startResp.Code)
	}

	select {
	case <-c.streaming:
	default:
		close(c.streaming)
	}

	// Data messages are JSON
	var msgCount uint64
	const confirmInterval = 1000

	for {
		raw, err := recvRaw(conn)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("recv: %w", err)
		}

		var msg jsonMessage
		if err := json.Unmarshal(raw, &msg); err != nil {
			slog.Warn("skip malformed JSON message", "error", err)
			continue
		}

		c.mu.Lock()
		if msg.SCN > 0 {
			c.lastSCN = msg.SCN
		}
		c.lastCSCN = msg.CSCN
		c.lastCIdx = msg.CIdx
		c.mu.Unlock()

		for _, p := range msg.Payload {
			if !c.tables[p.Schema.Table] {
				continue
			}

			pkCol := c.pkColumns[p.Schema.Table]
			ev, err := convertJSONPayload(p, msg.SCN, pkCol)
			if errors.Is(err, ErrSkipEvent) {
				continue
			}
			if err != nil {
				slog.Warn("skip event", "error", err)
				continue
			}

			select {
			case events <- ev:
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		// CONFIRM is still protobuf
		msgCount++
		if msgCount%confirmInterval == 0 {
			confirm := &pb.RedoRequest{
				Code:         pb.RequestCode_CONFIRM,
				DatabaseName: c.dbName,
				CScn:         &msg.CSCN,
				CIdx:         func() *uint64 { v := msg.CIdx; return &v }(),
			}
			if err := sendMsg(conn, confirm); err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				return fmt.Errorf("send confirm: %w", err)
			}
		}
	}
}
