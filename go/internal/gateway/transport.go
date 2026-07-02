package gateway

import (
	"context"
	"errors"

	"github.com/coder/websocket"
)

// frameConn is the transport a gateway connection reads and writes protobuf frames over. Production
// wraps a WebSocket (wsFrameConn); the DST harness uses an in-memory pipe so the connection's
// goroutines block only on channels and timers — operations a testing/synctest bubble treats as
// "durably blocked" and can drive with a fake clock, which a real socket's network I/O is not.
type frameConn interface {
	// ReadFrame blocks for the next inbound binary frame's payload. A non-binary frame is reported as
	// errNonBinaryFrame — the read loop answers with a protocol Error and continues; any other error
	// is terminal (peer gone or context cancelled).
	ReadFrame(ctx context.Context) ([]byte, error)
	// WriteFrame writes one binary frame.
	WriteFrame(ctx context.Context, data []byte) error
	// Ping sends a transport-level keepalive and waits for the pong (or ctx timeout).
	Ping(ctx context.Context) error
	// Close closes the transport.
	Close() error
}

// errNonBinaryFrame reports a received frame that wasn't binary — a protocol error the read loop
// answers with an Error frame rather than tearing the connection down.
var errNonBinaryFrame = errors.New("gateway: non-binary frame")

// wsFrameConn adapts a coder/websocket connection to frameConn — the production transport.
type wsFrameConn struct{ ws *websocket.Conn }

func newWSFrameConn(ws *websocket.Conn) *wsFrameConn {
	ws.SetReadLimit(maxFrameBytes)
	return &wsFrameConn{ws: ws}
}

func (c *wsFrameConn) ReadFrame(ctx context.Context) ([]byte, error) {
	typ, data, err := c.ws.Read(ctx)
	if err != nil {
		return nil, err
	}
	if typ != websocket.MessageBinary {
		return nil, errNonBinaryFrame
	}
	return data, nil
}

func (c *wsFrameConn) WriteFrame(ctx context.Context, data []byte) error {
	return c.ws.Write(ctx, websocket.MessageBinary, data)
}

func (c *wsFrameConn) Ping(ctx context.Context) error { return c.ws.Ping(ctx) }

func (c *wsFrameConn) Close() error { return c.ws.Close(websocket.StatusNormalClosure, "") }
