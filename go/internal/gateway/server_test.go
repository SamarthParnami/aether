package gateway_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"google.golang.org/protobuf/proto"

	aetherv1 "github.com/SamarthParnami/aether/go/gen/aether/v1"
	"github.com/SamarthParnami/aether/go/internal/coord"
	"github.com/SamarthParnami/aether/go/internal/gateway"
)

const authHeader = "X-Aether-Principal"

func newTestServer() *httptest.Server {
	return httptest.NewServer(gateway.NewServer(
		gateway.DevAuthenticator{Header: authHeader},
		gateway.NewOwnerLocator(coord.NewMemory()),
	))
}

func wsURL(srv *httptest.Server) string { return "ws" + strings.TrimPrefix(srv.URL, "http") }

// dial connects an authenticated client and returns the conn, failing the test on error. It
// closes the handshake response body itself, so callers don't carry that obligation.
func dial(ctx context.Context, t *testing.T, srv *httptest.Server, principal string) *websocket.Conn {
	t.Helper()
	opts := &websocket.DialOptions{HTTPHeader: http.Header{}}
	opts.HTTPHeader.Set(authHeader, principal)
	ws, resp, err := websocket.Dial(ctx, wsURL(srv), opts)
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	if err != nil {
		t.Fatal(err)
	}
	return ws
}

func writeFrame(ctx context.Context, t *testing.T, ws *websocket.Conn, m *aetherv1.ClientMessage) {
	t.Helper()
	data, err := proto.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if err := ws.Write(ctx, websocket.MessageBinary, data); err != nil {
		t.Fatal(err)
	}
}

func readFrame(ctx context.Context, t *testing.T, ws *websocket.Conn) *aetherv1.ServerMessage {
	t.Helper()
	typ, data, err := ws.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if typ != websocket.MessageBinary {
		t.Fatalf("message type = %v, want binary", typ)
	}
	var m aetherv1.ServerMessage
	if err := proto.Unmarshal(data, &m); err != nil {
		t.Fatal(err)
	}
	return &m
}

// An authenticated client's app-level Ping is answered with a Pong echoing its id.
func TestPingPong(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ws := dial(ctx, t, srv, "user-1")
	defer func() { _ = ws.Close(websocket.StatusNormalClosure, "") }()

	writeFrame(ctx, t, ws, &aetherv1.ClientMessage{
		Body: &aetherv1.ClientMessage_Ping{Ping: &aetherv1.Ping{Id: "p1"}},
	})
	if got := readFrame(ctx, t, ws).GetPong().GetId(); got != "p1" {
		t.Fatalf("pong id = %q, want p1", got)
	}
}

// Room frames aren't wired yet: the skeleton answers them with an UNIMPLEMENTED error rather
// than dropping the connection.
func TestRoomFrameUnimplemented(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ws := dial(ctx, t, srv, "user-1")
	defer func() { _ = ws.Close(websocket.StatusNormalClosure, "") }()

	writeFrame(ctx, t, ws, &aetherv1.ClientMessage{
		Body: &aetherv1.ClientMessage_Commit{Commit: &aetherv1.Commit{RoomId: "r", ClientSeq: 1}},
	})
	if got := readFrame(ctx, t, ws).GetError().GetCode(); got != "UNIMPLEMENTED" {
		t.Fatalf("error code = %q, want UNIMPLEMENTED", got)
	}
}

// A handshake without a valid principal is rejected at the HTTP layer (no upgrade).
func TestAuthRejected(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ws, resp, err := websocket.Dial(ctx, wsURL(srv), nil) // no principal header
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	if err == nil {
		_ = ws.Close(websocket.StatusNormalClosure, "")
		t.Fatal("dial succeeded without authentication")
	}
	if resp == nil || resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %v, want 401", resp)
	}
}
