package wsstream

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestUpResultWaitsForStop(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(HandleWS))
	defer server.Close()

	url := "ws" + strings.TrimPrefix(server.URL, "http") +
		"/ws/test?mode=tcp&dir=up&streams=1&duration=30&packet_len=1024"
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer conn.Close()

	var start ControlMessage
	if err := conn.ReadJSON(&start); err != nil {
		t.Fatalf("read start: %v", err)
	}
	if start.Type != "start" {
		t.Fatalf("first message type = %q, want start", start.Type)
	}

	wantBytes := 0
	for _, size := range []int{1024, 333, 17} {
		if err := conn.WriteMessage(websocket.BinaryMessage, make([]byte, size)); err != nil {
			t.Fatalf("write %d-byte payload: %v", size, err)
		}
		wantBytes += size
	}
	if err := conn.WriteJSON(map[string]string{"type": "stop"}); err != nil {
		t.Fatalf("write stop: %v", err)
	}

	if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	for {
		messageType, data, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("read result after stop: %v", err)
		}
		if messageType != websocket.TextMessage {
			continue
		}
		var message ControlMessage
		if err := json.Unmarshal(data, &message); err != nil {
			t.Fatalf("decode control message: %v", err)
		}
		if message.Type != "result" {
			continue
		}
		if message.Result == nil {
			t.Fatal("result message has nil stats")
		}
		if got := message.Result.TotalBytes; got != uint64(wantBytes) {
			t.Fatalf("result bytes = %d, want %d", got, wantBytes)
		}
		return
	}
}

func TestDownResultFollowsAllBinaryFrames(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(HandleWS))
	defer server.Close()

	url := "ws" + strings.TrimPrefix(server.URL, "http") +
		"/ws/test?mode=tcp&dir=down&streams=1&duration=0.05&packet_len=4096"
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer conn.Close()
	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}

	var received uint64
	for {
		messageType, data, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("read down stream: %v", err)
		}
		if messageType == websocket.BinaryMessage {
			received += uint64(len(data))
			continue
		}
		if messageType != websocket.TextMessage {
			continue
		}
		var message ControlMessage
		if err := json.Unmarshal(data, &message); err != nil {
			t.Fatalf("decode control message: %v", err)
		}
		if message.Type != "result" {
			continue
		}
		if message.Result == nil {
			t.Fatal("result message has nil stats")
		}
		if received == 0 {
			t.Fatal("result arrived without any binary payload")
		}
		if message.Result.TotalBytes != received {
			t.Fatalf("received %d bytes before result, server reports %d", received, message.Result.TotalBytes)
		}
		return
	}
}
