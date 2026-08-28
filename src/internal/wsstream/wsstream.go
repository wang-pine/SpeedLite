// Package wsstream 实现基于 WebSocket 的 TCP 测速。
// WebSocket 运行在 TCP 之上，其吞吐与原生 TCP 基本一致（仅少量帧头开销），
// 因此前端页面通过 WS 打流即可测量 TCP 上下行带宽。
package wsstream

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"speedTest/internal/engine"
)

// upgrader 允许跨域（测速页面可能与服务器不同源）。
var upgrader = websocket.Upgrader{
	ReadBufferSize:  1 << 20,
	WriteBufferSize: 1 << 20,
	CheckOrigin:     func(r *http.Request) bool { return true },
}

// ControlMessage 服务器 -> 客户端 的控制/采样消息。
type ControlMessage struct {
	Type       string        `json:"type"` // "start" | "sample" | "result" | "error"
	UpMBps     float64       `json:"up_mbps,omitempty"`
	UpMbitps   float64       `json:"up_mbitps,omitempty"`
	DownMBps   float64       `json:"down_mbps,omitempty"`
	DownMbitps float64       `json:"down_mbitps,omitempty"`
	UpBytes    uint64        `json:"up_bytes,omitempty"`
	DownBytes  uint64        `json:"down_bytes,omitempty"`
	Result     *engine.Stats `json:"result,omitempty"`
	Error      string        `json:"error,omitempty"`
}

// parseParams 从 URL query 解析并校验测速参数。
func parseParams(r *http.Request) (*engine.Params, error) {
	q := r.URL.Query()
	p := &engine.Params{
		Mode:      engine.Mode(q.Get("mode")),
		Direction: engine.Direction(q.Get("dir")),
		Streams:   atoi(q.Get("streams")),
		Duration:  atof(q.Get("duration")),
		PacketLen: atoi(q.Get("packet_len")),
	}
	if err := p.Validate(); err != nil {
		return nil, err
	}
	return p, nil
}

func atoi(s string) int {
	n := 0
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0
		}
		n = n*10 + int(c-'0')
	}
	return n
}

func atof(s string) float64 {
	f := 0.0
	dec := 0.1
	inDec := false
	neg := false
	for _, c := range s {
		if c == '-' && !inDec {
			neg = true
			continue
		}
		if c == '.' && !inDec {
			inDec = true
			continue
		}
		if c < '0' || c > '9' {
			return 0
		}
		if inDec {
			f += float64(c-'0') * dec
			dec /= 10
		} else {
			f = f*10 + float64(c-'0')
		}
	}
	if neg {
		return -f
	}
	return f
}

// HandleWS 处理一条 WebSocket 测速连接（一个流）。
func HandleWS(w http.ResponseWriter, r *http.Request) {
	p, err := parseParams(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("ws upgrade failed: %v", err)
		return
	}
	defer conn.Close()

	runStream(conn, p)
}

// runStream 执行单个流的测速逻辑。
//
// 服务器视角：客户端上行 = 服务器"接收"(rx)；客户端下行 = 服务器"发送"(tx)。
//   - down: 服务器持续写二进制帧（下行），txSampler 计发送量。
//   - up:   服务器持续读二进制帧（上行），rxSampler 计接收量。
//   - both: 读、写两个 goroutine 同时进行。
//
// 采样消息每 100ms 推送一次，同时携带 up/down 两个方向的速率，供前端画两条曲线。
func runStream(conn *websocket.Conn, p *engine.Params) {
	// gorilla/websocket 一个连接同一时刻只允许一个 writer，
	// 用 connMu 串行化所有写（二进制数据帧 + JSON 控制帧）。
	var connMu sync.Mutex
	writeJSON := func(msg ControlMessage) error {
		connMu.Lock()
		defer connMu.Unlock()
		return conn.WriteJSON(msg)
	}

	_ = writeJSON(ControlMessage{Type: "start"})

	rx := engine.NewSampler(100 * time.Millisecond) // 服务器接收（客户端上行）
	tx := engine.NewSampler(100 * time.Millisecond) // 服务器发送（客户端下行）
	stopSampling := make(chan struct{})
	cancel := make(chan struct{})
	var cancelOnce sync.Once
	cancelAll := func() { cancelOnce.Do(func() { close(cancel) }) }
	var wg sync.WaitGroup
	upDone := make(chan struct{})
	downDone := make(chan struct{})
	streamErr := make(chan error, 2)
	reportErr := func(err error) {
		select {
		case streamErr <- err:
		default:
		}
	}

	// 上行的结束边界由同一 WebSocket 上、排在全部二进制帧后的 stop 消息确定。
	if p.Direction == engine.DirUp || p.Direction == engine.DirBoth {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				msgType, data, err := conn.ReadMessage()
				if err != nil {
					reportErr(err)
					return
				}
				switch msgType {
				case websocket.BinaryMessage:
					rx.Add(uint64(len(data)))
				case websocket.TextMessage:
					var control struct {
						Type string `json:"type"`
					}
					if err := json.Unmarshal(data, &control); err != nil {
						reportErr(err)
						return
					}
					if control.Type == "stop" {
						close(upDone)
						return
					}
				}
			}
		}()
	} else {
		close(upDone)
	}

	// 采样推送 goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopSampling:
				return
			case <-ticker.C:
				rx.Tick()
				tx.Tick()
				rxRes := rx.Result()
				txRes := tx.Result()
				_ = writeJSON(ControlMessage{
					Type:       "sample",
					UpMBps:     rxRes.AvgMBps,
					UpMbitps:   rxRes.AvgMbps,
					DownMBps:   txRes.AvgMBps,
					DownMbitps: txRes.AvgMbps,
					UpBytes:    rxRes.TotalBytes,
					DownBytes:  txRes.TotalBytes,
				})
			}
		}
	}()

	// 下行在独立 goroutine 中按服务端时长发送；downDone 是其有序结束边界。
	if p.Direction == engine.DirDown || p.Direction == engine.DirBoth {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer close(downDone)
			buf := engine.NewZeroBuffer(p.PacketLen)
			timer := time.NewTimer(time.Duration(p.Duration * float64(time.Second)))
			defer timer.Stop()
			for {
				select {
				case <-cancel:
					return
				case <-timer.C:
					return
				default:
				}
				connMu.Lock()
				err := conn.WriteMessage(websocket.BinaryMessage, buf)
				connMu.Unlock()
				if err != nil {
					reportErr(err)
					return
				}
				tx.Add(uint64(len(buf)))
			}
		}()
	} else {
		close(downDone)
	}

	watchdog := time.NewTimer(time.Duration((p.Duration + 10) * float64(time.Second)))
	defer watchdog.Stop()
	waitFor := func(done <-chan struct{}) bool {
		select {
		case <-done:
			return true
		case <-streamErr:
			return false
		case <-watchdog.C:
			return false
		}
	}
	if !waitFor(downDone) || !waitFor(upDone) {
		cancelAll()
		_ = conn.Close()
		close(stopSampling)
		wg.Wait()
		return
	}

	cancelAll()
	close(stopSampling)
	rx.Tick()
	tx.Tick()

	rxRes := rx.Result()
	txRes := tx.Result()
	// 合并结果：选择有数据的方向
	res := &engine.Stats{}
	if p.Direction == engine.DirDown {
		res = &txRes
	} else if p.Direction == engine.DirUp {
		res = &rxRes
	} else {
		// both：取上行+下行合计
		res.TotalBytes = rxRes.TotalBytes + txRes.TotalBytes
		res.Duration = rxRes.Duration
		res.AvgMBps = float64(res.TotalBytes) / (1024 * 1024) / max(rxRes.Duration, 0.001)
		res.AvgMbps = res.AvgMBps * 8
		res.PeakMBps = rxRes.PeakMBps + txRes.PeakMBps
		res.PeakMbps = res.PeakMBps * 8
	}
	// result 与此前全部下行帧共用串行 writer；浏览器收到它时已处理完数据帧。
	_ = writeJSON(ControlMessage{Type: "result", Result: res})
	_ = conn.Close()
	wg.Wait()
}

func max(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}
