// Package wsstream 实现基于 WebSocket 的 TCP 测速。
// WebSocket 运行在 TCP 之上，其吞吐与原生 TCP 基本一致（仅少量帧头开销），
// 因此前端页面通过 WS 打流即可测量 TCP 上下行带宽。
package wsstream

import (
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"speedlite/internal/engine"
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
		Mode:       engine.Mode(q.Get("mode")),
		Direction:  engine.Direction(q.Get("dir")),
		Streams:    atoi(q.Get("streams")),
		Duration:   atof(q.Get("duration")),
		PacketLen:  atoi(q.Get("packet_len")),
		PacketKind: q.Get("packet_kind"),
		PacketSizes: parseSizes(q.Get("packet_sizes")),
	}
	if p.PacketKind == "dynamic" && len(p.PacketSizes) > 0 {
		p.PacketLen = p.PacketSizes[0]
	}
	if err := p.Validate(); err != nil {
		return nil, err
	}
	return p, nil
}

// parseSizes 解析逗号分隔的包长序列，如 "512,1200,1400,1500"。
func parseSizes(s string) []int {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]int, 0, len(parts))
	for _, part := range parts {
		v := atoi(strings.TrimSpace(part))
		if v > 0 {
			out = append(out, v)
		}
	}
	return out
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
	// 为防前端 stop 因发送积压/网络阻塞而迟到：除读到 stop 外，还加一个「时长定时器」，
	// 到点也主动关闭 upDone，保证上行最迟 duration 秒结束（不会拖到 watchdog 超时）。
	if p.Direction == engine.DirUp || p.Direction == engine.DirBoth {
		var upOnce sync.Once
		finishUp := func() { upOnce.Do(func() { close(upDone) }) }
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
						finishUp()
						return
					}
				}
			}
		}()
		// 时长到点兜底：即使 stop 未及时到达，也在 duration 后结束上行边界。
		// 留 +1s 余量给前端的 stop（若前端 stop 在 10s 到达，它会先关闭 upDone，
		// 读 goroutine 读完所有数据帧后自然结束；此定时器仅在 stop 真的丢失时兜底，
		// 避免与前端 stop 竞争导致 result 早于数据发回）。
		wg.Add(1)
		go func() {
			defer wg.Done()
			timer := time.NewTimer(time.Duration((p.Duration + 1) * float64(time.Second)))
			defer timer.Stop()
			select {
			case <-cancel:
				return
			case <-timer.C:
				finishUp()
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
	// 关键：WriteMessage 是无超时的阻塞写，若客户端读慢/TCP 拥塞，它会无限阻塞，
	// 即使 10s timer 到点 goroutine 也退不出，downDone 不关闭，测试拖到 watchdog 才超时。
	// 因此给每次写设绝对写超时（开始 + duration + 2s），TCP 阻塞时超时返回错误，
	// 强制 goroutine 退出并关闭 downDone，保证按时结束。
	if p.Direction == engine.DirDown || p.Direction == engine.DirBoth {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer close(downDone)
			// 包长迭代器：固定模式（单包长）；动态模式按 PacketSizes 轮换（媒体混合）。
			pktIter := makePktIter(p.PacketLen, p.PacketKind, p.PacketSizes)
			timer := time.NewTimer(time.Duration(p.Duration * float64(time.Second)))
			defer timer.Stop()
			// 写超时设为「测试开始 + duration + 1s」的绝对上限：正常测试在 duration 内
			// 快速写完所有帧，永不触发超时（连接完好，result 可正常回）；仅当 TCP 严重
			// 拥塞导致某次写在 deadline 后仍未完成时，才超时返回错误强制结束，避免
			// “10s 测试被拖到 20s”。超时后连接 corrupt，此时回不到 result 也属极端场景。
			writeDeadline := time.Now().Add(time.Duration((p.Duration + 1) * float64(time.Second)))
			for {
				select {
				case <-cancel:
					return
				case <-timer.C:
					return
				default:
				}
				buf := pktIter()
				_ = conn.SetWriteDeadline(writeDeadline)
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

	watchdog := time.NewTimer(time.Duration((p.Duration + 4) * float64(time.Second)))
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
		res.DownBytes = txRes.TotalBytes
	} else if p.Direction == engine.DirUp {
		res = &rxRes
		res.UpBytes = rxRes.TotalBytes
	} else {
		// both：取上行+下行合计
		res.TotalBytes = rxRes.TotalBytes + txRes.TotalBytes
		res.UpBytes = rxRes.TotalBytes
		res.DownBytes = txRes.TotalBytes
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

// makePktIter 返回一个包长迭代器：固定模式每个包用 packetLen；动态模式按 sizes 轮换
// （模拟媒体混合：大小不一的 RTP 包）。预分配各档 buffer，避免每包重复分配。
func makePktIter(packetLen int, kind string, sizes []int) func() []byte {
	if kind != "dynamic" || len(sizes) == 0 {
		buf := engine.NewZeroBuffer(packetLen)
		return func() []byte { return buf }
	}
	bufs := make([][]byte, len(sizes))
	for i, sz := range sizes {
		if sz <= 0 {
			sz = packetLen
		}
		bufs[i] = engine.NewZeroBuffer(sz)
	}
	i := 0
	return func() []byte {
		b := bufs[i]
		i = (i + 1) % len(bufs)
		return b
	}
}
