// Package rtcbridge 实现基于 WebRTC DataChannel 的 UDP 测速。
//
// 前端通过 WebRTC DataChannel（ordered:false, maxRetransmits:0）建立真实走 UDP 的数据通道，
// 信令经 /ws/signal 交换 SDP。DataChannel 底层是 SCTP-over-DTLS-over-UDP，
// 因此虽然浏览器无法直接发裸 UDP 包，但 WebRTC 路径的流量确实经过 UDP。
//
// 测速方向（前端本地采样，服务器负责搬运数据）：
//   - down: 服务器持续向 DataChannel 写满包（前端收）
//   - up:   前端向 DataChannel 写满包（服务器读）
//   - both: 同时进行
package rtcbridge

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pion/webrtc/v4"

	"speedTest/internal/engine"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1 << 20,
	WriteBufferSize: 1 << 20,
	CheckOrigin:     func(r *http.Request) bool { return true },
}

// signalMessage 信令消息。
type signalMessage struct {
	Type        string        `json:"type"` // offer | answer | stop | result | error
	SDP         string        `json:"sdp,omitempty"`
	Error       string        `json:"error,omitempty"`
	QueuedBytes uint64        `json:"queued_bytes,omitempty"`
	Result      *engine.Stats `json:"result,omitempty"`
}

// rtcStats 汇聚单个 DataChannel 测速流的两端统计。
// 服务器视角：客户端上行 = rx（收到）；客户端下行 = tx（发出）。
type rtcStats struct {
	rx        *engine.Sampler
	tx        *engine.Sampler
	startOnce sync.Once
	startedAt time.Time

	mu            sync.Mutex
	downSubmitted uint64
	downQueued    uint64
	downDrained   uint64
	downTruncated bool
}

func newRTCStats() *rtcStats { return &rtcStats{} }

func (s *rtcStats) start() {
	s.startOnce.Do(func() {
		s.startedAt = time.Now()
		s.rx = engine.NewSampler(100 * time.Millisecond)
		s.tx = engine.NewSampler(100 * time.Millisecond)
	})
}

func (s *rtcStats) activeDuration() float64 {
	s.start()
	return time.Since(s.startedAt).Seconds()
}

func parseSignalParams(q url.Values) (*engine.Params, error) {
	p := &engine.Params{Mode: engine.ModeUDP, Direction: engine.Direction(q.Get("dir")), Streams: 1}
	var err error
	if raw := q.Get("duration"); raw != "" {
		p.Duration, err = strconv.ParseFloat(raw, 64)
		if err != nil || math.IsNaN(p.Duration) || math.IsInf(p.Duration, 0) || p.Duration <= 0 {
			return nil, fmt.Errorf("invalid duration: %q", raw)
		}
	}
	if raw := q.Get("packet_len"); raw != "" {
		value, parseErr := strconv.ParseInt(raw, 10, 32)
		if parseErr != nil || value <= 0 {
			return nil, fmt.Errorf("invalid packet_len: %q", raw)
		}
		p.PacketLen = int(value)
	}
	if err := p.Validate(); err != nil {
		return nil, err
	}
	return p, nil
}

func drainSnapshot(submitted, buffered uint64) (drained, queued uint64, truncated bool) {
	queued = buffered
	if queued > submitted {
		queued = submitted
	}
	return submitted - queued, queued, queued > 0
}

func waitForDrain(buffered func() uint64, timeout time.Duration) uint64 {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		remaining := buffered()
		if remaining == 0 {
			return 0
		}
		select {
		case <-deadline.C:
			return buffered()
		case <-ticker.C:
		}
	}
}

// HandleSignal 处理 WebRTC 信令连接。
// query 参数：dir(down|up|both)、duration、packet_len、stream_id
func HandleSignal(w http.ResponseWriter, r *http.Request) {
	p, err := parseSignalParams(r.URL.Query())
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	dir := string(p.Direction)
	duration := p.Duration
	packetLen := p.PacketLen

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("signal upgrade: %v", err)
		return
	}
	defer conn.Close()

	// 读 offer
	conn.SetReadDeadline(time.Now().Add(30 * time.Second))
	_, data, err := conn.ReadMessage()
	if err != nil {
		return
	}
	var msg signalMessage
	if err := json.Unmarshal(data, &msg); err != nil || msg.Type != "offer" {
		_ = conn.WriteJSON(signalMessage{Type: "error", Error: "expected offer"})
		return
	}
	_ = conn.SetReadDeadline(time.Time{})

	// 创建 PeerConnection
	pc, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		_ = conn.WriteJSON(signalMessage{Type: "error", Error: err.Error()})
		return
	}
	defer pc.Close()

	cancel := make(chan struct{})
	var cancelOnce sync.Once
	cancelAll := func() { cancelOnce.Do(func() { close(cancel) }) }
	defer cancelAll()
	downDone := make(chan error, 1)
	upDone := make(chan error, 1)
	if dir != "down" && dir != "both" {
		downDone <- nil
	}
	if dir != "up" && dir != "both" {
		upDone <- nil
	}

	st := newRTCStats()

	// 数据通道处理
	pc.OnDataChannel(func(dc *webrtc.DataChannel) {
		handleDataChannel(dc, dir, duration, packetLen, st, cancel, downDone)
	})

	// 交换 SDP
	if err := pc.SetRemoteDescription(webrtc.SessionDescription{
		Type: webrtc.SDPTypeOffer,
		SDP:  msg.SDP,
	}); err != nil {
		_ = conn.WriteJSON(signalMessage{Type: "error", Error: err.Error()})
		return
	}
	answer, err := pc.CreateAnswer(nil)
	if err != nil {
		_ = conn.WriteJSON(signalMessage{Type: "error", Error: err.Error()})
		return
	}
	if err := pc.SetLocalDescription(answer); err != nil {
		_ = conn.WriteJSON(signalMessage{Type: "error", Error: err.Error()})
		return
	}
	// 等待 ICE gathering 完成，把全部 host candidate 内联进 answer，
	// 避免额外的 trickle ICE 信令往返（局域网直连场景）。
	<-webrtc.GatheringCompletePromise(pc)
	_ = conn.WriteJSON(signalMessage{Type: "answer", SDP: pc.LocalDescription().SDP})

	// 上行完成由浏览器在 DataChannel 排空后，通过可靠信令发送 stop。
	signalErr := make(chan error, 1)
	if dir == "up" || dir == "both" {
		go func() {
			for {
				var control signalMessage
				if err := conn.ReadJSON(&control); err != nil {
					signalErr <- err
					return
				}
				if control.Type == "stop" {
					upDone <- nil
					return
				}
			}
		}()
	}

	watchdog := time.NewTimer(time.Duration(duration+10) * time.Second)
	defer watchdog.Stop()
	waitDirection := func(done <-chan error) error {
		select {
		case err := <-done:
			return err
		case err := <-signalErr:
			return err
		case <-watchdog.C:
			return fmt.Errorf("WebRTC speed test timed out")
		case <-cancel:
			return fmt.Errorf("WebRTC speed test cancelled")
		}
	}
	if err := waitDirection(downDone); err != nil {
		_ = conn.WriteJSON(signalMessage{Type: "error", Error: err.Error()})
		return
	}
	if err := waitDirection(upDone); err != nil {
		_ = conn.WriteJSON(signalMessage{Type: "error", Error: err.Error()})
		return
	}
	activeDuration := st.activeDuration()
	cancelAll()

	// stop 触发后，给在途数据（上行残留包 / 下行缓冲排空）一点时间抵达
	time.Sleep(800 * time.Millisecond)

	// 发送服务端最终统计（双重对账），再关闭连接
	st.rx.Tick()
	st.tx.Tick()
	rxRes := st.rx.Result()
	txRes := st.tx.Result()
	res := &engine.Stats{}
	st.mu.Lock()
	downSubmitted := st.downSubmitted
	downQueued := st.downQueued
	downDrained := st.downDrained
	downTruncated := st.downTruncated
	st.mu.Unlock()
	switch dir {
	case "down":
		res = &txRes
		res.TotalBytes = downDrained
		res.SubmittedBytes = downSubmitted
		res.QueuedBytes = downQueued
		res.DownBytes = downDrained
		res.Truncated = downTruncated
		setResultAverage(res)
	case "up":
		res = &rxRes
		res.UpBytes = rxRes.TotalBytes
	default: // both
		res.TotalBytes = rxRes.TotalBytes + downDrained
		res.SubmittedBytes = downSubmitted
		res.QueuedBytes = downQueued
		res.UpBytes = rxRes.TotalBytes
		res.DownBytes = downDrained
		res.Truncated = downTruncated
		res.Duration = rxRes.Duration
		res.PeakMBps = rxRes.PeakMBps + txRes.PeakMBps
		res.PeakMbps = res.PeakMBps * 8
		setResultAverage(res)
	}
	res.Duration = activeDuration
	setResultAverage(res)
	_ = conn.WriteJSON(signalMessage{Type: "result", Result: res})
	_ = conn.Close()
}

// handleDataChannel 在一个 DataChannel 上执行打流。
func handleDataChannel(dc *webrtc.DataChannel, dir string, duration float64, packetLen int, st *rtcStats, cancel <-chan struct{}, downDone chan<- error) {
	dc.SetBufferedAmountLowThreshold(1 << 20)

	// 读：处理客户端上行（up/both）——服务器计数接收量
	if dir == "up" || dir == "both" {
		dc.OnMessage(func(msg webrtc.DataChannelMessage) {
			if !msg.IsString {
				st.start()
				st.rx.Add(uint64(len(msg.Data)))
			}
		})
	}

	dc.OnOpen(func() {
		st.start()
		if dir == "down" || dir == "both" {
			go func() {
				downDone <- sendDown(dc, duration, packetLen, st, cancel)
			}()
		}
	})
}

type dataChannelSender interface {
	Send([]byte) error
	BufferedAmount() uint64
}

// sendDown 服务器持续发送满包（下行）。
//
// 关键：pion 的 DataChannel.Send 缓冲会无限增长（尤其 unreliable 模式下
// SCTP 流控宽松），若不做背压会导致 bufferedAmount 失控、测量失真。
// 因此用 bufferedAmount 阈值做发送端背压：缓冲水位高时暂停，
// 让接收端消化速度成为真实测速瓶颈（与浏览器端 send 背压同理）。
// 发送量为真实进入通道的字节（st.tx 计数），用于服务端对账。
func sendDown(dc dataChannelSender, duration float64, packetLen int, st *rtcStats, cancel <-chan struct{}) error {
	st.start()
	buf := engine.NewZeroBuffer(packetLen)
	deadline := time.NewTimer(time.Duration(duration * float64(time.Second)))
	defer deadline.Stop()

	const (
		highWater = 4 * 1024 * 1024 // 4MiB 缓冲水位上限
		lowWater  = 1 * 1024 * 1024 // 降至 1MiB 再继续
		sleepNs   = 200 * time.Microsecond
	)

	for {
		select {
		case <-cancel:
			finalizeDown(st, dc.BufferedAmount())
			return fmt.Errorf("downlink cancelled")
		case <-deadline.C:
			remaining := waitForDrain(dc.BufferedAmount, 3*time.Second)
			finalizeDown(st, remaining)
			return nil
		default:
		}
		// 背压：缓冲超过高水位时等待其回落
		if dc.BufferedAmount() > highWater {
			// 简单忙等（不依赖 OnBufferedAmountLow，避免事件竞态）
			for dc.BufferedAmount() > lowWater {
				select {
				case <-cancel:
					finalizeDown(st, dc.BufferedAmount())
					return fmt.Errorf("downlink cancelled")
				case <-deadline.C:
					remaining := waitForDrain(dc.BufferedAmount, 3*time.Second)
					finalizeDown(st, remaining)
					return nil
				default:
					time.Sleep(sleepNs)
				}
			}
		}
		if err := dc.Send(buf); err != nil {
			finalizeDown(st, dc.BufferedAmount())
			return err
		}
		st.tx.Add(uint64(len(buf)))
	}
}

func finalizeDown(st *rtcStats, remaining uint64) {
	submitted := st.tx.Bytes()
	drained, queued, truncated := drainSnapshot(submitted, remaining)
	st.mu.Lock()
	st.downSubmitted = submitted
	st.downQueued = queued
	st.downDrained = drained
	st.downTruncated = truncated
	st.mu.Unlock()
}

func setResultAverage(result *engine.Stats) {
	duration := max(result.Duration, 0.001)
	result.AvgMBps = float64(result.TotalBytes) / (1024 * 1024) / duration
	result.AvgMbps = result.AvgMBps * 8
}

func max(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}
