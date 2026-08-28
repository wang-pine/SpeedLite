// Package tcpnative 实现基于原生 TCP socket 的测速（iperf 风格）。
// 协议：客户端连接后先发送一行 JSON 控制帧（以 \n 结尾），格式：
//
//	{"mode":"tcp","dir":"up|down","streams":N,"duration":S,"packet_len":N,"stream_id":K}
//
// 然后进入数据阶段：
//   - dir=up:   客户端持续发送二进制满包，服务器读取计数。
//   - dir=down: 服务器持续发送二进制满包，客户端读取计数。
//
// 结束后服务器发送一行 JSON 结果帧（{"type":"result","result":{...}}）并关闭连接。
package tcpnative

import (
	"bufio"
	"encoding/json"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"speedlite/internal/engine"
)

// Request 客户端发送的控制帧。
type Request struct {
	Mode      string  `json:"mode"`
	Dir       string  `json:"dir"`
	Streams   int     `json:"streams"`
	Duration  float64 `json:"duration"`
	PacketLen int     `json:"packet_len"`
	StreamID  int     `json:"stream_id"`
}

// Response 服务器返回的结果帧。
type Response struct {
	Type   string        `json:"type"`
	Result *engine.Stats `json:"result,omitempty"`
	Error  string        `json:"error,omitempty"`
}

// HandleConn 处理一条原生 TCP 测速连接。
func HandleConn(conn net.Conn) {
	defer conn.Close()
	// 读控制帧（一行 JSON）
	br := bufio.NewReaderSize(conn, 1<<16)
	line, err := br.ReadBytes('\n')
	if err != nil {
		return
	}
	var req Request
	if err := json.Unmarshal(line, &req); err != nil {
		sendError(conn, "bad request: "+err.Error())
		return
	}
	p := &engine.Params{
		Mode:      engine.Mode(req.Mode),
		Direction: engine.Direction(req.Dir),
		Streams:   req.Streams,
		Duration:  req.Duration,
		PacketLen: req.PacketLen,
	}
	if err := p.Validate(); err != nil {
		sendError(conn, err.Error())
		return
	}

	sampler := engine.NewSampler(100 * time.Millisecond)
	deadline := time.NewTimer(time.Duration(p.Duration * float64(time.Second)))
	defer deadline.Stop()
	stop := make(chan struct{})
	var wg sync.WaitGroup

	if p.Direction == engine.DirDown {
		// 服务器发送，客户端接收
		buf := engine.NewZeroBuffer(p.PacketLen)
	loop:
		for {
			select {
			case <-deadline.C:
				break loop
			case <-stop:
				break loop
			default:
			}
			if _, err := conn.Write(buf); err != nil {
				break
			}
			sampler.Add(uint64(len(buf)))
		}
	} else {
		// 服务器接收，客户端发送（up）
		// 读 goroutine 用带超时的读，deadline 到后 stop 关闭，它随之退出。
		wg.Add(1)
		go func() {
			defer wg.Done()
			buf := make([]byte, 1<<20)
			for {
				select {
				case <-stop:
					return
				default:
				}
				_ = conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
				n, err := conn.Read(buf)
				if n > 0 {
					sampler.Add(uint64(n))
				}
				if err != nil {
					if err == io.EOF {
						return
					}
					if ne, ok := err.(net.Error); ok && ne.Timeout() {
						continue // 超时只是唤醒检查 stop
					}
					return
				}
			}
		}()
		<-deadline.C
		close(stop)
	}

	wg.Wait()
	sampler.Tick()
	res := sampler.Result()
	enc := json.NewEncoder(conn)
	_ = enc.Encode(Response{Type: "result", Result: &res})
}

// Listen 监听原生 TCP 测速端口。
func Listen(addr string) (net.Listener, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go HandleConn(conn)
		}
	}()
	log.Printf("[tcp-native] listening on %s", addr)
	return ln, nil
}

func sendError(conn net.Conn, msg string) {
	enc := json.NewEncoder(conn)
	_ = enc.Encode(Response{Type: "error", Error: msg})
}
