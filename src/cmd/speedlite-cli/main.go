// Command speedlite-cli 是测速 CLI 客户端：用原生 TCP/UDP socket 测量到服务器的上下行带宽。
//
// 用法（flags 可放在任意位置，包括位置参数之后）：
//
//	speedlite-cli -s host:port [mode] [direction] [flags]
//	speedlite-cli -s 192.168.1.10 tcp up -P 8 -t 5 -l 65536
//	speedlite-cli -s 192.168.1.10 tcp down -t 5 -P 4
//
// 位置参数：mode=tcp|udp（默认 tcp），direction=up|down（默认 up）。
// 服务器端口约定：tcp 用 -tcp-port（默认 5001），udp 用 -udp-port（默认 5201）。
package main

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"speedlite/internal/engine"
	"speedlite/internal/version"
)

// options 用简化自定义解析（标准 flag 包遇到首个位置参数就停止解析，
// 无法支持 "speedlite-cli tcp down -P 8" 这种混合写法）。
type options struct {
	server    string
	tcpPort   int
	udpPort   int
	streams   int
	duration  float64
	packetLen int
}

func defaultOptions() options {
	return options{
		server:    "127.0.0.1:5001",
		tcpPort:   5001,
		udpPort:   5201,
		streams:   4,
		duration:  10,
		packetLen: 131072,
	}
}

func parseArgs(args []string) (options, []string, error) {
	opts := defaultOptions()
	var pos []string

	// 支持 -name value 与 -name=value 两种形式
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if !strings.HasPrefix(arg, "-") || arg == "-" {
			pos = append(pos, arg)
			continue
		}
		name := strings.TrimLeft(arg, "-")
		val := ""
		hasVal := false
		if eq := strings.Index(name, "="); eq >= 0 {
			val = name[eq+1:]
			name = name[:eq]
			hasVal = true
		}
		needValue := func() (string, error) {
			if hasVal {
				return val, nil
			}
			if i+1 >= len(args) {
				return "", fmt.Errorf("flag -%s needs a value", name)
			}
			i++
			return args[i], nil
		}
		switch name {
		case "s":
			v, err := needValue()
			if err != nil {
				return opts, nil, err
			}
			opts.server = v
		case "tcp-port":
			v, err := needValue()
			if err != nil {
				return opts, nil, err
			}
			opts.tcpPort, err = strconv.Atoi(v)
			if err != nil {
				return opts, nil, fmt.Errorf("bad -tcp-port %q", v)
			}
		case "udp-port":
			v, err := needValue()
			if err != nil {
				return opts, nil, err
			}
			opts.udpPort, err = strconv.Atoi(v)
			if err != nil {
				return opts, nil, fmt.Errorf("bad -udp-port %q", v)
			}
		case "P":
			v, err := needValue()
			if err != nil {
				return opts, nil, err
			}
			opts.streams, err = strconv.Atoi(v)
			if err != nil {
				return opts, nil, fmt.Errorf("bad -P %q", v)
			}
		case "t":
			v, err := needValue()
			if err != nil {
				return opts, nil, err
			}
			opts.duration, err = strconv.ParseFloat(v, 64)
			if err != nil {
				return opts, nil, fmt.Errorf("bad -t %q", v)
			}
		case "l":
			v, err := needValue()
			if err != nil {
				return opts, nil, err
			}
			opts.packetLen, err = strconv.Atoi(v)
			if err != nil {
				return opts, nil, fmt.Errorf("bad -l %q", v)
			}
		case "h", "help":
			return opts, nil, errHelp
		default:
			return opts, nil, fmt.Errorf("unknown flag: -%s", name)
		}
	}
	return opts, pos, nil
}

var errHelp = fmt.Errorf("help requested")

func usage() {
	fmt.Fprintf(os.Stderr, "speedlite-cli - 用原生 socket 测量 TCP/UDP 上下行带宽\n\n")
	fmt.Fprintf(os.Stderr, "用法:\n  speedlite-cli -s host:port [tcp|udp] [up|down] [flags]\n\n")
	fmt.Fprintf(os.Stderr, "位置参数:\n  mode       tcp|udp（默认 tcp）\n  direction  up|down（默认 up）\n\n")
	fmt.Fprintf(os.Stderr, "Flags:\n")
	fmt.Fprintf(os.Stderr, "  -s <host:port>   服务器地址（默认 127.0.0.1:5001）\n")
	fmt.Fprintf(os.Stderr, "  -tcp-port <n>    原生 TCP 测速端口（默认 5001）\n")
	fmt.Fprintf(os.Stderr, "  -udp-port <n>    原生 UDP 测速端口（默认 5201）\n")
	fmt.Fprintf(os.Stderr, "  -P <n>           并行流数（默认 4）\n")
	fmt.Fprintf(os.Stderr, "  -t <秒>          测速时长（默认 10）\n")
	fmt.Fprintf(os.Stderr, "  -l <字节>        包大小（默认 131072）\n")
	fmt.Fprintf(os.Stderr, "  -version         显示版本（默认 %s）\n", version.Version)
}

func main() {
	// -version / --version 直接退出
	for _, a := range os.Args[1:] {
		if a == "-version" || a == "--version" {
			fmt.Printf("speedlite-cli %s\n", version.Version)
			os.Exit(0)
		}
	}

	opts, pos, err := parseArgs(os.Args[1:])
	if err == errHelp {
		usage()
		os.Exit(0)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "参数错误: %v\n\n", err)
		usage()
		os.Exit(2)
	}

	mode := engine.ModeTCP
	dir := engine.DirUp
	if len(pos) > 0 {
		switch strings.ToLower(pos[0]) {
		case "tcp":
			mode = engine.ModeTCP
		case "udp":
			mode = engine.ModeUDP
		default:
			fmt.Fprintf(os.Stderr, "unknown mode %q (tcp|udp)\n", pos[0])
			os.Exit(2)
		}
	}
	if len(pos) > 1 {
		switch strings.ToLower(pos[1]) {
		case "up":
			dir = engine.DirUp
		case "down":
			dir = engine.DirDown
		default:
			fmt.Fprintf(os.Stderr, "unknown direction %q (up|down)\n", pos[1])
			os.Exit(2)
		}
	}

	p := &engine.Params{Mode: mode, Direction: dir, Streams: opts.streams, Duration: opts.duration, PacketLen: opts.packetLen}
	if err := p.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "参数错误: %v\n", err)
		os.Exit(2)
	}

	fmt.Printf("speedlite-cli → %s\n模式=%s 方向=%s 流数=%d 时长=%.1fs 包大小=%dB\n\n",
		opts.server, p.Mode, p.Direction, p.Streams, p.Duration, p.PacketLen)

	var wg sync.WaitGroup
	results := make([]engine.Stats, 0, p.Streams)
	var mu sync.Mutex
	start := time.Now()

	for i := 0; i < p.Streams; i++ {
		wg.Add(1)
		go func(streamID int) {
			defer wg.Done()
			var st engine.Stats
			var err error
			if p.Mode == engine.ModeTCP {
				st, err = runTCPStream(opts, p, streamID)
			} else {
				st, err = runUDPStream(opts, p, streamID)
			}
			if err != nil {
				fmt.Fprintf(os.Stderr, "  [stream %d] %v\n", streamID, err)
				return
			}
			mu.Lock()
			results = append(results, st)
			mu.Unlock()
		}(i)
	}
	wg.Wait()

	elapsed := time.Since(start).Seconds()
	if len(results) == 0 {
		fmt.Println("没有有效的测速结果")
		os.Exit(1)
	}

	// 汇聚
	total := engine.Stats{Duration: elapsed}
	var sumBytes, lost uint64
	var peak float64
	for _, r := range results {
		sumBytes += r.TotalBytes
		lost += r.Lost
		if r.PeakMBps > peak {
			peak = r.PeakMBps
		}
		total.Packets += r.Packets
	}
	total.TotalBytes = sumBytes
	total.AvgMBps = float64(sumBytes) / (1024 * 1024) / elapsed
	total.AvgMbps = total.AvgMBps * 8
	total.PeakMBps = peak
	total.PeakMbps = peak * 8
	total.Lost = lost
	if total.Packets > 0 {
		total.LostPct = float64(lost) / float64(total.Packets) * 100
	}

	fmt.Println("────────────── 结果 ──────────────")
	printStats(total, p.Mode)
}

func printStats(st engine.Stats, mode engine.Mode) {
	fmt.Printf("总传输:   %.2f MiB (%.2f MB)\n", float64(st.TotalBytes)/(1024*1024), float64(st.TotalBytes)/(1000*1000))
	fmt.Printf("平均速率: %.2f MB/s (%.2f Mbit/s)\n", st.AvgMBps, st.AvgMbps)
	fmt.Printf("峰值速率: %.2f MB/s (%.2f Mbit/s)\n", st.PeakMBps, st.PeakMbps)
	fmt.Printf("实际用时: %.2f s\n", st.Duration)
	if mode == engine.ModeUDP {
		fmt.Printf("UDP 包数: %d\n", st.Packets)
		fmt.Printf("丢包数:   %d (%.2f%%)\n", st.Lost, st.LostPct)
		fmt.Printf("抖动:     %.2f ms\n", st.Jitter)
	}
}

// runTCPStream 单个 TCP 流。
func runTCPStream(opts options, p *engine.Params, streamID int) (engine.Stats, error) {
	addr := joinPort(hostOnly(opts.server), opts.tcpPort)
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return engine.Stats{}, err
	}
	defer conn.Close()

	// 发送控制帧
	req := tcpRequest{
		Mode:      "tcp",
		Dir:       string(p.Direction),
		Streams:   p.Streams,
		Duration:  p.Duration,
		PacketLen: p.PacketLen,
		StreamID:  streamID,
	}
	line, _ := json.Marshal(req)
	if _, err := conn.Write(append(line, '\n')); err != nil {
		return engine.Stats{}, err
	}

	if p.Direction == engine.DirDown {
		return tcpReceive(conn, p)
	}
	return tcpSend(conn, p)
}

// tcpRequest 与服务器 tcpnative.Request 一致。
type tcpRequest struct {
	Mode      string  `json:"mode"`
	Dir       string  `json:"dir"`
	Streams   int     `json:"streams"`
	Duration  float64 `json:"duration"`
	PacketLen int     `json:"packet_len"`
	StreamID  int     `json:"stream_id"`
}

// tcpSend 上行：客户端发送满包。
func tcpSend(conn net.Conn, p *engine.Params) (engine.Stats, error) {
	sampler := engine.NewSampler(100 * time.Millisecond)
	buf := engine.NewZeroBuffer(p.PacketLen)
	deadline := time.Now().Add(time.Duration(p.Duration * float64(time.Second)))
	// 每 100ms 采样（本地瞬时速率）
	ticker := time.NewTicker(100 * time.Millisecond)
	go func() {
		defer ticker.Stop()
		for range ticker.C {
			sampler.Tick()
		}
	}()
	for time.Now().Before(deadline) {
		if _, err := conn.Write(buf); err != nil {
			break
		}
		sampler.Add(uint64(len(buf)))
	}
	sampler.Tick()
	res := sampler.Result()
	// 读服务器最终结果
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	var resp tcpResponse
	dec := json.NewDecoder(bufio.NewReader(conn))
	if err := dec.Decode(&resp); err == nil && resp.Result != nil {
		res = *resp.Result
	}
	return res, nil
}

// tcpReceive 下行：客户端接收。
func tcpReceive(conn net.Conn, p *engine.Params) (engine.Stats, error) {
	sampler := engine.NewSampler(100 * time.Millisecond)
	buf := make([]byte, 1<<20)
	ticker := time.NewTicker(100 * time.Millisecond)
	done := make(chan struct{})
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				sampler.Tick()
			}
		}
	}()
	_ = conn.SetReadDeadline(time.Now().Add(time.Duration(p.Duration*float64(time.Second)) + 5*time.Second))
	for {
		n, err := conn.Read(buf)
		if n > 0 {
			sampler.Add(uint64(n))
		}
		if err != nil {
			if err == io.EOF {
				break
			}
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				break
			}
			return sampler.Result(), err
		}
	}
	close(done)
	sampler.Tick()
	return sampler.Result(), nil
}

type tcpResponse struct {
	Type   string       `json:"type"`
	Result *engine.Stats `json:"result,omitempty"`
	Error  string       `json:"error,omitempty"`
}

// runUDPStream 单个 UDP 流。
func runUDPStream(opts options, p *engine.Params, streamID int) (engine.Stats, error) {
	raddr, err := net.ResolveUDPAddr("udp", joinPort(hostOnly(opts.server), opts.udpPort))
	if err != nil {
		return engine.Stats{}, err
	}
	conn, err := net.DialUDP("udp", nil, raddr)
	if err != nil {
		return engine.Stats{}, err
	}
	defer conn.Close()

	const sessID = 1
	// HELLO
	direction := byte(dirUp)
	if p.Direction == engine.DirDown {
		direction = dirDown
	}
	hello := makeHello(sessID, direction, p.Duration, p.PacketLen)
	if _, err := conn.Write(hello); err != nil {
		return engine.Stats{}, err
	}
	// 等 ACK
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	ackBuf := make([]byte, 128)
	n, err := conn.Read(ackBuf)
	if err != nil {
		return engine.Stats{}, fmt.Errorf("no ack: %v", err)
	}
	if n < 26 || ackBuf[24] != ctrlAck {
		return engine.Stats{}, fmt.Errorf("bad ack")
	}

	if p.Direction == engine.DirUp {
		return udpSend(conn, sessID, p)
	}
	return udpReceive(conn, sessID, p)
}

// udpSend 上行：客户端发送满包 UDP。
func udpSend(conn *net.UDPConn, sessID uint32, p *engine.Params) (engine.Stats, error) {
	sampler := engine.NewSampler(100 * time.Millisecond)
	payload := engine.NewZeroBuffer(p.PacketLen - udpHdrLen)
	if len(payload) <= 0 {
		payload = engine.NewZeroBuffer(1400 - udpHdrLen)
	}
	deadline := time.Now().Add(time.Duration(p.Duration * float64(time.Second)))
	seq := uint64(1)
	ticker := time.NewTicker(100 * time.Millisecond)
	go func() {
		defer ticker.Stop()
		for range ticker.C {
			sampler.Tick()
		}
	}()
	for time.Now().Before(deadline) {
		pkt := makeDataPacket(sessID, seq, time.Now().UnixNano(), payload)
		if _, err := conn.Write(pkt); err != nil {
			break
		}
		sampler.Add(uint64(len(pkt)))
		seq++
	}
	// DONE
	done := makeCtrlPacket(sessID, ctrlDone)
	_, _ = conn.Write(done)
	// 读结果（服务器可能在收到 DONE 前就已统计完毕）
	_ = conn.SetReadDeadline(time.Now().Add(1 * time.Second))
	buf := make([]byte, 4096)
	local := sampler.Result()
	for {
		n, err := conn.Read(buf)
		if err != nil {
			break
		}
		if n > 26 && buf[24] == ctrlResult {
			var remote engine.Stats
			_ = json.Unmarshal(buf[26:n], &remote)
			// 速率/字节以本地采样为准；丢包/抖动以服务器统计为准
			local.Lost = remote.Lost
			local.LostPct = remote.LostPct
			local.Jitter = remote.Jitter
			if local.Packets == 0 {
				local.Packets = remote.Packets
			}
			local.Packets = seq - 1
			return local, nil
		}
	}
	return local, nil
}

// udpReceive 下行：客户端接收 UDP。
func udpReceive(conn *net.UDPConn, sessID uint32, p *engine.Params) (engine.Stats, error) {
	sampler := engine.NewSampler(100 * time.Millisecond)
	var mu sync.Mutex
	var lastSeq uint64
	var got, lost uint64
	lastTs := int64(0)
	lastDelta := int64(0)
	jitter := 0.0

	buf := make([]byte, 65536)
	deadline := time.Now().Add(time.Duration(p.Duration*float64(time.Second)) + 5*time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	done := make(chan struct{})
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				sampler.Tick()
			}
		}
	}()

	for {
		_ = conn.SetReadDeadline(deadline)
		n, _, err := conn.ReadFromUDP(buf)
		if err != nil {
			break
		}
		if n < 26 {
			continue
		}
		seq := binary.BigEndian.Uint64(buf[8:16])
		ts := int64(binary.BigEndian.Uint64(buf[16:24]))
		if seq == 0 {
			// 控制包：服务器通知下行结束
			if n > 24 && buf[24] == ctrlDone {
				break
			}
			continue
		}
		sampler.Add(uint64(n))

		mu.Lock()
		if seq <= lastSeq {
			// 重复/乱序
		} else if lastSeq > 0 && seq != lastSeq+1 {
			lost += seq - lastSeq - 1
		}
		lastSeq = seq
		if lastTs != 0 {
			delta := ts - lastTs
			if lastDelta == 0 {
				lastDelta = delta
			} else {
				d := math.Abs(float64(delta - lastDelta))
				jitter = (jitter*15 + d/1e6) / 16
				lastDelta = delta
			}
		} else {
			lastDelta = 0
		}
		lastTs = ts
		got++
		mu.Unlock()
	}
	close(done)
	sampler.Tick()
	res := sampler.Result()
	res.Packets = got
	res.Lost = lost
	if got > 0 {
		res.LostPct = float64(lost) / float64(got+lost) * 100
	}
	res.Jitter = jitter
	return res, nil
}

// --- UDP 包构造（与 udpnative 保持一致）---

const udpHdrLen = 24

var udpMagic = []byte{'S', 'P', 'D', 'U'}

const (
	ctrlHello  = 1
	ctrlAck    = 2
	ctrlDone   = 3
	ctrlResult = 4
	dirUp      = 1
	dirDown    = 2
)

func makeHello(sessID uint32, direction byte, dur float64, plen int) []byte {
	// 26B payload: type + direction + dur(8) + plen(8)
	buf := make([]byte, udpHdrLen+26)
	copy(buf, udpMagic)
	binary.BigEndian.PutUint32(buf[4:8], sessID)
	binary.BigEndian.PutUint64(buf[8:16], 0) // seq=0 control
	binary.BigEndian.PutUint64(buf[16:24], 0)
	buf[24] = ctrlHello
	buf[25] = direction
	binary.BigEndian.PutUint64(buf[26:34], math.Float64bits(dur))
	binary.BigEndian.PutUint64(buf[34:42], uint64(plen))
	return buf
}

func makeDataPacket(sessID uint32, seq uint64, tsNs int64, payload []byte) []byte {
	buf := make([]byte, udpHdrLen+len(payload))
	copy(buf, udpMagic)
	binary.BigEndian.PutUint32(buf[4:8], sessID)
	binary.BigEndian.PutUint64(buf[8:16], seq)
	binary.BigEndian.PutUint64(buf[16:24], uint64(tsNs))
	copy(buf[udpHdrLen:], payload)
	return buf
}

func makeCtrlPacket(sessID uint32, ctype byte) []byte {
	buf := make([]byte, udpHdrLen+2)
	copy(buf, udpMagic)
	binary.BigEndian.PutUint32(buf[4:8], sessID)
	binary.BigEndian.PutUint64(buf[8:16], 0)
	binary.BigEndian.PutUint64(buf[16:24], 0)
	buf[24] = ctype
	return buf
}

// hostOnly 从 server 参数中提取主机名（正确处理 IPv6 与带端口形式）。
func hostOnly(s string) string {
	if h, _, err := net.SplitHostPort(s); err == nil {
		return h
	}
	// 没有端口：可能是裸主机名或裸 IPv6
	s = strings.Trim(s, "[]")
	return s
}

// joinPort 组装 host:port（正确处理 IPv6）。
func joinPort(host string, port int) string {
	return net.JoinHostPort(host, fmt.Sprintf("%d", port))
}
