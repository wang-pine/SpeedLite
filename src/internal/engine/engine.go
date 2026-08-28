// Package engine 提供测速的统计核心：窗口采样、速率计算与结果汇聚。
// 它是纯逻辑模块，不依赖任何传输层，可独立单元测试。
// 传输层（WS/UDP/WebRTC）负责建立连接与打流，把字节计数喂给 Sampler。
package engine

import (
	"fmt"
	"sync"
	"time"
)

// Mode 测速模式。
type Mode string

const (
	ModeTCP Mode = "tcp"
	ModeUDP Mode = "udp"
)

// Direction 测速方向（以"客户端"视角定义：上行=客户端→服务器）。
type Direction string

const (
	DirUp   Direction = "up"
	DirDown Direction = "down"
	DirBoth Direction = "both"
)

// Params 一次测速的参数。
type Params struct {
	Mode      Mode      `json:"mode"`
	Direction Direction `json:"direction"`
	Streams   int       `json:"streams"`    // 并行流数
	Duration  float64   `json:"duration"`   // 秒
	PacketLen int       `json:"packet_len"` // 帧/包大小（字节）
	PacketKind string   `json:"packet_kind,omitempty"`  // fixed | dynamic
	PacketSizes []int   `json:"packet_sizes,omitempty"` // 动态包长轮换序列（bytes）
	NoTLS     bool      `json:"-"`          // CLI 用：是否关闭 TLS
	Server    string    `json:"-"`          // CLI 用：目标地址
}

// Validate 校验并补齐默认值。
func (p *Params) Validate() error {
	if p.Mode == "" {
		p.Mode = ModeTCP
	}
	if p.Direction == "" {
		p.Direction = DirUp
	}
	if p.Mode != ModeTCP && p.Mode != ModeUDP {
		return fmt.Errorf("unknown mode: %q", p.Mode)
	}
	if p.Direction != DirUp && p.Direction != DirDown && p.Direction != DirBoth {
		return fmt.Errorf("unknown direction: %q", p.Direction)
	}
	if p.Streams <= 0 {
		p.Streams = 4
	}
	if p.Streams > 64 {
		return fmt.Errorf("streams too large: %d", p.Streams)
	}
	if p.Duration <= 0 {
		p.Duration = 10
	}
	if p.Duration > 600 {
		return fmt.Errorf("duration too large: %.0f", p.Duration)
	}
	if p.PacketLen <= 0 {
		p.PacketLen = 131072 // 128 KiB
	}
	if p.PacketLen > 8*1024*1024 {
		return fmt.Errorf("packet_len too large: %d", p.PacketLen)
	}
	return nil
}

// Sample 单个时间窗口的统计点。
type Sample struct {
	Time     float64 `json:"t"` // 相对开始时间的秒数
	Bytes    uint64  `json:"b"` // 本窗口内字节数
	RateMBps float64 `json:"r"` // 本窗口平均速率 MB/s
	RateMbps float64 `json:"m"` // 本窗口平均速率 Mbit/s
}

// Sampler 是字节计数 + 窗口采样的核心。线程安全。
type Sampler struct {
	mu       sync.Mutex
	started  time.Time
	window   time.Duration
	bytes    uint64 // 累计字节
	last     uint64 // 上次采样时累计
	lastTime time.Time
	peak     float64
	samples  []Sample
	// 峰值平滑：保存最近若干窗口的瞬时速率，峰值取这些窗口的滑动平均最大值，
	// 避免单窗口突发导致峰值虚高（如小包高 pps 下瞬时速率远超真实链路带宽）。
	rateWin []float64
	peakOn  bool // 是否已启用峰值平滑（window>0）
}

// NewSampler 创建采样器。window 为采样窗口（如 100ms）。
func NewSampler(window time.Duration) *Sampler {
	now := time.Now()
	return &Sampler{
		started:  now,
		window:   window,
		lastTime: now,
	}
}

// Add 累加字节数。
func (s *Sampler) Add(n uint64) {
	s.mu.Lock()
	s.bytes += n
	s.mu.Unlock()
}

// Bytes 返回累计字节。
func (s *Sampler) Bytes() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.bytes
}

// Tick 触发一次采样（通常由 ticker 或传输层每窗口调用一次）。
func (s *Sampler) Tick() {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	dt := now.Sub(s.lastTime).Seconds()
	if dt <= 0 {
		return
	}
	delta := s.bytes - s.last
	rate := float64(delta) / dt / (1024 * 1024) // MB/s
	s.samples = append(s.samples, Sample{
		Time:     now.Sub(s.started).Seconds(),
		Bytes:    delta,
		RateMBps: rate,
		RateMbps: rate * 8,
	})
	// 峰值平滑：把本窗口速率加入滑动窗口（窗口数=1s/window），
	// 每加入一个窗口就重算该窗口内平均，峰值取“最近 1s 滑动平均”的最大值。
	if s.window > 0 {
		s.rateWin = append(s.rateWin, rate)
		winN := int((time.Second + s.window - 1) / s.window) // 约 1s 的窗口数
		if len(s.rateWin) > winN {
			s.rateWin = s.rateWin[len(s.rateWin)-winN:]
		}
		avg := avgOf(s.rateWin)
		if avg > s.peak {
			s.peak = avg
		}
	} else if rate > s.peak {
		s.peak = rate
	}
	s.last = s.bytes
	s.lastTime = now
}

func avgOf(vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	sum := 0.0
	for _, v := range vals {
		sum += v
	}
	return sum / float64(len(vals))
}

// Elapsed 返回已运行秒数。
func (s *Sampler) Elapsed() float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return time.Since(s.started).Seconds()
}

// Result 计算最终统计。
func (s *Sampler) Result() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	elapsed := time.Since(s.started).Seconds()
	avg := float64(s.bytes) / (1024 * 1024)
	if elapsed > 0 {
		avg = avg / elapsed
	}
	return Stats{
		TotalBytes: s.bytes,
		Duration:   elapsed,
		AvgMBps:    avg,
		AvgMbps:    avg * 8,
		PeakMBps:   s.peak,
		PeakMbps:   s.peak * 8,
	}
}

// Samples 导出采样序列（副本）。
func (s *Sampler) Samples() []Sample {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Sample, len(s.samples))
	copy(out, s.samples)
	return out
}

// Stats 一次测速的最终统计。
type Stats struct {
	TotalBytes     uint64  `json:"total_bytes"`
	SubmittedBytes uint64  `json:"submitted_bytes,omitempty"`
	QueuedBytes    uint64  `json:"queued_bytes,omitempty"`
	UpBytes        uint64  `json:"up_bytes,omitempty"`
	DownBytes      uint64  `json:"down_bytes,omitempty"`
	Truncated      bool    `json:"truncated,omitempty"`
	Duration       float64 `json:"duration"` // 实际秒数
	AvgMBps        float64 `json:"avg_mbps"` // 平均 MB/s
	AvgMbps        float64 `json:"avg_mbitps"`
	PeakMBps       float64 `json:"peak_mbps"` // 峰值 MB/s
	PeakMbps       float64 `json:"peak_mbitps"`
	Packets        uint64  `json:"packets"`   // UDP 发送/接收包数
	Lost           uint64  `json:"lost"`      // UDP 丢失包数
	LostPct        float64 `json:"lost_pct"`  // 丢包率 %
	Jitter         float64 `json:"jitter_ms"` // 抖动 ms
}

// UDPStats 记录 UDP 丢包/抖动统计。
type UDPStats struct {
	Packets uint64
	Lost    uint64
	Jitter  float64 // ms
}

// LostPct 计算丢包率百分比。
func (u UDPStats) LostPct() float64 {
	if u.Packets == 0 {
		return 0
	}
	return float64(u.Lost) / float64(u.Packets) * 100
}

// NewZeroBuffer 返回一个全零填充的字节切片（“写满的空包”），长度由调用方保证。
// 通过 make 一次性分配，避免每帧重复编码。
func NewZeroBuffer(n int) []byte {
	return make([]byte, n)
}
