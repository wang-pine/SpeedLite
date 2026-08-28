package engine

import (
	"encoding/json"
	"math"
	"testing"
	"time"
)

func TestStatsQueueFieldsJSON(t *testing.T) {
	data, err := json.Marshal(Stats{
		SubmittedBytes: 10,
		QueuedBytes:    4,
		UpBytes:        3,
		DownBytes:      6,
		Truncated:      true,
	})
	if err != nil {
		t.Fatalf("marshal stats: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal stats: %v", err)
	}
	want := map[string]any{
		"submitted_bytes": float64(10),
		"queued_bytes":    float64(4),
		"up_bytes":        float64(3),
		"down_bytes":      float64(6),
		"truncated":       true,
	}
	for key, value := range want {
		if got[key] != value {
			t.Fatalf("JSON field %q = %#v, want %#v; JSON=%s", key, got[key], value, data)
		}
	}
}

func TestParamsValidate(t *testing.T) {
	p := &Params{}
	if err := p.Validate(); err != nil {
		t.Fatalf("defaults should validate: %v", err)
	}
	if p.Streams != 4 || p.Duration != 10 || p.PacketLen != 131072 {
		t.Fatalf("defaults wrong: %+v", p)
	}
	if p.Mode != ModeTCP || p.Direction != DirUp {
		t.Fatalf("mode/dir defaults wrong: %+v", p)
	}

	if err := (&Params{Mode: "foo"}).Validate(); err == nil {
		t.Fatal("bad mode should fail")
	}
	if err := (&Params{Streams: 100}).Validate(); err == nil {
		t.Fatal("too many streams should fail")
	}
	if err := (&Params{Duration: 99999}).Validate(); err == nil {
		t.Fatal("too long duration should fail")
	}
}

func TestSamplerRate(t *testing.T) {
	s := NewSampler(100 * time.Millisecond)
	// 模拟 0.2s 内喂 2 MiB -> 期望平均 10 MB/s
	// 每毫秒喂 2MiB/200 = 10485 字节，200 次共 2MiB
	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) {
		s.Add(2 * 1024 * 1024 / 200)
		time.Sleep(time.Millisecond)
	}
	time.Sleep(150 * time.Millisecond) // 等最后一个窗口 + 采样间隔
	s.Tick()
	res := s.Result()
	if res.TotalBytes < 2*1024*1024*90/100 {
		t.Fatalf("expected ~2MiB, got %d", res.TotalBytes)
	}
	// 核心恒等式：avg = total / duration
	expectAvg := float64(res.TotalBytes) / (1024 * 1024) / res.Duration
	if math.Abs(res.AvgMBps-expectAvg) > 0.5 {
		t.Fatalf("avg %.2f != total/duration %.2f", res.AvgMBps, expectAvg)
	}
	if res.Duration <= 0 {
		t.Fatal("duration must be positive")
	}
	if res.PeakMBps <= 0 {
		t.Fatal("peak must be positive")
	}
	if len(s.Samples()) == 0 {
		t.Fatal("should have samples")
	}
}

func TestSamplerConcurrent(t *testing.T) {
	s := NewSampler(10 * time.Millisecond)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 10000; i++ {
			s.Add(1024)
		}
	}()
	for i := 0; i < 1000; i++ {
		s.Tick()
	}
	<-done
	if s.Bytes() != 10000*1024 {
		t.Fatalf("byte count wrong: %d", s.Bytes())
	}
}

func TestUDPStatsLostPct(t *testing.T) {
	u := UDPStats{Packets: 100, Lost: 10}
	if u.LostPct() != 10 {
		t.Fatalf("expected 10%%, got %v", u.LostPct())
	}
	if (UDPStats{}).LostPct() != 0 {
		t.Fatal("empty should be 0")
	}
}
