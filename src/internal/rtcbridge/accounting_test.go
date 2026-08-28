package rtcbridge

import (
	"errors"
	"net/url"
	"testing"
	"time"
)

func TestParseSignalParamsRejectsUnsafeValues(t *testing.T) {
	tests := []url.Values{
		{"dir": {"sideways"}},
		{"dir": {"down"}, "duration": {"999999"}},
		{"dir": {"down"}, "duration": {"NaN"}},
		{"dir": {"down"}, "duration": {"-1"}},
		{"dir": {"down"}, "packet_len": {"2147483647"}},
		{"dir": {"down"}, "packet_len": {"-1"}},
		{"dir": {"down"}, "duration": {"not-a-number"}},
	}
	for _, values := range tests {
		if _, err := parseSignalParams(values); err == nil {
			t.Fatalf("parseSignalParams(%v) succeeded, want error", values)
		}
	}
}

func TestParseSignalParamsAppliesSafeDefaults(t *testing.T) {
	p, err := parseSignalParams(url.Values{"dir": {"both"}})
	if err != nil {
		t.Fatal(err)
	}
	if p.Direction != "both" || p.Duration != 10 || p.PacketLen != 131072 {
		t.Fatalf("unexpected params: %+v", p)
	}
}

type failingDataChannel struct{}

func (failingDataChannel) Send([]byte) error      { return errors.New("send failed") }
func (failingDataChannel) BufferedAmount() uint64 { return 0 }

func TestSendDownPropagatesSendError(t *testing.T) {
	st := newRTCStats()
	err := sendDown(failingDataChannel{}, 1, 1024, st, make(chan struct{}))
	if err == nil || err.Error() != "send failed" {
		t.Fatalf("sendDown error = %v, want send failed", err)
	}
}

func TestDrainSnapshot(t *testing.T) {
	tests := []struct {
		name          string
		submitted     uint64
		buffered      uint64
		wantDrained   uint64
		wantQueued    uint64
		wantTruncated bool
	}{
		{name: "residual queue", submitted: 1000, buffered: 637, wantDrained: 363, wantQueued: 637, wantTruncated: true},
		{name: "fully drained", submitted: 1000, buffered: 0, wantDrained: 1000, wantQueued: 0, wantTruncated: false},
		{name: "buffer exceeds submitted", submitted: 1000, buffered: 1200, wantDrained: 0, wantQueued: 1000, wantTruncated: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			drained, queued, truncated := drainSnapshot(tt.submitted, tt.buffered)
			if drained != tt.wantDrained || queued != tt.wantQueued || truncated != tt.wantTruncated {
				t.Fatalf(
					"drainSnapshot(%d, %d) = (%d, %d, %v), want (%d, %d, %v)",
					tt.submitted, tt.buffered, drained, queued, truncated,
					tt.wantDrained, tt.wantQueued, tt.wantTruncated,
				)
			}
		})
	}
}

func TestWaitForDrain(t *testing.T) {
	values := []uint64{10, 10, 0}
	index := 0
	remaining := waitForDrain(func() uint64 {
		value := values[index]
		if index < len(values)-1 {
			index++
		}
		return value
	}, 100*time.Millisecond)
	if remaining != 0 {
		t.Fatalf("remaining after drain = %d, want 0", remaining)
	}

	remaining = waitForDrain(func() uint64 { return 7 }, 2*time.Millisecond)
	if remaining != 7 {
		t.Fatalf("remaining after timeout = %d, want 7", remaining)
	}
}
