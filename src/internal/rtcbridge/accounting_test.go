package rtcbridge

import (
	"testing"
	"time"
)

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
