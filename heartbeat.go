package cyclone

import "time"

// heartbeat tracks Ping/Pong timeout state, using time.Now() (monotonic
// within a process) the same way the sibling SDKs use their own platform's
// monotonic clock rather than a wall-clock timestamp.
type heartbeat struct {
	interval time.Duration
	timeout  time.Duration
	lastPong time.Time
}

func newHeartbeat(interval, timeout time.Duration) *heartbeat {
	return &heartbeat{interval: interval, timeout: timeout, lastPong: time.Now()}
}

func (h *heartbeat) isTimeout() bool {
	return time.Since(h.lastPong) > h.timeout
}

func (h *heartbeat) shouldPing() bool {
	return time.Since(h.lastPong) >= h.interval
}

func (h *heartbeat) markPong() {
	h.lastPong = time.Now()
}
