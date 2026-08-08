package cyclone

import (
	"testing"
	"time"
)

func TestFreshHeartbeatDoesNotPingOrTimeOut(t *testing.T) {
	h := newHeartbeat(5*time.Second, 15*time.Second)
	if h.shouldPing() {
		t.Error("a fresh heartbeat should not want to ping yet")
	}
	if h.isTimeout() {
		t.Error("a fresh heartbeat should not be timed out")
	}
}

func TestShouldPingAfterTheIntervalElapses(t *testing.T) {
	h := newHeartbeat(20*time.Millisecond, 15*time.Second)
	time.Sleep(30 * time.Millisecond)
	if !h.shouldPing() {
		t.Error("expected shouldPing() to be true after the interval elapsed")
	}
	if h.isTimeout() {
		t.Error("should not be timed out yet")
	}
}

func TestTimesOutAfterTheTimeoutElapses(t *testing.T) {
	h := newHeartbeat(5*time.Millisecond, 20*time.Millisecond)
	time.Sleep(30 * time.Millisecond)
	if !h.isTimeout() {
		t.Error("expected isTimeout() to be true after the timeout elapsed")
	}
}

func TestMarkPongResetsBothChecks(t *testing.T) {
	h := newHeartbeat(5*time.Millisecond, 20*time.Millisecond)
	time.Sleep(30 * time.Millisecond)
	if !h.isTimeout() {
		t.Fatal("expected timeout before markPong")
	}
	h.markPong()
	if h.isTimeout() {
		t.Error("markPong should have reset the timeout")
	}
	if h.shouldPing() {
		t.Error("markPong should have reset shouldPing too")
	}
}
