package fomoxa

import (
	"testing"
	"time"
)

func readyClient(t *testing.T, cfg Config) *harness {
	t.Helper()
	wire := newFake(KindStream)
	conn, err := NewConn(wire, schemaOf(t, 0x1111, msg(1, 0xAA)), cfg)
	if err != nil {
		t.Fatal(err)
	}
	h := &harness{t: t, conn: conn, wire: wire}
	h.tick(0)
	h.handshake([]byte{0}, 0)
	if h.conn.State() != StateReady {
		t.Fatalf("client is %s, want ready", h.conn.State())
	}
	h.wire.sent = nil
	return h
}

func (h *harness) probesSent() int {
	h.t.Helper()
	count := 0
	for _, f := range h.wire.sentFrames(h.t) {
		if f.typ == FrameProbe {
			count++
		}
	}
	return count
}

func TestTrafficSuppressesProbes(t *testing.T) {
	h := readyClient(t, DefaultConfig())
	data, _ := encodeData(1, []byte("x"))

	for elapsed := time.Second; elapsed < 60*time.Second; elapsed += time.Second {
		h.wire.deliver(data)
		h.tick(elapsed)
	}
	if n := h.probesSent(); n != 0 {
		t.Fatalf("%d probes were sent while data kept arriving", n)
	}
	mustNotEvent(t, h.events, EventDisconnected)
}

func TestSilenceSendsExactlyOneProbe(t *testing.T) {
	h := readyClient(t, DefaultConfig())

	for elapsed := time.Second; elapsed <= 4*time.Second; elapsed += time.Second {
		h.tick(elapsed)
	}
	if n := h.probesSent(); n != 0 {
		t.Fatalf("%d probes were sent before the silence window closed", n)
	}

	for elapsed := 5 * time.Second; elapsed <= 12*time.Second; elapsed += time.Second {
		h.tick(elapsed)
	}
	if n := h.probesSent(); n != 1 {
		t.Fatalf("%d probes were sent, want exactly one for the cycle", n)
	}
}

func TestAckEndsTheProbeCycle(t *testing.T) {
	h := readyClient(t, DefaultConfig())
	h.tick(5 * time.Second)
	if n := h.probesSent(); n != 1 {
		t.Fatalf("%d probes after the silence window", n)
	}

	h.wire.deliver(ackFrame)
	events := h.tick(6 * time.Second)
	mustEvent(t, events, EventAck)

	for elapsed := 7 * time.Second; elapsed <= 10*time.Second; elapsed += time.Second {
		h.tick(elapsed)
	}
	if n := h.probesSent(); n != 1 {
		t.Fatalf("%d probes, want the cycle to have restarted from the ack", n)
	}

	h.tick(11 * time.Second)
	if n := h.probesSent(); n != 2 {
		t.Fatalf("%d probes, want a second probe five seconds after the ack", n)
	}
	mustNotEvent(t, h.events, EventDisconnected)
}

func TestAnyFrameEndsTheProbeCycle(t *testing.T) {
	h := readyClient(t, DefaultConfig())
	h.tick(5 * time.Second)

	data, _ := encodeData(1, []byte("still here"))
	h.wire.deliver(data)
	events := h.tick(6 * time.Second)
	mustEvent(t, events, EventMessage)

	for elapsed := 7 * time.Second; elapsed <= 21*time.Second; elapsed += time.Second {
		mustNotEvent(t, h.tick(elapsed), EventDisconnected)
	}
}

func TestNoAnswerDeclaresThePeerDead(t *testing.T) {
	h := readyClient(t, DefaultConfig())

	for elapsed := time.Second; elapsed < 20*time.Second; elapsed += time.Second {
		mustNotEvent(t, h.tick(elapsed), EventDisconnected)
	}
	events := h.tick(20 * time.Second)
	e := mustEvent(t, events, EventDisconnected)
	if e.Err == nil {
		t.Fatal("the disconnect event carries no reason")
	}
	if h.conn.State() != StateClosed {
		t.Fatalf("session is %s, want closed", h.conn.State())
	}
}

func TestDeathIsDetectedWithoutWaiting(t *testing.T) {
	started := time.Now()
	h := readyClient(t, DefaultConfig())
	h.tick(5 * time.Second)
	mustEvent(t, h.tick(20*time.Second), EventDisconnected)
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("a full expiry cycle took %s of real time; time must be a parameter", elapsed)
	}
}

func TestProbeIsAnsweredEvenBeforeReady(t *testing.T) {
	h := newClientHarness(t, schemaOf(t, 0x1111, msg(1, 0xAA)))
	h.wire.sent = nil

	h.wire.deliver(probeFrame)
	events := h.tick(time.Second)
	mustNotEvent(t, events, EventProbe)

	acks := 0
	for _, f := range h.wire.sentFrames(t) {
		if f.typ == FrameAck {
			acks++
		}
	}
	if acks != 1 {
		t.Fatalf("%d acks were written while handshaking, want exactly one", acks)
	}
}

func TestProbeWhileReadyIsAnsweredAndReported(t *testing.T) {
	h := readyClient(t, DefaultConfig())
	h.wire.deliver(probeFrame)
	events := h.tick(time.Second)
	mustEvent(t, events, EventProbe)

	acks := 0
	for _, f := range h.wire.sentFrames(t) {
		if f.typ == FrameAck {
			acks++
		}
	}
	if acks != 1 {
		t.Fatalf("%d acks, want exactly one", acks)
	}
}

func TestServerUsesTheHandshakeWindowUntilReady(t *testing.T) {
	cfg := DefaultConfig()
	cfg.HandshakeTimeout = 8 * time.Second
	cfg.HeartbeatInterval = 2 * time.Second
	cfg.HeartbeatTimeout = 30 * time.Second

	wire := newFake(KindStream)
	h := &harness{t: t, conn: newServerConn(wire, schemaOf(t, 0x1111, msg(1, 0xAA)), cfg.normalized(), 1), wire: wire}
	h.tick(0)

	for elapsed := time.Second; elapsed <= 7*time.Second; elapsed += time.Second {
		h.tick(elapsed)
	}
	if n := h.probesSent(); n != 0 {
		t.Fatalf("%d probes before the handshake window closed, want none", n)
	}
	h.tick(8 * time.Second)
	if n := h.probesSent(); n != 1 {
		t.Fatalf("%d probes at the handshake window, want one", n)
	}
}

func TestServerNarrowsTheWindowOnceReady(t *testing.T) {
	cfg := DefaultConfig()
	cfg.HandshakeTimeout = 8 * time.Second
	cfg.HeartbeatInterval = 2 * time.Second
	cfg.HeartbeatTimeout = 30 * time.Second

	wire := newFake(KindStream)
	h := &harness{t: t, conn: newServerConn(wire, schemaOf(t, 0x1111, msg(1, 0xAA)), cfg.normalized(), 1), wire: wire}
	h.tick(0)
	h.handshake(encodeHello(schemaOf(t, 0x1111, msg(1, 0xAA))), time.Second)
	if h.conn.State() != StateReady {
		t.Fatalf("peer is %s, want ready", h.conn.State())
	}
	h.wire.sent = nil

	h.tick(2 * time.Second)
	if n := h.probesSent(); n != 0 {
		t.Fatalf("%d probes one second after the hello", n)
	}
	h.tick(3 * time.Second)
	if n := h.probesSent(); n != 1 {
		t.Fatalf("%d probes two seconds after the hello, want one", n)
	}
}
