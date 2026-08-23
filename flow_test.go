package fomoxa

import (
	"bytes"
	"errors"
	"testing"
	"time"
)

func TestConnectedIsAlwaysTheFirstEvent(t *testing.T) {
	h := newClientHarness(t, schemaOf(t, 0x1111, msg(1, 0xAA)))
	if len(h.events) == 0 || h.events[0].Kind != EventConnected {
		t.Fatalf("first events are %v, want connected first", kinds(h.events))
	}
	if _, ok := kindOf(h.tick(time.Second), EventConnected); ok {
		t.Fatal("connected was reported twice")
	}
}

func TestSendBeforeReadyIsRefused(t *testing.T) {
	h := newClientHarness(t, schemaOf(t, 0x1111, msg(1, 0xAA)))
	if err := h.conn.Send(1, []byte("early")); !errors.Is(err, ErrNotReady) {
		t.Fatalf("send before the verdict returned %v, want ErrNotReady", err)
	}
	for _, f := range h.wire.sentFrames(t) {
		if f.typ == FrameData {
			t.Fatal("an application frame reached the wire before the handshake finished")
		}
	}
}

func TestDataBeforeReadyIsDropped(t *testing.T) {
	h := newClientHarness(t, schemaOf(t, 0x1111, msg(1, 0xAA)))
	data, _ := encodeData(1, []byte("too early"))
	h.wire.deliver(data)
	mustNotEvent(t, h.tick(time.Second), EventMessage)
}

func TestBlockedTransportHoldsTheFrameAndResendsItOnce(t *testing.T) {
	h := readyClient(t, DefaultConfig())
	h.wire.blocked = true

	if err := h.conn.Send(7, []byte("first")); err != nil {
		t.Fatalf("send while blocked returned %v, want success with the frame held", err)
	}
	if len(h.wire.sent) != 0 {
		t.Fatal("a blocked transport still received bytes")
	}
	if err := h.conn.Send(8, []byte("second")); !errors.Is(err, ErrCongested) {
		t.Fatalf("second send returned %v, want ErrCongested", err)
	}

	h.tick(time.Second)
	if len(h.wire.sent) != 0 {
		t.Fatal("the frame went out while the transport was still blocked")
	}

	h.wire.blocked = false
	h.tick(2 * time.Second)

	frames := h.wire.sentFrames(t)
	if len(frames) != 1 || frames[0].typ != FrameData || frames[0].messageID != 7 {
		t.Fatalf("wire carries %d frames: %+v", len(frames), frames)
	}
	if string(frames[0].payload) != "first" {
		t.Fatalf("payload is %q", frames[0].payload)
	}

	if err := h.conn.Send(9, []byte("third")); err != nil {
		t.Fatalf("send after the queue drained returned %v", err)
	}
}

func TestOversizedFrameDoesNotKillTheSession(t *testing.T) {
	h := readyClient(t, DefaultConfig())
	h.wire.sendLimit = 16

	err := h.conn.Send(1, bytes.Repeat([]byte{0xAA}, 64))
	if !errors.Is(err, ErrTransportLimit) {
		t.Fatalf("send returned %v, want ErrTransportLimit", err)
	}
	if len(h.wire.sent) != 0 {
		t.Fatal("bytes reached a transport that refused the frame")
	}

	events := h.tick(time.Second)
	mustNotEvent(t, events, EventDisconnected)
	if h.conn.State() != StateReady {
		t.Fatalf("session is %s, want still ready", h.conn.State())
	}

	if err := h.conn.Send(1, []byte("small")); err != nil {
		t.Fatalf("a following small message returned %v", err)
	}
}

func TestPayloadOverTheProtocolLimitIsRefused(t *testing.T) {
	h := readyClient(t, DefaultConfig())
	err := h.conn.Send(1, make([]byte, MaxMessagePayload+1))
	if !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("send returned %v, want ErrPayloadTooLarge", err)
	}
}

func TestTooSmallBufferGrowsAndKeepsThePacket(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ReadBufferSize = 4

	wire := newFake(KindPacket)
	conn, err := NewConn(wire, schemaOf(t, 0x1111, msg(1, 0xAA)), cfg)
	if err != nil {
		t.Fatal(err)
	}
	h := &harness{t: t, conn: conn, wire: wire}
	h.tick(0)
	h.handshake([]byte{0}, 0)

	payload := bytes.Repeat([]byte{0x5A}, 200)
	data, _ := encodeData(3, payload)
	wire.deliver(data)

	e := mustEvent(t, h.tick(time.Second), EventMessage)
	if e.MessageID != 3 || !bytes.Equal(e.Payload, payload) {
		t.Fatalf("message %d of %d bytes survived the resize badly", e.MessageID, len(e.Payload))
	}
}

func TestDrainStopsAtTheBudget(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxFramesPerTick = 3
	h := readyClient(t, cfg)

	for i := 0; i < 10; i++ {
		data, _ := encodeData(uint32(i), []byte{byte(i)})
		h.wire.deliver(data)
	}

	seen := 0
	first := h.tick(time.Second)
	for _, e := range first {
		if e.Kind == EventMessage {
			seen++
		}
	}
	if seen != 3 {
		t.Fatalf("%d messages in the first tick, want the budget of 3", seen)
	}

	for tickAt := 2; seen < 10 && tickAt < 20; tickAt++ {
		for _, e := range h.tick(time.Duration(tickAt) * time.Second) {
			if e.Kind == EventMessage {
				if e.MessageID != uint32(seen) {
					t.Fatalf("message %d arrived where %d was expected: order was not kept", e.MessageID, seen)
				}
				seen++
			}
		}
	}
	if seen != 10 {
		t.Fatalf("%d of 10 messages arrived", seen)
	}
}

func TestStreamViolationEndsTheSession(t *testing.T) {
	h := readyClient(t, DefaultConfig())
	h.wire.deliver([]byte{0x09, 0x09, 0x09})

	events := h.tick(time.Second)
	e := mustEvent(t, events, EventDisconnected)
	if e.Err == nil {
		t.Fatal("the disconnect event carries no reason")
	}
}

func TestPacketViolationOnlyDropsThatPacket(t *testing.T) {
	wire := newFake(KindPacket)
	conn, err := NewConn(wire, schemaOf(t, 0x1111, msg(1, 0xAA)), DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	h := &harness{t: t, conn: conn, wire: wire}
	h.tick(0)
	h.handshake([]byte{0}, 0)

	good, _ := encodeData(4, []byte("kept"))
	wire.deliver([]byte{0x09, 0x09})
	wire.deliver(append(append([]byte(nil), good...), 0xFF))
	wire.deliver(good)

	events := h.tick(time.Second)
	mustNotEvent(t, events, EventDisconnected)
	e := mustEvent(t, events, EventMessage)
	if string(e.Payload) != "kept" {
		t.Fatalf("payload is %q", e.Payload)
	}
}

func TestOnlyOneTerminalEventPerSession(t *testing.T) {
	server := schemaOf(t, 0x1111, msg(1, 0xAA, 0xBB))
	client := schemaOf(t, 0x2222, msg(1, 0xAA, 0xCC))

	h := judge(t, server, client)
	mustEvent(t, h.events, EventHandshakeFailed)

	h.wire.closed = true
	for elapsed := time.Second; elapsed < 60*time.Second; elapsed += time.Second {
		mustNotEvent(t, h.tick(elapsed), EventDisconnected)
	}

	terminal := 0
	for _, e := range h.events {
		if e.Kind == EventHandshakeFailed || e.Kind == EventDisconnected {
			terminal++
		}
	}
	if terminal != 1 {
		t.Fatalf("%d terminal events, want exactly one", terminal)
	}
}

func TestRefusalIsWrittenBeforeTheTransportIsToldToClose(t *testing.T) {
	server := schemaOf(t, 0x1111, msg(1, 0xAA, 0xBB))
	client := schemaOf(t, 0x2222, msg(1, 0xAA, 0xCC))

	h := judge(t, server, client)
	if v := h.verdict(); v != VerdictConflict {
		t.Fatalf("verdict %s", v)
	}
	if h.wire.closeSends == 0 {
		t.Fatal("the transport was never told to close after the refusal")
	}
}

func TestClosingProducesNoFurtherEvents(t *testing.T) {
	h := readyClient(t, DefaultConfig())
	if err := h.conn.Close(); err != nil {
		t.Fatal(err)
	}
	if h.wire.hardCloses == 0 {
		t.Fatal("closing the session left the transport open")
	}

	data, _ := encodeData(1, []byte("late"))
	h.wire.deliver(data)
	for elapsed := time.Second; elapsed < 60*time.Second; elapsed += time.Second {
		if events := h.tick(elapsed); len(events) != 0 {
			t.Fatalf("a closed session still produced %v", kinds(events))
		}
	}
	if err := h.conn.Send(1, []byte("x")); !errors.Is(err, ErrSessionClosed) {
		t.Fatalf("send on a closed session returned %v", err)
	}
}

func TestTransportDeathReportsDisconnected(t *testing.T) {
	h := readyClient(t, DefaultConfig())
	h.wire.closed = true
	events := h.tick(time.Second)
	mustEvent(t, events, EventDisconnected)
	if h.conn.State() != StateClosed {
		t.Fatalf("session is %s, want closed", h.conn.State())
	}
}

func TestPayloadOutlivesTheTick(t *testing.T) {
	h := readyClient(t, DefaultConfig())
	first, _ := encodeData(1, []byte("first payload"))
	h.wire.deliver(first)

	e := mustEvent(t, h.tick(time.Second), EventMessage)
	kept := e.Payload

	for i := 0; i < 5; i++ {
		other, _ := encodeData(2, bytes.Repeat([]byte{0xFF}, 64))
		h.wire.deliver(other)
		h.tick(time.Duration(2+i) * time.Second)
	}
	if string(kept) != "first payload" {
		t.Fatalf("the payload handed to the application became %q", kept)
	}
}

// 02 §8: the pending queue must have a ceiling. A peer that probes every tick
// while never reading keeps our silence clock alive, so the heartbeat never
// ends the session - only the ceiling does.
func TestABlockedTransportAndAProbingPeerStopAtTheOutboundCeiling(t *testing.T) {
	h := readyClient(t, DefaultConfig())
	h.wire.blocked = true

	ended := false
	for i := 0; i < 200000 && !ended; i++ {
		h.wire.deliver(probeFrame)
		if _, ok := kindOf(h.tick(time.Millisecond), EventDisconnected); ok {
			ended = true
		}
	}

	if !ended {
		t.Fatal("the ceiling never ended the session, so the queue grew without bound")
	}
	if got := len(h.conn.outbound); got == 0 {
		t.Fatal("the outbound queue was emptied rather than capped")
	}
}

// 01 §6: the UDP receive queue is bounded and the oldest datagram is the one
// that goes, so the freshest data survives a burst.
func TestUDPQueueDropsTheOldestDatagram(t *testing.T) {
	cfg := DefaultConfig()
	peer := &udpPeer{}
	for i := 0; i < cfg.MaxPeerBacklog+5; i++ {
		if len(peer.inbox) >= cfg.MaxPeerBacklog {
			peer.inbox = peer.inbox[1:]
		}
		peer.inbox = append(peer.inbox, []byte{byte(i)})
	}

	if len(peer.inbox) != cfg.MaxPeerBacklog {
		t.Fatalf("queue holds %d datagrams, want %d", len(peer.inbox), cfg.MaxPeerBacklog)
	}
	if got := peer.inbox[0][0]; got != 5 {
		t.Fatalf("oldest surviving datagram is %d, want 5 - the first five should have been dropped", got)
	}
}
