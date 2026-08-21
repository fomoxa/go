package cyclone

import (
	"net"
	"testing"
	"time"
)

type fakeTransport struct {
	kind       Kind
	sent       [][]byte
	inbox      [][]byte
	blocked    bool
	closed     bool
	failing    bool
	sendLimit  int
	closeSends int
	hardCloses int
	peer       *fakeTransport
}

func newFake(kind Kind) *fakeTransport { return &fakeTransport{kind: kind} }

func pipe(kind Kind) (*fakeTransport, *fakeTransport) {
	a, b := newFake(kind), newFake(kind)
	a.peer, b.peer = b, a
	return a, b
}

func (f *fakeTransport) Kind() Kind { return f.kind }

func (f *fakeTransport) Send(b []byte) Status {
	if f.closed {
		return StatusClosed
	}
	if f.failing {
		return StatusError
	}
	if f.sendLimit > 0 && len(b) > f.sendLimit {
		return StatusTooLarge
	}
	if f.blocked {
		return StatusPending
	}
	chunk := append([]byte(nil), b...)
	f.sent = append(f.sent, chunk)
	if f.peer != nil {
		f.peer.inbox = append(f.peer.inbox, chunk)
	}
	return StatusOK
}

func (f *fakeTransport) Receive(buf []byte) (int, int, Status) {
	if len(f.inbox) == 0 {
		if f.closed {
			return 0, 0, StatusClosed
		}
		return 0, 0, StatusPending
	}
	packet := f.inbox[0]
	if len(packet) > len(buf) {
		return 0, len(packet), StatusTooSmall
	}
	copy(buf, packet)
	f.inbox = f.inbox[1:]
	return len(packet), 0, StatusOK
}

func (f *fakeTransport) CloseSend() { f.closeSends++ }

func (f *fakeTransport) Close() {
	f.closed = true
	f.hardCloses++
}

func (f *fakeTransport) deliver(b ...[]byte) {
	for _, chunk := range b {
		f.inbox = append(f.inbox, append([]byte(nil), chunk...))
	}
}

func (f *fakeTransport) sentBytes() []byte {
	var out []byte
	for _, chunk := range f.sent {
		out = append(out, chunk...)
	}
	return out
}

func (f *fakeTransport) sentFrames(t *testing.T) []frame {
	t.Helper()
	var d streamDecoder
	d.feed(f.sentBytes())
	var frames []frame
	for {
		fr, ok, err := d.next()
		if err != nil {
			t.Fatalf("a frame the implementation itself wrote does not decode: %v", err)
		}
		if !ok {
			return frames
		}
		frames = append(frames, fr)
	}
}

func (f *fakeTransport) lastHandshakePayload(t *testing.T) []byte {
	t.Helper()
	var payload []byte
	for _, fr := range f.sentFrames(t) {
		if fr.typ == FrameHandshake {
			payload = append([]byte(nil), fr.payload...)
		}
	}
	if payload == nil {
		t.Fatal("no handshake frame was written")
	}
	return payload
}

type fakeListener struct {
	arrivals []Transport
	forgot   []Transport
	closed   bool
}

func (l *fakeListener) poll() ([]Transport, error) {
	out := l.arrivals
	l.arrivals = nil
	return out, nil
}

func (l *fakeListener) forget(t Transport) { l.forgot = append(l.forgot, t) }

func (l *fakeListener) addr() net.Addr { return nil }

func (l *fakeListener) close() { l.closed = true }

func msg(id uint32, prefixes ...uint64) Message {
	m := Message{ID: id, Prefixes: prefixes}
	if len(prefixes) > 0 {
		m.Fingerprint = prefixes[len(prefixes)-1]
	}
	return m
}

func schemaOf(t *testing.T, fingerprint uint64, messages ...Message) *Schema {
	t.Helper()
	s, err := NewSchema(fingerprint, messages)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

var epoch = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func at(d time.Duration) time.Time { return epoch.Add(d) }

func kindOf(events []Event, kind EventKind) (Event, bool) {
	for _, e := range events {
		if e.Kind == kind {
			return e, true
		}
	}
	return Event{}, false
}

func mustEvent(t *testing.T, events []Event, kind EventKind) Event {
	t.Helper()
	e, ok := kindOf(events, kind)
	if !ok {
		t.Fatalf("no %s event in %v", kind, kinds(events))
	}
	return e
}

func mustNotEvent(t *testing.T, events []Event, kind EventKind) {
	t.Helper()
	if _, ok := kindOf(events, kind); ok {
		t.Fatalf("unexpected %s event in %v", kind, kinds(events))
	}
}

func kinds(events []Event) []EventKind {
	out := make([]EventKind, len(events))
	for i, e := range events {
		out[i] = e.Kind
	}
	return out
}
