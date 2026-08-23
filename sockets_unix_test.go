//go:build unix

package fomoxa

import (
	"bytes"
	"testing"
	"time"
)

type live struct {
	t            *testing.T
	server       *Server
	client       *Conn
	serverEvents []Event
	clientEvents []Event
}

func (l *live) pump(until time.Duration, stop func() bool) {
	l.t.Helper()
	deadline := time.Now().Add(until)
	for time.Now().Before(deadline) {
		now := time.Now()
		l.clientEvents = append(l.clientEvents, l.client.Tick(now)...)
		l.serverEvents = append(l.serverEvents, l.server.Tick(now)...)
		if stop != nil && stop() {
			return
		}
		time.Sleep(time.Millisecond)
	}
}

func (l *live) sawServer(kind EventKind) bool {
	_, ok := kindOf(l.serverEvents, kind)
	return ok
}

func (l *live) sawClient(kind EventKind) bool {
	_, ok := kindOf(l.clientEvents, kind)
	return ok
}

func (l *live) messageFrom(events []Event, want []byte) bool {
	for _, e := range events {
		if e.Kind == EventMessage && bytes.Equal(e.Payload, want) {
			return true
		}
	}
	return false
}

func liveTCP(t *testing.T, schema *Schema) *live {
	t.Helper()
	server, err := ListenTCP("127.0.0.1:0", schema, DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close() })

	client, err := DialTCP(server.Addr().String(), schema, DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })

	return &live{t: t, server: server, client: client}
}

func liveUDP(t *testing.T, schema *Schema) *live {
	t.Helper()
	server, err := ListenUDP("127.0.0.1:0", schema, DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close() })

	client, err := DialUDP(server.Addr().String(), schema, DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })

	return &live{t: t, server: server, client: client}
}

func TestLiveTCPSession(t *testing.T) {
	schema := schemaOf(t, 0x1111, msg(1, 0xAA, 0xBB))
	l := liveTCP(t, schema)

	l.pump(5*time.Second, func() bool { return l.sawClient(EventReady) && l.sawServer(EventReady) })
	if !l.sawClient(EventReady) || !l.sawServer(EventReady) {
		t.Fatalf("handshake never finished: client %v server %v", kinds(l.clientEvents), kinds(l.serverEvents))
	}

	payload := bytes.Repeat([]byte{0x37}, 200*1024)
	if err := l.client.Send(9, payload); err != nil {
		t.Fatal(err)
	}
	l.pump(5*time.Second, func() bool { return l.messageFrom(l.serverEvents, payload) })
	if !l.messageFrom(l.serverEvents, payload) {
		t.Fatal("a 200 KiB message never arrived whole")
	}

	peers := l.server.Peers()
	if len(peers) != 1 {
		t.Fatalf("server tracks %d peers", len(peers))
	}
	reply := []byte("from the server")
	if err := l.server.Send(peers[0], 10, reply); err != nil {
		t.Fatal(err)
	}
	l.pump(5*time.Second, func() bool { return l.messageFrom(l.clientEvents, reply) })
	if !l.messageFrom(l.clientEvents, reply) {
		t.Fatal("the server's reply never arrived")
	}
}

func TestLiveUDPSession(t *testing.T) {
	schema := schemaOf(t, 0x1111, msg(1, 0xAA, 0xBB))
	l := liveUDP(t, schema)

	l.pump(5*time.Second, func() bool { return l.sawClient(EventReady) && l.sawServer(EventReady) })
	if !l.sawClient(EventReady) || !l.sawServer(EventReady) {
		t.Fatalf("handshake never finished: client %v server %v", kinds(l.clientEvents), kinds(l.serverEvents))
	}

	payload := bytes.Repeat([]byte{0x91}, 1000)
	if err := l.client.Send(5, payload); err != nil {
		t.Fatal(err)
	}
	l.pump(5*time.Second, func() bool { return l.messageFrom(l.serverEvents, payload) })
	if !l.messageFrom(l.serverEvents, payload) {
		t.Fatal("a datagram message never arrived")
	}

	peers := l.server.Peers()
	if len(peers) != 1 {
		t.Fatalf("server tracks %d peers", len(peers))
	}
	reply := []byte("datagram back")
	if err := l.server.Send(peers[0], 6, reply); err != nil {
		t.Fatal(err)
	}
	l.pump(5*time.Second, func() bool { return l.messageFrom(l.clientEvents, reply) })
	if !l.messageFrom(l.clientEvents, reply) {
		t.Fatal("the server's datagram never arrived")
	}
}

func TestLiveTCPRefusesAConflictingSchema(t *testing.T) {
	server, err := ListenTCP("127.0.0.1:0", schemaOf(t, 0x1111, msg(1, 0xAA, 0xBB)), DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	client, err := DialTCP(server.Addr().String(), schemaOf(t, 0x2222, msg(1, 0xAA, 0xCC)), DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	l := &live{t: t, server: server, client: client}
	l.pump(5*time.Second, func() bool { return l.sawClient(EventHandshakeFailed) })

	e, ok := kindOf(l.clientEvents, EventHandshakeFailed)
	if !ok {
		t.Fatalf("client events were %v", kinds(l.clientEvents))
	}
	if e.Verdict != VerdictConflict {
		t.Fatalf("client saw verdict %s, want a schema conflict", e.Verdict)
	}
}

func TestLiveTCPNoticesTheServerGoingAway(t *testing.T) {
	schema := schemaOf(t, 0x1111, msg(1, 0xAA))
	l := liveTCP(t, schema)
	l.pump(5*time.Second, func() bool { return l.sawClient(EventReady) })
	if !l.sawClient(EventReady) {
		t.Fatal("handshake never finished")
	}

	peers := l.server.Peers()
	if len(peers) != 1 {
		t.Fatalf("server tracks %d peers", len(peers))
	}
	l.server.Disconnect(peers[0])

	l.pump(5*time.Second, func() bool { return l.sawClient(EventDisconnected) })
	if !l.sawClient(EventDisconnected) {
		t.Fatalf("client never noticed: %v", kinds(l.clientEvents))
	}
}

func TestLiveUDPIgnoresStrangers(t *testing.T) {
	schema := schemaOf(t, 0x1111, msg(1, 0xAA))
	l := liveUDP(t, schema)
	l.pump(5*time.Second, func() bool { return l.sawClient(EventReady) && l.sawServer(EventReady) })
	if !l.sawServer(EventReady) {
		t.Fatal("handshake never finished")
	}

	stranger, err := DialUDP(l.server.Addr().String(), schema, DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer stranger.Close()

	strangerEvents := []Event{}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		now := time.Now()
		strangerEvents = append(strangerEvents, stranger.Tick(now)...)
		l.clientEvents = append(l.clientEvents, l.client.Tick(now)...)
		l.serverEvents = append(l.serverEvents, l.server.Tick(now)...)
		if _, ok := kindOf(strangerEvents, EventReady); ok {
			break
		}
		time.Sleep(time.Millisecond)
	}

	if _, ok := kindOf(strangerEvents, EventReady); !ok {
		t.Fatalf("the second client never got its own session: %v", kinds(strangerEvents))
	}
	if len(l.server.Peers()) != 2 {
		t.Fatalf("server tracks %d peers, want one per source address", len(l.server.Peers()))
	}
	if _, ok := kindOf(l.clientEvents, EventDisconnected); ok {
		t.Fatal("the first session was disturbed by the second client")
	}
}
