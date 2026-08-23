package fomoxa

import (
	"bytes"
	"testing"
	"time"
)

type pair struct {
	t             *testing.T
	client        *Conn
	server        *Conn
	clientEvents  []Event
	serverEvents  []Event
	elapsedTicker time.Duration
}

func newPair(t *testing.T, kind Kind, clientSchema, serverSchema *Schema) *pair {
	t.Helper()
	a, b := pipe(kind)
	client, err := NewConn(a, clientSchema, DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	return &pair{
		t:      t,
		client: client,
		server: newServerConn(b, serverSchema, DefaultConfig().normalized(), 1),
	}
}

func (p *pair) tick() {
	p.t.Helper()
	p.elapsedTicker += 100 * time.Millisecond
	now := at(p.elapsedTicker)
	p.clientEvents = append(p.clientEvents, p.client.Tick(now)...)
	p.serverEvents = append(p.serverEvents, p.server.Tick(now)...)
}

func (p *pair) tickTimes(n int) {
	p.t.Helper()
	for i := 0; i < n; i++ {
		p.tick()
	}
}

func TestPipeSessionCompletes(t *testing.T) {
	for _, kind := range []Kind{KindStream, KindPacket} {
		t.Run(kind.String(), func(t *testing.T) {
			schema := schemaOf(t, 0x1111, msg(1, 0xAA, 0xBB))
			p := newPair(t, kind, schema, schema)
			p.tickTimes(4)

			mustEvent(t, p.clientEvents, EventReady)
			mustEvent(t, p.serverEvents, EventReady)

			payload := bytes.Repeat([]byte{0x42}, 300)
			if err := p.client.Send(1, payload); err != nil {
				t.Fatal(err)
			}
			p.tickTimes(2)
			e := mustEvent(t, p.serverEvents, EventMessage)
			if e.MessageID != 1 || !bytes.Equal(e.Payload, payload) {
				t.Fatalf("server received message %d of %d bytes", e.MessageID, len(e.Payload))
			}
			if e.Peer != 1 {
				t.Fatalf("event carries peer %d, want 1", e.Peer)
			}

			if err := p.server.Send(2, []byte("pong")); err != nil {
				t.Fatal(err)
			}
			p.tickTimes(2)
			back := mustEvent(t, p.clientEvents, EventMessage)
			if back.MessageID != 2 || string(back.Payload) != "pong" {
				t.Fatalf("client received %d %q", back.MessageID, back.Payload)
			}
		})
	}
}

func TestPipeSessionSurvivesAQueryRound(t *testing.T) {
	client := schemaOf(t, 0x1111, msg(1, 0xAA, 0xBB))
	server := schemaOf(t, 0x2222, msg(1, 0xAA))

	p := newPair(t, KindStream, client, server)
	p.tickTimes(6)

	mustEvent(t, p.clientEvents, EventReady)
	mustEvent(t, p.serverEvents, EventReady)
	mustNotEvent(t, p.clientEvents, EventHandshakeFailed)
}

func TestPipeSessionRefusesAConflict(t *testing.T) {
	client := schemaOf(t, 0x1111, msg(1, 0xAA, 0xBB))
	server := schemaOf(t, 0x2222, msg(1, 0xAA, 0xCC))

	p := newPair(t, KindStream, client, server)
	p.tickTimes(6)

	if e := mustEvent(t, p.clientEvents, EventHandshakeFailed); e.Verdict != VerdictConflict {
		t.Fatalf("client saw verdict %s", e.Verdict)
	}
	mustEvent(t, p.serverEvents, EventHandshakeFailed)
	mustNotEvent(t, p.clientEvents, EventReady)
}

func TestPipeHeartbeatCrossesTheWire(t *testing.T) {
	schema := schemaOf(t, 0x1111, msg(1, 0xAA))
	p := newPair(t, KindStream, schema, schema)
	p.tickTimes(4)
	mustEvent(t, p.clientEvents, EventReady)

	before := len(p.clientEvents)
	p.elapsedTicker = 6 * time.Second
	p.tickTimes(3)

	if _, ok := kindOf(p.serverEvents, EventProbe); !ok {
		if _, ok := kindOf(p.clientEvents[before:], EventProbe); !ok {
			t.Fatalf("no probe crossed the wire: client %v server %v",
				kinds(p.clientEvents[before:]), kinds(p.serverEvents))
		}
	}
	mustNotEvent(t, p.clientEvents, EventDisconnected)
	mustNotEvent(t, p.serverEvents, EventDisconnected)
}

func TestServerRunsManyPeers(t *testing.T) {
	schema := schemaOf(t, 0x1111, msg(1, 0xAA))
	l := &fakeListener{}
	server := newServer(l, schema, DefaultConfig().normalized())

	var clients []*Conn
	for i := 0; i < 3; i++ {
		a, b := pipe(KindStream)
		conn, err := NewConn(a, schema, DefaultConfig())
		if err != nil {
			t.Fatal(err)
		}
		clients = append(clients, conn)
		l.arrivals = append(l.arrivals, b)
	}

	var serverEvents []Event
	for i := 0; i < 4; i++ {
		now := at(time.Duration(i) * 100 * time.Millisecond)
		for _, c := range clients {
			c.Tick(now)
		}
		serverEvents = append(serverEvents, server.Tick(now)...)
	}

	ready := map[PeerID]bool{}
	for _, e := range serverEvents {
		if e.Kind == EventReady {
			ready[e.Peer] = true
		}
	}
	if len(ready) != 3 {
		t.Fatalf("%d peers became ready, want 3: %v", len(ready), serverEvents)
	}
	if len(server.Peers()) != 3 {
		t.Fatalf("server tracks %d peers", len(server.Peers()))
	}

	for peer := range ready {
		if err := server.Send(peer, 1, []byte("hi")); err != nil {
			t.Fatalf("send to peer %d: %v", peer, err)
		}
	}

	now := at(time.Second)
	server.Tick(now)
	for _, c := range clients {
		got := false
		for _, e := range c.Tick(now) {
			if e.Kind == EventMessage && string(e.Payload) == "hi" {
				got = true
			}
		}
		if !got {
			t.Fatal("a client never received the server's message")
		}
	}

	if err := server.Send(PeerID(99), 1, nil); err == nil {
		t.Fatal("sending to an unknown peer succeeded")
	}
}

func TestServerRetiresARefusedPeer(t *testing.T) {
	l := &fakeListener{}
	server := newServer(l, schemaOf(t, 0x1111, msg(1, 0xAA, 0xBB)), DefaultConfig().normalized())

	a, b := pipe(KindStream)
	client, err := NewConn(a, schemaOf(t, 0x2222, msg(1, 0xAA, 0xCC)), DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	l.arrivals = append(l.arrivals, b)

	var events []Event
	for i := 0; i < 4; i++ {
		now := at(time.Duration(i) * 100 * time.Millisecond)
		client.Tick(now)
		events = append(events, server.Tick(now)...)
	}

	mustEvent(t, events, EventHandshakeFailed)
	if len(server.Peers()) != 0 {
		t.Fatalf("server still tracks %v after refusing the peer", server.Peers())
	}
	if len(l.forgot) != 1 {
		t.Fatalf("the listener was told to forget %d transports, want 1", len(l.forgot))
	}
	if b.hardCloses == 0 {
		t.Fatal("the refused peer's transport was never released")
	}
}

func TestPeerConnServesACustomTransport(t *testing.T) {
	schema := schemaOf(t, 0x1111, msg(1, 0xAA, 0xBB))
	a, b := pipe(KindPacket)

	client, err := NewConn(a, schema, DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	peer, err := NewPeerConn(b, schema, DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}

	var clientEvents, peerEvents []Event
	for i := 0; i < 4; i++ {
		now := at(time.Duration(i) * 100 * time.Millisecond)
		clientEvents = append(clientEvents, client.Tick(now)...)
		peerEvents = append(peerEvents, peer.Tick(now)...)
	}

	mustEvent(t, clientEvents, EventReady)
	mustEvent(t, peerEvents, EventReady)

	if err := peer.Send(1, []byte("from a hand-built peer")); err != nil {
		t.Fatal(err)
	}
	now := at(time.Second)
	peer.Tick(now)
	e := mustEvent(t, client.Tick(now), EventMessage)
	if string(e.Payload) != "from a hand-built peer" {
		t.Fatalf("payload is %q", e.Payload)
	}
}
