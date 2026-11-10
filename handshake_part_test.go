package fomoxa

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"
	"time"
)

func many(t *testing.T, count int) *Schema {
	t.Helper()
	messages := make([]Message, count)
	for i := range messages {
		id := uint32(i + 1)
		messages[i] = msg(id, uint64(id))
	}
	return schemaOf(t, 0xC0FFEE00+uint64(count), messages...)
}

func partClient(t *testing.T, schema *Schema, kind Kind) (*Conn, *fakeTransport) {
	t.Helper()
	wire := newFake(kind)
	conn, err := NewConn(wire, schema, DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	conn.Tick(at(0))
	return conn, wire
}

func partServer(t *testing.T, schema *Schema, cfg Config) (*Conn, *fakeTransport) {
	t.Helper()
	wire := newFake(KindPacket)
	conn := newServerConn(wire, schema, cfg.normalized(), 1)
	conn.Tick(at(0))
	return conn, wire
}

func helloParts(t *testing.T, schema *Schema) [][]byte {
	t.Helper()
	_, wire := partClient(t, schema, KindPacket)
	return wire.sent
}

func sentVerdict(t *testing.T, wire *fakeTransport) Verdict {
	t.Helper()
	if len(wire.sent) == 0 {
		t.Fatal("nothing was sent")
	}
	f, err := decodePacket(wire.sent[len(wire.sent)-1])
	if err != nil {
		t.Fatal(err)
	}
	if f.typ != FrameHandshake || len(f.payload) != 1 {
		t.Fatalf("last packet is %s with %d bytes, want a one-byte verdict", f.typ, len(f.payload))
	}
	return Verdict(f.payload[0])
}

func refusedWith(t *testing.T, schema *Schema, packets ...[]byte) {
	t.Helper()
	conn, wire := partServer(t, schema, DefaultConfig())
	wire.deliver(packets...)
	mustEvent(t, conn.Tick(at(0)), EventHandshakeFailed)
	if v := sentVerdict(t, wire); v != VerdictMalformed {
		t.Fatalf("verdict = %s, want %s", v, VerdictMalformed)
	}
}

func TestAHelloOf62MessagesLeavesAsOneHandshakeFrame(t *testing.T) {
	_, wire := partClient(t, many(t, 62), KindPacket)

	if len(wire.sent) != 1 {
		t.Fatalf("sent %d packets, want 1", len(wire.sent))
	}
	if FrameType(wire.sent[0][0]) != FrameHandshake {
		t.Fatalf("frame type is %s", FrameType(wire.sent[0][0]))
	}
	if got, want := len(wire.sent[0]), 5+16+14*62; got != want {
		t.Fatalf("hello frame is %d bytes, want %d", got, want)
	}
}

func TestAHelloOf63MessagesOnAPacketTransportLeavesInTwoParts(t *testing.T) {
	schema := many(t, 63)
	parts := helloParts(t, schema)

	if len(parts) != 2 {
		t.Fatalf("sent %d packets, want 2", len(parts))
	}
	first, second := parts[0], parts[1]
	if FrameType(first[0]) != FrameHandshakePart || FrameType(second[0]) != FrameHandshakePart {
		t.Fatalf("frame types are %s and %s", FrameType(first[0]), FrameType(second[0]))
	}
	if binary.LittleEndian.Uint16(first[1:3]) != 0 || binary.LittleEndian.Uint16(second[1:3]) != 1 {
		t.Fatal("part indexes are not 0 and 1")
	}
	if binary.LittleEndian.Uint16(first[3:5]) != 2 || binary.LittleEndian.Uint16(second[3:5]) != 2 {
		t.Fatal("part counts are not 2")
	}
	if binary.LittleEndian.Uint32(first[5:9]) != 891 || binary.LittleEndian.Uint32(second[5:9]) != 7 {
		t.Fatal("part lengths are not 891 and 7")
	}

	joined := append(append([]byte(nil), first[9:]...), second[9:]...)
	if !bytes.Equal(joined, encodeHello(schema)) {
		t.Fatal("the parts do not carry the hello unchanged")
	}
}

func TestEveryPartIsAtMost900BytesAndOnlyTheLastIsShort(t *testing.T) {
	parts := helloParts(t, many(t, 500))
	content := 16 + 14*500

	if got, want := len(parts), (content+890)/891; got != want {
		t.Fatalf("sent %d parts, want %d", got, want)
	}
	for i, part := range parts {
		if len(part) > 900 {
			t.Fatalf("part %d is %d bytes", i, len(part))
		}
		if i < len(parts)-1 && len(part) != 900 {
			t.Fatalf("part %d is %d bytes but is not the last", i, len(part))
		}
	}
}

func TestAStreamTransportSendsALargeHelloAsOneHandshakeFrame(t *testing.T) {
	_, wire := partClient(t, many(t, 200), KindStream)

	if len(wire.sent) != 1 {
		t.Fatalf("sent %d writes, want 1", len(wire.sent))
	}
	if FrameType(wire.sent[0][0]) != FrameHandshake {
		t.Fatalf("frame type is %s", FrameType(wire.sent[0][0]))
	}
	if got, want := len(wire.sent[0]), 5+16+14*200; got != want {
		t.Fatalf("hello frame is %d bytes, want %d", got, want)
	}
}

func TestAServerAssemblesAHelloSentInPartsAndAccepts(t *testing.T) {
	p := newPair(t, KindPacket, many(t, 200), many(t, 200))
	p.tickTimes(3)

	mustEvent(t, p.serverEvents, EventReady)
	mustEvent(t, p.clientEvents, EventReady)
}

func TestPartsThatArriveInReverseOrderAreAssembled(t *testing.T) {
	schema := many(t, 200)
	parts := helloParts(t, schema)
	conn, wire := partServer(t, schema, DefaultConfig())
	for i := len(parts) - 1; i >= 0; i-- {
		wire.deliver(parts[i])
	}

	mustEvent(t, conn.Tick(at(0)), EventReady)
	if v := sentVerdict(t, wire); v != VerdictAccept {
		t.Fatalf("verdict = %s", v)
	}
}

func TestARepeatedIdenticalPartIsIgnored(t *testing.T) {
	schema := many(t, 200)
	parts := helloParts(t, schema)
	conn, wire := partServer(t, schema, DefaultConfig())
	wire.deliver(parts[0], parts[0])
	wire.deliver(parts[1:]...)

	mustEvent(t, conn.Tick(at(0)), EventReady)
	if v := sentVerdict(t, wire); v != VerdictAccept {
		t.Fatalf("verdict = %s", v)
	}
}

func TestAPartThatDiffersFromAnEarlierCopyIsRefused(t *testing.T) {
	schema := many(t, 200)
	parts := helloParts(t, schema)
	altered := append([]byte(nil), parts[0]...)
	altered[20] ^= 0xFF

	refusedWith(t, schema, parts[0], altered)
}

func TestAPartWhoseCountChangesIsRefused(t *testing.T) {
	refusedWith(t, many(t, 200), encodePart(0, 3, make([]byte, 891)), encodePart(1, 4, make([]byte, 891)))
}

func TestAShortPartBeforeTheLastIsRefused(t *testing.T) {
	refusedWith(t, many(t, 200), encodePart(0, 3, make([]byte, 890)))
}

func TestAnEmptyLastPartIsRefused(t *testing.T) {
	refusedWith(t, many(t, 200), encodePart(0, 2, make([]byte, 891)), encodePart(1, 2, nil))
}

func TestAPartCountOfOneIsRefused(t *testing.T) {
	schema := many(t, 62)
	refusedWith(t, schema, encodePart(0, 1, encodeHello(schema)))
}

func TestAPartCountPastTheCeilingIsRefused(t *testing.T) {
	refusedWith(t, many(t, 200), encodePart(0, maxHandshakeParts+1, make([]byte, 891)))
}

func TestAPartIndexPastTheCountIsRefused(t *testing.T) {
	refusedWith(t, many(t, 200), encodePart(2, 2, make([]byte, 891)))
}

func TestAPartCarryingMoreThan891BytesIsDroppedAsABrokenPacket(t *testing.T) {
	schema := many(t, 200)
	conn, wire := partServer(t, schema, DefaultConfig())
	wire.deliver(encodePart(0, 2, make([]byte, 892)))

	events := conn.Tick(at(0))
	mustNotEvent(t, events, EventHandshakeFailed)
	if len(wire.sent) != 0 {
		t.Fatalf("sent %d packets, want none", len(wire.sent))
	}

	wire.deliver(helloParts(t, schema)...)
	mustEvent(t, conn.Tick(at(0)), EventReady)
	if v := sentVerdict(t, wire); v != VerdictAccept {
		t.Fatalf("verdict = %s", v)
	}
}

func TestAHandshakeFrameWhilePartsArePendingIsRefused(t *testing.T) {
	schema := many(t, 200)
	parts := helloParts(t, schema)
	whole, err := encodeHandshake(encodeHello(schema))
	if err != nil {
		t.Fatal(err)
	}

	refusedWith(t, schema, parts[0], whole)
}

func TestContentAssembledPast1MiBIsRefused(t *testing.T) {
	const entries = 74_900
	schema := many(t, 1)
	hello := make([]byte, 16+14*entries)
	binary.LittleEndian.PutUint32(hello[0:4], ProtocolVersion)
	binary.LittleEndian.PutUint64(hello[4:12], schema.Fingerprint())
	binary.LittleEndian.PutUint32(hello[12:16], entries)
	if len(hello) <= MaxHandshakePayload {
		t.Fatal("the hello is not past 1 MiB")
	}

	cfg := DefaultConfig()
	cfg.MaxFramesPerTick = 2048
	conn, wire := partServer(t, schema, cfg)
	parts := encodeParts(hello)
	if len(parts) > maxHandshakeParts {
		t.Fatalf("%d parts is past the ceiling of %d", len(parts), maxHandshakeParts)
	}
	wire.deliver(parts...)

	mustEvent(t, conn.Tick(at(0)), EventHandshakeFailed)
	if v := sentVerdict(t, wire); v != VerdictMalformed {
		t.Fatalf("verdict = %s", v)
	}
}

func TestAPartAfterTheHandshakeIsDroppedAndIsNotActivity(t *testing.T) {
	schema := many(t, 1)
	conn, wire := partServer(t, schema, DefaultConfig())
	whole, err := encodeHandshake(encodeHello(schema))
	if err != nil {
		t.Fatal(err)
	}
	wire.deliver(whole)
	mustEvent(t, conn.Tick(at(0)), EventReady)
	wire.sent = nil

	interval := DefaultConfig().HeartbeatInterval
	wire.deliver(encodePart(0, 2, make([]byte, 891)))
	if events := conn.Tick(at(interval - time.Millisecond)); len(events) != 0 {
		t.Fatalf("events %v, want none", kinds(events))
	}
	if len(wire.sent) != 0 {
		t.Fatalf("sent %d packets, want none", len(wire.sent))
	}
	if conn.State() != StateReady {
		t.Fatalf("state is %s", conn.State())
	}

	conn.Tick(at(interval))
	if len(wire.sent) != 1 || FrameType(wire.sent[0][0]) != FrameProbe {
		t.Fatal("no probe after the silence window, so the dropped part counted as activity")
	}
}

func TestAPartOnAByteStreamIsAFatalFramingError(t *testing.T) {
	wire := newFake(KindStream)
	conn := newServerConn(wire, many(t, 1), DefaultConfig().normalized(), 1)
	conn.Tick(at(0))
	wire.deliver(encodePart(0, 2, make([]byte, 891)))

	e := mustEvent(t, conn.Tick(at(0)), EventDisconnected)
	var frameErr *FrameError
	if !errors.As(e.Err, &frameErr) {
		t.Fatalf("disconnect reason is %v, want a framing error", e.Err)
	}
}

func TestTheClientFailsOnABrokenAnswerSentInParts(t *testing.T) {
	conn, wire := partClient(t, many(t, 1), KindPacket)
	wire.deliver(encodePart(0, 2, make([]byte, 891)), encodePart(1, 2, nil))

	e := mustEvent(t, conn.Tick(at(0)), EventHandshakeFailed)
	if e.Verdict != VerdictMalformed {
		t.Fatalf("verdict = %s, want %s", e.Verdict, VerdictMalformed)
	}
}

func TestAQueryAndItsReplyThatExceedTheBudgetBothTravelInParts(t *testing.T) {
	const messages = 160
	clientMessages := make([]Message, messages)
	serverMessages := make([]Message, messages)
	for i := range clientMessages {
		id := uint32(i + 1)
		clientMessages[i] = msg(id, uint64(id)*10, uint64(id)*10+1)
		serverMessages[i] = msg(id, uint64(id)*10)
	}
	client, clientWire := partClient(t, schemaOf(t, 1, clientMessages...), KindPacket)
	server, serverWire := partServer(t, schemaOf(t, 2, serverMessages...), DefaultConfig())

	serverWire.deliver(clientWire.sent...)
	clientWire.sent = nil
	server.Tick(at(0))
	query := serverWire.sent
	serverWire.sent = nil
	if len(query) != 2 || FrameType(query[0][0]) != FrameHandshakePart {
		t.Fatalf("query left in %d packets, want 2 parts", len(query))
	}

	clientWire.deliver(query...)
	client.Tick(at(0))
	reply := clientWire.sent
	if len(reply) != 3 || FrameType(reply[0][0]) != FrameHandshakePart {
		t.Fatalf("reply left in %d packets, want 3 parts", len(reply))
	}

	serverWire.deliver(reply...)
	mustEvent(t, server.Tick(at(0)), EventReady)
	if v := sentVerdict(t, serverWire); v != VerdictAccept {
		t.Fatalf("verdict = %s", v)
	}
}

func TestPartsHeldBackByABlockedSocketLeaveLaterAsSeparatePackets(t *testing.T) {
	wire := newFake(KindPacket)
	wire.blocked = true
	conn, err := NewConn(wire, many(t, 200), DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	conn.Tick(at(0))
	if len(wire.sent) != 0 {
		t.Fatalf("sent %d packets while blocked", len(wire.sent))
	}
	if len(conn.outbound) == 0 {
		t.Fatal("the parts are not waiting")
	}

	wire.blocked = false
	conn.Tick(at(0))
	if got, want := len(wire.sent), (16+14*200+890)/891; got != want {
		t.Fatalf("sent %d packets, want %d", got, want)
	}
	for i, part := range wire.sent {
		if FrameType(part[0]) != FrameHandshakePart {
			t.Fatalf("packet %d is %s", i, FrameType(part[0]))
		}
		if got, want := len(part), 9+int(binary.LittleEndian.Uint32(part[5:9])); got != want {
			t.Fatalf("packet %d is %d bytes, want exactly one part of %d", i, got, want)
		}
	}
	if len(conn.outbound) != 0 {
		t.Fatal("parts are still waiting")
	}
}

func TestAHelloLargerThan64KiBHeldByABlockedSocketDoesNotHitTheCeiling(t *testing.T) {
	if 16+14*5000 <= maxOutboundBytes {
		t.Fatal("the hello is not larger than the outbound ceiling")
	}
	wire := newFake(KindPacket)
	wire.blocked = true
	conn, err := NewConn(wire, many(t, 5000), DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	mustNotEvent(t, conn.Tick(at(0)), EventDisconnected)

	wire.blocked = false
	conn.Tick(at(0))
	if got, want := len(wire.sent), (16+14*5000+890)/891; got != want {
		t.Fatalf("sent %d parts, want %d", got, want)
	}
}

func TestAPendingPartCountsAsActivityForTheServer(t *testing.T) {
	schema := many(t, 200)
	parts := helloParts(t, schema)
	conn, wire := partServer(t, schema, DefaultConfig())

	almost := DefaultConfig().HandshakeTimeout - time.Millisecond
	wire.deliver(parts[0])
	conn.Tick(at(almost))
	conn.Tick(at(almost + almost))
	if len(wire.sent) != 0 {
		t.Fatalf("sent %d packets, want no probe while parts keep the peer active", len(wire.sent))
	}
}

func TestAHandshakePartLayout(t *testing.T) {
	got := encodePart(3, 5, []byte{0xAA, 0xBB})
	want := []byte{0x04, 0x03, 0x00, 0x05, 0x00, 0x02, 0x00, 0x00, 0x00, 0xAA, 0xBB}
	if !bytes.Equal(got, want) {
		t.Fatalf("part is % X, want % X", got, want)
	}

	f, err := decodePacket(got)
	if err != nil {
		t.Fatal(err)
	}
	if f.typ != FrameHandshakePart || f.partIndex != 3 || f.partCount != 5 || !bytes.Equal(f.payload, []byte{0xAA, 0xBB}) {
		t.Fatalf("decoded %+v", f)
	}
}
