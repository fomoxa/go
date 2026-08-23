package fomoxa

import (
	"encoding/binary"
	"testing"
	"time"
)

type harness struct {
	t      *testing.T
	conn   *Conn
	wire   *fakeTransport
	events []Event
}

func newServerHarness(t *testing.T, schema *Schema) *harness {
	t.Helper()
	wire := newFake(KindStream)
	h := &harness{t: t, conn: newServerConn(wire, schema, DefaultConfig().normalized(), 1), wire: wire}
	h.tick(0)
	return h
}

func newClientHarness(t *testing.T, schema *Schema) *harness {
	t.Helper()
	wire := newFake(KindStream)
	conn, err := NewConn(wire, schema, DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	h := &harness{t: t, conn: conn, wire: wire}
	h.tick(0)
	return h
}

func (h *harness) tick(d time.Duration) []Event {
	h.t.Helper()
	events := h.conn.Tick(at(d))
	h.events = append(h.events, events...)
	return events
}

func (h *harness) handshake(payload []byte, d time.Duration) []Event {
	h.t.Helper()
	f, err := encodeHandshake(payload)
	if err != nil {
		h.t.Fatal(err)
	}
	h.wire.deliver(f)
	return h.tick(d)
}

func (h *harness) verdict() Verdict {
	h.t.Helper()
	payload := h.wire.lastHandshakePayload(h.t)
	if len(payload) != 1 {
		h.t.Fatalf("last handshake payload is %d bytes, want a one-byte verdict: % X", len(payload), payload)
	}
	return Verdict(payload[0])
}

func (h *harness) sentQuery() ([]queryItem, bool) {
	h.t.Helper()
	for _, f := range h.wire.sentFrames(h.t) {
		if f.typ != FrameHandshake || len(f.payload) == 0 || f.payload[0] != verdictQuery {
			continue
		}
		items, err := decodeQuery(f.payload)
		if err != nil {
			h.t.Fatalf("the query this implementation wrote does not decode: %v", err)
		}
		return items, true
	}
	return nil, false
}

func judge(t *testing.T, serverSchema, clientSchema *Schema) *harness {
	t.Helper()
	h := newServerHarness(t, serverSchema)
	h.handshake(encodeHello(clientSchema), 0)
	return h
}

func TestServerAcceptsAnIdenticalSchemaFingerprintWithoutReadingItems(t *testing.T) {
	server := schemaOf(t, 0xABCD, msg(1, 0xAA, 0xBB))
	client := schemaOf(t, 0xABCD, msg(1, 0x11, 0x22))

	h := judge(t, server, client)
	if v := h.verdict(); v != VerdictAccept {
		t.Fatalf("verdict %d (%s), want accept: the schema fingerprints match, so no item may be read", v, v)
	}
	mustEvent(t, h.events, EventReady)
}

func TestServerAcceptsWhenEveryCommonMessageAgrees(t *testing.T) {
	server := schemaOf(t, 0x1111, msg(1, 0xAA, 0xBB), msg(2, 0xCC))
	client := schemaOf(t, 0x2222, msg(1, 0xAA, 0xBB), msg(3, 0xDD))

	h := judge(t, server, client)
	if v := h.verdict(); v != VerdictAccept {
		t.Fatalf("verdict %s, want accept", v)
	}
	if items, asked := h.sentQuery(); asked {
		t.Fatalf("a query was sent for %v, but every common message already agreed", items)
	}
}

func TestServerRefusesWhenTheSameFieldCountDisagrees(t *testing.T) {
	server := schemaOf(t, 0x1111, msg(1, 0xAA, 0xBB))
	client := schemaOf(t, 0x2222, msg(1, 0xAA, 0xCC))

	h := judge(t, server, client)
	if v := h.verdict(); v != VerdictConflict {
		t.Fatalf("verdict %s, want a schema conflict", v)
	}
	if e := mustEvent(t, h.events, EventHandshakeFailed); e.Verdict != VerdictConflict {
		t.Fatalf("event carries verdict %s", e.Verdict)
	}
}

func TestServerAcceptsAShorterClientPrefixWithoutAsking(t *testing.T) {
	server := schemaOf(t, 0x1111, msg(1, 0xAA, 0xBB, 0xCC))
	client := schemaOf(t, 0x2222, msg(1, 0xAA))

	h := judge(t, server, client)
	if v := h.verdict(); v != VerdictAccept {
		t.Fatalf("verdict %s, want accept", v)
	}
	if _, asked := h.sentQuery(); asked {
		t.Fatal("a query was sent although the client's own fingerprint already answered it")
	}
}

func TestServerRefusesAShorterClientPrefixThatDiffers(t *testing.T) {
	server := schemaOf(t, 0x1111, msg(1, 0xAA, 0xBB, 0xCC))
	client := schemaOf(t, 0x2222, msg(1, 0x99))

	h := judge(t, server, client)
	if v := h.verdict(); v != VerdictConflict {
		t.Fatalf("verdict %s, want a schema conflict", v)
	}
	if _, asked := h.sentQuery(); asked {
		t.Fatal("a query was sent for a case that was already decided")
	}
}

func TestServerAsksWhenTheClientCarriesMoreFields(t *testing.T) {
	server := schemaOf(t, 0x1111, msg(1, 0xAA))
	client := schemaOf(t, 0x2222, msg(1, 0xAA, 0xBB))

	h := judge(t, server, client)
	items, asked := h.sentQuery()
	if !asked {
		t.Fatal("no query was sent although the answer lives at an index only the client can produce")
	}
	if len(items) != 1 || items[0].id != 1 || items[0].fieldCount != 1 {
		t.Fatalf("query asked %v, want message 1 at field count 1", items)
	}
	mustNotEvent(t, h.events, EventReady)
	mustNotEvent(t, h.events, EventHandshakeFailed)

	h.handshake(encodeQueryReply([]replyItem{{id: 1, fingerprint: 0xAA}}), 0)
	if v := h.verdict(); v != VerdictAccept {
		t.Fatalf("verdict %s, want accept", v)
	}
	mustEvent(t, h.events, EventReady)
}

func TestServerRefusesAQueryReplyThatDiffers(t *testing.T) {
	server := schemaOf(t, 0x1111, msg(1, 0xAA))
	client := schemaOf(t, 0x2222, msg(1, 0x99, 0xBB))

	h := judge(t, server, client)
	if _, asked := h.sentQuery(); !asked {
		t.Fatal("no query was sent")
	}
	h.handshake(encodeQueryReply([]replyItem{{id: 1, fingerprint: 0x99}}), 0)
	if v := h.verdict(); v != VerdictConflict {
		t.Fatalf("verdict %s, want a schema conflict", v)
	}
}

func TestServerAcceptsWhenItHasNoFieldsForThatMessage(t *testing.T) {
	server := schemaOf(t, 0x1111, Message{ID: 1, Fingerprint: 0xEE})
	client := schemaOf(t, 0x2222, msg(1, 0xAA, 0xBB))

	h := judge(t, server, client)
	if v := h.verdict(); v != VerdictAccept {
		t.Fatalf("verdict %s, want accept: the empty prefix is a prefix of everything", v)
	}
	if _, asked := h.sentQuery(); asked {
		t.Fatal("a query was sent although the local field count is zero")
	}
}

func TestServerRefusesAMalformedQueryReply(t *testing.T) {
	cases := []struct {
		name  string
		reply []replyItem
	}{
		{"no items", nil},
		{"an extra item", []replyItem{{id: 1, fingerprint: 0xAA}, {id: 2, fingerprint: 0xBB}}},
		{"the wrong message", []replyItem{{id: 7, fingerprint: 0xAA}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			server := schemaOf(t, 0x1111, msg(1, 0xAA))
			client := schemaOf(t, 0x2222, msg(1, 0xAA, 0xBB))

			h := judge(t, server, client)
			h.handshake(encodeQueryReply(c.reply), 0)
			if v := h.verdict(); v != VerdictMalformed {
				t.Fatalf("verdict %s, want malformed", v)
			}
		})
	}
}

func TestServerIgnoresAHandshakeFrameAfterTheVerdict(t *testing.T) {
	server := schemaOf(t, 0x1111, msg(1, 0xAA))
	client := schemaOf(t, 0x2222, msg(1, 0xAA, 0xBB))

	h := judge(t, server, client)
	h.handshake(encodeQueryReply([]replyItem{{id: 1, fingerprint: 0xAA}}), 0)
	if v := h.verdict(); v != VerdictAccept {
		t.Fatalf("verdict %s, want accept", v)
	}
	before := len(h.events)
	h.handshake(encodeQueryReply([]replyItem{{id: 1, fingerprint: 0xAA}}), 0)
	if len(h.events) != before {
		t.Fatalf("a handshake frame after the verdict produced %v", kinds(h.events[before:]))
	}
}

func TestServerRefusesTheWrongProtocolVersion(t *testing.T) {
	server := schemaOf(t, 0x1111, msg(1, 0xAA))
	hello := encodeHello(schemaOf(t, 0x1111, msg(1, 0xAA)))
	binary.LittleEndian.PutUint32(hello[0:4], 1)

	h := newServerHarness(t, server)
	h.handshake(hello, 0)
	if v := h.verdict(); v != VerdictVersion {
		t.Fatalf("verdict %s, want a version refusal", v)
	}
}

func TestServerRefusesAMalformedHello(t *testing.T) {
	good := encodeHello(schemaOf(t, 0x1111, msg(1, 0xAA), msg(2, 0xBB)))

	short := append([]byte(nil), good[:15]...)

	offByOne := append([]byte(nil), good...)
	offByOne = offByOne[:len(offByOne)-1]

	hugeCount := append([]byte(nil), good...)
	binary.LittleEndian.PutUint32(hugeCount[12:16], 0xFFFFFFFF)

	overCount := append([]byte(nil), good...)
	binary.LittleEndian.PutUint32(overCount[12:16], maxHelloMessages+1)

	cases := map[string][]byte{
		"shorter than the header": short,
		"one byte short":          offByOne,
		"a count of 2^32-1":       hugeCount,
		"a count over the cap":    overCount,
	}
	for name, hello := range cases {
		t.Run(name, func(t *testing.T) {
			h := newServerHarness(t, schemaOf(t, 0x1111, msg(1, 0xAA)))
			h.handshake(hello, 0)
			if v := h.verdict(); v != VerdictMalformed {
				t.Fatalf("verdict %s, want malformed", v)
			}
		})
	}
}

func TestServerNeverExpiresAPeerThatAnswersProbes(t *testing.T) {
	h := newServerHarness(t, schemaOf(t, 0x1111, msg(1, 0xAA)))

	for elapsed := time.Second; elapsed < 10*time.Minute; elapsed += time.Second {
		events := h.tick(elapsed)
		mustNotEvent(t, events, EventDisconnected)
		for _, f := range h.wire.sentFrames(t) {
			if f.typ == FrameProbe {
				h.wire.deliver(ackFrame)
				h.wire.sent = nil
				break
			}
		}
	}
	if h.conn.State() != StateHandshaking {
		t.Fatalf("peer is %s, want still handshaking", h.conn.State())
	}
}

func TestClientBecomesReadyOnVerdictZero(t *testing.T) {
	h := newClientHarness(t, schemaOf(t, 0x1111, msg(1, 0xAA)))
	events := h.handshake([]byte{0}, 0)
	mustEvent(t, events, EventReady)
	if h.conn.State() != StateReady {
		t.Fatalf("client is %s, want ready", h.conn.State())
	}
}

func TestClientFailsOnEveryRefusal(t *testing.T) {
	for _, v := range []Verdict{VerdictVersion, VerdictConflict, VerdictMalformed} {
		h := newClientHarness(t, schemaOf(t, 0x1111, msg(1, 0xAA)))
		events := h.handshake([]byte{byte(v)}, 0)
		e := mustEvent(t, events, EventHandshakeFailed)
		if e.Verdict != v {
			t.Fatalf("event carries verdict %s, want %s", e.Verdict, v)
		}
		if h.conn.State() != StateClosed {
			t.Fatalf("client is %s after a refusal, want closed", h.conn.State())
		}
	}
}

func TestClientRejectsAMalformedVerdict(t *testing.T) {
	cases := map[string][]byte{
		"a value of 5":     {5},
		"a value of 255":   {255},
		"two bytes":        {0, 0},
		"an empty payload": {},
	}
	for name, payload := range cases {
		t.Run(name, func(t *testing.T) {
			h := newClientHarness(t, schemaOf(t, 0x1111, msg(1, 0xAA)))
			events := h.handshake(payload, 0)
			mustEvent(t, events, EventHandshakeFailed)
		})
	}
}

func TestClientAnswersAQueryFromItsOwnPrefixChain(t *testing.T) {
	h := newClientHarness(t, schemaOf(t, 0x1111, msg(1, 0xAA, 0xBB, 0xCC)))
	events := h.handshake(encodeQuery([]queryItem{{id: 1, fieldCount: 2}}), 0)
	mustNotEvent(t, events, EventReady)
	mustNotEvent(t, events, EventHandshakeFailed)

	reply, err := decodeQueryReply(h.wire.lastHandshakePayload(t))
	if err != nil {
		t.Fatal(err)
	}
	if len(reply) != 1 || reply[0].id != 1 || reply[0].fingerprint != 0xBB {
		t.Fatalf("reply is %v, want message 1 answered with the prefix at field 2", reply)
	}
}

func TestClientRejectsASecondQuery(t *testing.T) {
	h := newClientHarness(t, schemaOf(t, 0x1111, msg(1, 0xAA, 0xBB)))
	h.handshake(encodeQuery([]queryItem{{id: 1, fieldCount: 1}}), 0)
	events := h.handshake(encodeQuery([]queryItem{{id: 1, fieldCount: 1}}), 0)
	mustEvent(t, events, EventHandshakeFailed)
}

func TestClientRejectsAnImpossibleQuery(t *testing.T) {
	cases := map[string]queryItem{
		"a message it never declared":    {id: 9, fieldCount: 1},
		"a field count of zero":          {id: 1, fieldCount: 0},
		"a field count it does not have": {id: 1, fieldCount: 5},
	}
	for name, item := range cases {
		t.Run(name, func(t *testing.T) {
			h := newClientHarness(t, schemaOf(t, 0x1111, msg(1, 0xAA, 0xBB)))
			events := h.handshake(encodeQuery([]queryItem{item}), 0)
			mustEvent(t, events, EventHandshakeFailed)
		})
	}
}

func TestClientHandshakeDeadlineIsNotResetByTheQueryRound(t *testing.T) {
	h := newClientHarness(t, schemaOf(t, 0x1111, msg(1, 0xAA, 0xBB, 0xCC)))
	h.handshake(encodeQuery([]queryItem{{id: 1, fieldCount: 2}}), 3*time.Second)
	mustNotEvent(t, h.events, EventHandshakeFailed)

	events := h.tick(5 * time.Second)
	e := mustEvent(t, events, EventHandshakeFailed)
	if e.Err == nil {
		t.Fatal("the timeout event carries no reason")
	}
}

func TestClientHandshakeTimesOut(t *testing.T) {
	h := newClientHarness(t, schemaOf(t, 0x1111, msg(1, 0xAA)))
	mustNotEvent(t, h.tick(4*time.Second), EventHandshakeFailed)
	mustEvent(t, h.tick(5*time.Second), EventHandshakeFailed)
}

func TestHelloLayout(t *testing.T) {
	s := schemaOf(t, 0x0102030405060708, msg(0x11223344, 0x99), msg(0x55667788, 0xAA, 0xBB))
	hello := encodeHello(s)

	if len(hello) != helloHeaderLen+2*helloItemLen {
		t.Fatalf("hello is %d bytes, want %d", len(hello), helloHeaderLen+2*helloItemLen)
	}
	if got := binary.LittleEndian.Uint32(hello[0:4]); got != ProtocolVersion {
		t.Fatalf("protocol version is %d, want %d", got, ProtocolVersion)
	}
	if got := binary.LittleEndian.Uint64(hello[4:12]); got != 0x0102030405060708 {
		t.Fatalf("schema fingerprint is 0x%X", got)
	}
	if got := binary.LittleEndian.Uint32(hello[12:16]); got != 2 {
		t.Fatalf("message count is %d, want 2", got)
	}
	if got := binary.LittleEndian.Uint32(hello[16:20]); got != 0x11223344 {
		t.Fatalf("first message id is 0x%X", got)
	}
	if got := binary.LittleEndian.Uint16(hello[20:22]); got != 1 {
		t.Fatalf("first field count is %d, want 1", got)
	}
	if got := binary.LittleEndian.Uint16(hello[34:36]); got != 2 {
		t.Fatalf("second field count is %d, want 2", got)
	}

	back, err := decodeHello(hello)
	if err != nil {
		t.Fatal(err)
	}
	if len(back.items) != 2 || back.items[1].fingerprint != 0xBB {
		t.Fatalf("decoded %+v", back)
	}
}

func TestSchemaRejectsAnInconsistentPrefixChain(t *testing.T) {
	if _, err := NewSchema(1, []Message{{ID: 1, Fingerprint: 0xAA, Prefixes: []uint64{0xBB}}}); err == nil {
		t.Fatal("a message whose last prefix is not its fingerprint was accepted")
	}
	if _, err := NewSchema(1, []Message{msg(1, 0xAA), msg(1, 0xBB)}); err == nil {
		t.Fatal("a duplicate message id was accepted")
	}
}

func TestAppendingAFieldAtTheEndMustInteroperate(t *testing.T) {
	short := schemaOf(t, 0x1111, msg(1, 0xAA))
	long := schemaOf(t, 0x2222, msg(1, 0xAA, 0xBB))

	server := judge(t, short, long)
	if items, asked := server.sentQuery(); asked {
		if len(items) != 1 || items[0].fieldCount != 1 {
			t.Fatalf("query asked %v, want message 1 at field count 1", items)
		}
		server.handshake(encodeQueryReply([]replyItem{{id: 1, fingerprint: 0xAA}}), 0)
	}
	if v := server.verdict(); v != VerdictAccept {
		t.Fatalf("server with fewer fields answered %s; RFC-0002 9.1 and RFC-0003 8.6 V-002 require this pair to interoperate", v)
	}

	other := judge(t, long, short)
	if v := other.verdict(); v != VerdictAccept {
		t.Fatalf("server with more fields answered %s; RFC-0003 8.6 V-001 requires this pair to interoperate", v)
	}
}
