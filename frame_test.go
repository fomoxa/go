package fomoxa

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestDataFrameLayout(t *testing.T) {
	payload := bytes.Repeat([]byte{0xAB}, 100)
	f, err := encodeData(42, payload)
	if err != nil {
		t.Fatal(err)
	}
	if len(f) != 111 {
		t.Fatalf("data frame is %d bytes, want 111", len(f))
	}
	if f[0] != 0x00 || f[1] != 0x43 || f[2] != 0x59 {
		t.Fatalf("header starts with %02X %02X %02X, want 00 43 59", f[0], f[1], f[2])
	}
	if got := binary.LittleEndian.Uint32(f[3:7]); got != 42 {
		t.Fatalf("message id is %d, want 42", got)
	}
	if got := binary.LittleEndian.Uint32(f[7:11]); got != 100 {
		t.Fatalf("length is %d, want 100", got)
	}
	if !bytes.Equal(f[11:], payload) {
		t.Fatal("payload is not copied verbatim")
	}
}

func TestHandshakeFrameLayout(t *testing.T) {
	f, err := encodeHandshake([]byte{7, 8, 9})
	if err != nil {
		t.Fatal(err)
	}
	want := []byte{0x03, 0x03, 0x00, 0x00, 0x00, 7, 8, 9}
	if !bytes.Equal(f, want) {
		t.Fatalf("handshake frame is % X, want % X", f, want)
	}
}

func TestProbeAndAckAreOneByte(t *testing.T) {
	if !bytes.Equal(probeFrame, []byte{0x01}) {
		t.Fatalf("probe frame is % X", probeFrame)
	}
	if !bytes.Equal(ackFrame, []byte{0x02}) {
		t.Fatalf("ack frame is % X", ackFrame)
	}
}

func TestFrameRoundTrip(t *testing.T) {
	data, _ := encodeData(0x11223344, []byte("hello"))
	handshake, _ := encodeHandshake([]byte("hs"))

	cases := []struct {
		name  string
		bytes []byte
		check func(f frame) bool
	}{
		{"data", data, func(f frame) bool {
			return f.typ == FrameData && f.messageID == 0x11223344 && string(f.payload) == "hello"
		}},
		{"probe", probeFrame, func(f frame) bool { return f.typ == FrameProbe }},
		{"ack", ackFrame, func(f frame) bool { return f.typ == FrameAck }},
		{"handshake", handshake, func(f frame) bool {
			return f.typ == FrameHandshake && string(f.payload) == "hs"
		}},
	}
	for _, c := range cases {
		f, err := decodePacket(c.bytes)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if !c.check(f) {
			t.Fatalf("%s: decoded into %+v", c.name, f)
		}
	}
}

func TestStreamDecoderOneByteAtATime(t *testing.T) {
	data, _ := encodeData(7, []byte("abcdef"))
	handshake, _ := encodeHandshake([]byte{0})
	stream := append(append(append([]byte{}, data...), probeFrame...), handshake...)

	var d streamDecoder
	var got []FrameType
	for i := 0; i < len(stream); i++ {
		d.feed(stream[i : i+1])
		for {
			f, ok, err := d.next()
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				break
			}
			got = append(got, f.typ)
		}
	}
	want := []FrameType{FrameData, FrameProbe, FrameHandshake}
	if len(got) != len(want) {
		t.Fatalf("decoded %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("decoded %v, want %v", got, want)
		}
	}
}

func TestStreamDecoderManyFramesInOneFeed(t *testing.T) {
	var stream []byte
	for i := 0; i < 5; i++ {
		f, _ := encodeData(uint32(i), []byte{byte(i)})
		stream = append(stream, f...)
	}

	var d streamDecoder
	d.feed(stream)
	for i := 0; i < 5; i++ {
		f, ok, err := d.next()
		if err != nil || !ok {
			t.Fatalf("frame %d: ok=%v err=%v", i, ok, err)
		}
		if f.messageID != uint32(i) || f.payload[0] != byte(i) {
			t.Fatalf("frame %d decoded as %+v", i, f)
		}
	}
	if _, ok, _ := d.next(); ok {
		t.Fatal("a sixth frame appeared")
	}
	if d.buffered() != 0 {
		t.Fatalf("%d bytes left over", d.buffered())
	}
}

func TestStreamDecoderStaysPoisoned(t *testing.T) {
	var d streamDecoder
	d.feed([]byte{0x09})
	_, _, first := d.next()
	if first == nil {
		t.Fatal("frame type 0x09 was accepted")
	}

	good, _ := encodeData(1, nil)
	d.feed(good)
	_, ok, again := d.next()
	if ok {
		t.Fatal("the decoder kept reading after a violation")
	}
	if again != first {
		t.Fatalf("second call returned %v, want the original %v", again, first)
	}
}

func TestBadMagicIsRejected(t *testing.T) {
	f, _ := encodeData(1, []byte("x"))
	f[1] = 'X'
	if _, err := decodePacket(f); err == nil {
		t.Fatal("a data frame with the wrong magic was accepted")
	}
}

func TestPacketMustHoldExactlyOneFrame(t *testing.T) {
	f, _ := encodeData(1, []byte("abc"))

	if _, err := decodePacket(f[:len(f)-1]); err == nil {
		t.Fatal("a truncated packet was accepted")
	}
	if _, err := decodePacket(append(append([]byte{}, f...), 0x00)); err == nil {
		t.Fatal("a packet with a trailing byte was accepted")
	}
}

func TestEmptyPacketIsRejected(t *testing.T) {
	if _, err := decodePacket(nil); err == nil {
		t.Fatal("an empty packet was accepted")
	}
}

func TestPayloadLimits(t *testing.T) {
	header := make([]byte, dataHeaderLen)
	header[0] = 0x00
	header[1] = magicC
	header[2] = magicY
	binary.LittleEndian.PutUint32(header[7:11], MaxMessagePayload)
	if _, _, err := decodeFrame(header); err != errIncomplete {
		t.Fatalf("a length of exactly the limit gave %v, want incomplete", err)
	}

	binary.LittleEndian.PutUint32(header[7:11], MaxMessagePayload+1)
	if _, _, err := decodeFrame(header); err == nil || err == errIncomplete {
		t.Fatalf("a length one over the limit gave %v, want a violation", err)
	}

	if _, err := encodeData(1, make([]byte, MaxMessagePayload+1)); err == nil {
		t.Fatal("a payload one byte over the limit was encoded")
	}

	handshake := []byte{0x03, 0, 0, 0, 0}
	binary.LittleEndian.PutUint32(handshake[1:5], MaxHandshakePayload+1)
	if _, _, err := decodeFrame(handshake); err == nil || err == errIncomplete {
		t.Fatalf("an oversized handshake length gave %v, want a violation", err)
	}
}

func TestHugeLengthDoesNotAllocate(t *testing.T) {
	header := make([]byte, dataHeaderLen)
	header[1] = magicC
	header[2] = magicY
	binary.LittleEndian.PutUint32(header[7:11], 0xFFFFFFFF)
	if _, _, err := decodeFrame(header); err == nil || err == errIncomplete {
		t.Fatalf("a length of 4294967295 gave %v, want a violation", err)
	}
}
