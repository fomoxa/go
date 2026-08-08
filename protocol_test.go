package cyclone

import "testing"

func TestFrameRoundTrip(t *testing.T) {
	message := Message{ID: 42, Payload: []byte("hello-cyclone")}
	frame := encodeFrame(message)

	if len(frame) != headerSize+len(message.Payload) {
		t.Fatalf("frame length = %d, want %d", len(frame), headerSize+len(message.Payload))
	}
	if frame[0] != magic1 || frame[1] != magic2 {
		t.Fatalf("frame does not start with magic: %v", frame[:2])
	}

	decoded, ok := tryDecodeFrame(frame)
	if !ok {
		t.Fatal("round-trip failed to decode")
	}
	if decoded.ID != message.ID {
		t.Errorf("decoded.ID = %d, want %d", decoded.ID, message.ID)
	}
	if string(decoded.Payload) != string(message.Payload) {
		t.Errorf("decoded.Payload = %q, want %q", decoded.Payload, message.Payload)
	}
}

func TestBadMagicIsRejected(t *testing.T) {
	frame := encodeFrame(Message{ID: 1})
	frame[0] = 0x00
	if _, ok := tryDecodeFrame(frame); ok {
		t.Fatal("expected rejection of a frame with the wrong magic")
	}
}

func TestTruncatedIsRejected(t *testing.T) {
	frame := encodeFrame(Message{ID: 1, Payload: []byte("abc")})
	if _, ok := tryDecodeFrame(frame[:len(frame)-1]); ok {
		t.Fatal("expected rejection of a truncated frame")
	}
	if _, ok := tryDecodeFrame(nil); ok {
		t.Fatal("expected rejection of an empty buffer")
	}
}

func TestWrongLengthIsRejected(t *testing.T) {
	frame := encodeFrame(Message{ID: 1, Payload: []byte("abc")})
	// Claim a longer payload than what actually follows.
	frame[magicSize+messageIDSize] = 0xFF
	frame[magicSize+messageIDSize+1] = 0xFF
	if _, ok := tryDecodeFrame(frame); ok {
		t.Fatal("expected rejection of a PayloadLength that does not match the bytes given")
	}
}

func TestEmptyPayloadRoundTrips(t *testing.T) {
	message := Message{ID: PingID}
	frame := encodeFrame(message)
	if len(frame) != headerSize {
		t.Fatalf("frame length = %d, want %d (header only)", len(frame), headerSize)
	}

	decoded, ok := tryDecodeFrame(frame)
	if !ok || decoded.ID != PingID || len(decoded.Payload) != 0 {
		t.Fatalf("Ping did not round-trip with an empty payload: %+v ok=%v", decoded, ok)
	}
}

func TestOversizedPayloadPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected a panic for an oversized payload")
		}
	}()
	encodeFrame(Message{ID: 1, Payload: make([]byte, maxPayloadLength+1)})
}
