package cyclone

import (
	"encoding/binary"
	"fmt"
)

// Message is a decoded Cyclone message: an id and its raw payload. Not
// itself decoded into a model - see cyclonec's generated Reader/Writer for
// that. This is the transport's unit, the same way Cyclone.Unity's
// CycloneMessage struct and cyclone-rust's CycloneMessage are (named
// Message, not CycloneMessage, here - see the package doc comment for why).
type Message struct {
	ID      uint32
	Payload []byte
}

// PingID and PongID are message ids Connection reserves for its own
// heartbeat - never a message a game defines. The exact values the C#,
// GDScript and Rust SDKs use.
const (
	PingID uint32 = 0xFFFF_0001
	PongID uint32 = 0xFFFF_0002
)

const (
	magic1            byte = 0x43 // 'C'
	magic2            byte = 0x59 // 'Y'
	magicSize              = 2
	messageIDSize          = 4
	payloadLengthSize      = 4
	headerSize             = magicSize + messageIDSize + payloadLengthSize

	// maxPayloadLength is 16 MiB - the same ceiling the C#, GDScript and
	// Rust SDKs refuse to encode or decode past.
	maxPayloadLength = 16 * 1024 * 1024
)

// encodeFrame encodes message as one frame:
// Magic(2) + MessageId(u32 LE) + PayloadLength(u32 LE) + Payload.
//
// Panics if the payload exceeds maxPayloadLength - the same refusal the
// sibling SDKs' encoders throw for. A payload this large is a caller
// mistake (RFC-0002's own string/bytes length prefix cannot even address
// it), not a runtime condition to recover from.
func encodeFrame(message Message) []byte {
	if len(message.Payload) > maxPayloadLength {
		panic(fmt.Sprintf(
			"cyclone: payload is too large: %d bytes exceeds the %d byte limit",
			len(message.Payload), maxPayloadLength))
	}

	frame := make([]byte, headerSize+len(message.Payload))
	frame[0] = magic1
	frame[1] = magic2
	binary.LittleEndian.PutUint32(frame[magicSize:], message.ID)
	binary.LittleEndian.PutUint32(frame[magicSize+messageIDSize:], uint32(len(message.Payload)))
	copy(frame[headerSize:], message.Payload)
	return frame
}

// tryDecodeFrame decodes exactly one frame's worth of bytes - data must be
// exactly one frame, no more and no less (the caller, Connection, is what
// finds where a frame starts and ends in a streamed buffer).
//
// Returns ok == false for anything that does not satisfy the layout: bad
// magic, a declared payload length over maxPayloadLength, or a declared
// length that does not match the bytes actually given.
func tryDecodeFrame(data []byte) (message Message, ok bool) {
	if len(data) < headerSize {
		return Message{}, false
	}
	if data[0] != magic1 || data[1] != magic2 {
		return Message{}, false
	}

	id := binary.LittleEndian.Uint32(data[magicSize:])
	payloadLength := binary.LittleEndian.Uint32(data[magicSize+messageIDSize:])

	if payloadLength > maxPayloadLength {
		return Message{}, false
	}

	actualPayloadLength := len(data) - headerSize
	if int(payloadLength) != actualPayloadLength {
		return Message{}, false
	}

	payload := make([]byte, payloadLength)
	copy(payload, data[headerSize:])
	return Message{ID: id, Payload: payload}, true
}
