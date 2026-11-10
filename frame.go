package fomoxa

import (
	"encoding/binary"
	"errors"
	"fmt"
)

type FrameType byte

const (
	FrameData          FrameType = 0
	FrameProbe         FrameType = 1
	FrameAck           FrameType = 2
	FrameHandshake     FrameType = 3
	FrameHandshakePart FrameType = 4
)

func (t FrameType) String() string {
	switch t {
	case FrameData:
		return "data"
	case FrameProbe:
		return "probe"
	case FrameAck:
		return "ack"
	case FrameHandshake:
		return "handshake"
	case FrameHandshakePart:
		return "handshake part"
	default:
		return fmt.Sprintf("frame type 0x%02X", byte(t))
	}
}

const (
	magicC = 0x43
	magicY = 0x59

	dataHeaderLen      = 11
	handshakeHeaderLen = 5
	partHeaderLen      = 9

	packetHandshakeBudget = 900
	partContentLen        = packetHandshakeBudget - partHeaderLen
	maxHandshakeParts     = (MaxHandshakePayload + partContentLen - 1) / partContentLen

	MaxMessagePayload   = 16 * 1024 * 1024
	MaxHandshakePayload = 1024 * 1024
	MaxFrameLen         = dataHeaderLen + MaxMessagePayload
)

var (
	probeFrame = []byte{byte(FrameProbe)}
	ackFrame   = []byte{byte(FrameAck)}
)

type frame struct {
	typ       FrameType
	messageID uint32
	partIndex int
	partCount int
	payload   []byte
}

type FrameError struct {
	msg string
}

func (e *FrameError) Error() string { return e.msg }

func frameErrorf(format string, args ...any) *FrameError {
	return &FrameError{msg: fmt.Sprintf(format, args...)}
}

var errIncomplete = errors.New("fomoxa: frame is incomplete")

func encodeData(messageID uint32, payload []byte) ([]byte, error) {
	if len(payload) > MaxMessagePayload {
		return nil, fmt.Errorf("%w: %d bytes", ErrPayloadTooLarge, len(payload))
	}
	out := make([]byte, dataHeaderLen+len(payload))
	out[0] = byte(FrameData)
	out[1] = magicC
	out[2] = magicY
	binary.LittleEndian.PutUint32(out[3:7], messageID)
	binary.LittleEndian.PutUint32(out[7:11], uint32(len(payload)))
	copy(out[dataHeaderLen:], payload)
	return out, nil
}

func encodeHandshake(payload []byte) ([]byte, error) {
	if len(payload) > MaxHandshakePayload {
		return nil, frameErrorf("fomoxa: handshake payload of %d bytes exceeds %d", len(payload), MaxHandshakePayload)
	}
	out := make([]byte, handshakeHeaderLen+len(payload))
	out[0] = byte(FrameHandshake)
	binary.LittleEndian.PutUint32(out[1:5], uint32(len(payload)))
	copy(out[handshakeHeaderLen:], payload)
	return out, nil
}

func encodePart(index, count int, content []byte) []byte {
	out := make([]byte, partHeaderLen+len(content))
	out[0] = byte(FrameHandshakePart)
	binary.LittleEndian.PutUint16(out[1:3], uint16(index))
	binary.LittleEndian.PutUint16(out[3:5], uint16(count))
	binary.LittleEndian.PutUint32(out[5:9], uint32(len(content)))
	copy(out[partHeaderLen:], content)
	return out
}

func encodeParts(body []byte) [][]byte {
	count := (len(body) + partContentLen - 1) / partContentLen
	parts := make([][]byte, count)
	for index := range parts {
		offset := index * partContentLen
		end := min(offset+partContentLen, len(body))
		parts[index] = encodePart(index, count, body[offset:end])
	}
	return parts
}

func decodeFrame(b []byte) (frame, int, error) {
	if len(b) == 0 {
		return frame{}, 0, errIncomplete
	}
	switch FrameType(b[0]) {
	case FrameProbe:
		return frame{typ: FrameProbe}, 1, nil
	case FrameAck:
		return frame{typ: FrameAck}, 1, nil
	case FrameData:
		if len(b) < dataHeaderLen {
			if len(b) >= 2 && b[1] != magicC {
				return frame{}, 0, frameErrorf("fomoxa: data frame magic is 0x%02X, not 'C'", b[1])
			}
			if len(b) >= 3 && b[2] != magicY {
				return frame{}, 0, frameErrorf("fomoxa: data frame magic is 0x%02X, not 'Y'", b[2])
			}
			return frame{}, 0, errIncomplete
		}
		if b[1] != magicC || b[2] != magicY {
			return frame{}, 0, frameErrorf("fomoxa: data frame magic is 0x%02X 0x%02X, not 'C' 'Y'", b[1], b[2])
		}
		length := binary.LittleEndian.Uint32(b[7:11])
		if length > MaxMessagePayload {
			return frame{}, 0, frameErrorf("fomoxa: message payload of %d bytes exceeds %d", length, MaxMessagePayload)
		}
		total := dataHeaderLen + int(length)
		if len(b) < total {
			return frame{}, 0, errIncomplete
		}
		return frame{
			typ:       FrameData,
			messageID: binary.LittleEndian.Uint32(b[3:7]),
			payload:   b[dataHeaderLen:total],
		}, total, nil
	case FrameHandshake:
		if len(b) < handshakeHeaderLen {
			return frame{}, 0, errIncomplete
		}
		length := binary.LittleEndian.Uint32(b[1:5])
		if length > MaxHandshakePayload {
			return frame{}, 0, frameErrorf("fomoxa: handshake payload of %d bytes exceeds %d", length, MaxHandshakePayload)
		}
		total := handshakeHeaderLen + int(length)
		if len(b) < total {
			return frame{}, 0, errIncomplete
		}
		return frame{typ: FrameHandshake, payload: b[handshakeHeaderLen:total]}, total, nil
	case FrameHandshakePart:
		if len(b) < partHeaderLen {
			return frame{}, 0, errIncomplete
		}
		length := binary.LittleEndian.Uint32(b[5:9])
		if length > partContentLen {
			return frame{}, 0, frameErrorf("fomoxa: handshake part of %d bytes exceeds %d", length, partContentLen)
		}
		total := partHeaderLen + int(length)
		if len(b) < total {
			return frame{}, 0, errIncomplete
		}
		return frame{
			typ:       FrameHandshakePart,
			partIndex: int(binary.LittleEndian.Uint16(b[1:3])),
			partCount: int(binary.LittleEndian.Uint16(b[3:5])),
			payload:   b[partHeaderLen:total],
		}, total, nil
	default:
		return frame{}, 0, frameErrorf("fomoxa: frame type 0x%02X is not 0..4", b[0])
	}
}

func decodePacket(b []byte) (frame, error) {
	f, n, err := decodeFrame(b)
	if err == errIncomplete {
		return frame{}, frameErrorf("fomoxa: packet of %d bytes ended before the frame it declared", len(b))
	}
	if err != nil {
		return frame{}, err
	}
	if n != len(b) {
		return frame{}, frameErrorf("fomoxa: %d bytes left over after a complete frame", len(b)-n)
	}
	return f, nil
}

type streamDecoder struct {
	buf      []byte
	off      int
	poisoned error
}

func (d *streamDecoder) feed(b []byte) {
	if d.off > 0 {
		d.buf = append(d.buf[:0], d.buf[d.off:]...)
		d.off = 0
	}
	d.buf = append(d.buf, b...)
}

func (d *streamDecoder) next() (frame, bool, error) {
	if d.poisoned != nil {
		return frame{}, false, d.poisoned
	}
	if d.off < len(d.buf) && FrameType(d.buf[d.off]) == FrameHandshakePart {
		d.poisoned = frameErrorf("fomoxa: a handshake part arrived on a byte stream")
		return frame{}, false, d.poisoned
	}
	f, n, err := decodeFrame(d.buf[d.off:])
	if err == errIncomplete {
		return frame{}, false, nil
	}
	if err != nil {
		d.poisoned = err
		return frame{}, false, err
	}
	d.off += n
	return f, true, nil
}

func (d *streamDecoder) buffered() int { return len(d.buf) - d.off }

func (d *streamDecoder) shrink() {
	live := d.buf[d.off:]
	fresh := make([]byte, len(live))
	copy(fresh, live)
	d.buf = fresh
	d.off = 0
}
