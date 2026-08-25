package fomoxa

import (
	"encoding/binary"
	"errors"
	"fmt"
)

type FrameType byte

const (
	FrameData      FrameType = 0
	FrameProbe     FrameType = 1
	FrameAck       FrameType = 2
	FrameHandshake FrameType = 3
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
	default:
		return fmt.Sprintf("frame type 0x%02X", byte(t))
	}
}

const (
	magicC = 0x43
	magicY = 0x59

	dataHeaderLen      = 11
	handshakeHeaderLen = 5

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
	default:
		return frame{}, 0, frameErrorf("fomoxa: frame type 0x%02X is not 0..3", b[0])
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
