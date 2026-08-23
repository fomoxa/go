package fomoxa

import (
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	ProtocolVersion = 2

	maxHelloMessages = 1000000
	maxQueryItems    = 1000000

	helloHeaderLen = 16
	helloItemLen   = 14
	queryHeaderLen = 5
	queryItemLen   = 6
	replyHeaderLen = 4
	replyItemLen   = 12

	verdictQuery = 4
)

type Verdict byte

const (
	VerdictAccept    Verdict = 0
	VerdictVersion   Verdict = 1
	VerdictConflict  Verdict = 2
	VerdictMalformed Verdict = 3
)

func (v Verdict) String() string {
	switch v {
	case VerdictAccept:
		return "accepted"
	case VerdictVersion:
		return "wrong protocol version"
	case VerdictConflict:
		return "schema conflict"
	case VerdictMalformed:
		return "malformed hello"
	default:
		return fmt.Sprintf("verdict %d", byte(v))
	}
}

var errHandshakeMalformed = errors.New("fomoxa: malformed handshake payload")

type helloItem struct {
	id          uint32
	fieldCount  uint32
	fingerprint uint64
}

type hello struct {
	version     uint32
	fingerprint uint64
	items       []helloItem
}

func encodeHello(s *Schema) []byte {
	messages := s.Messages()
	out := make([]byte, helloHeaderLen+helloItemLen*len(messages))
	binary.LittleEndian.PutUint32(out[0:4], ProtocolVersion)
	binary.LittleEndian.PutUint64(out[4:12], s.Fingerprint())
	binary.LittleEndian.PutUint32(out[12:16], uint32(len(messages)))

	at := helloHeaderLen
	for _, m := range messages {
		binary.LittleEndian.PutUint32(out[at:at+4], m.ID)
		binary.LittleEndian.PutUint16(out[at+4:at+6], uint16(m.FieldCount()))
		binary.LittleEndian.PutUint64(out[at+6:at+14], m.Fingerprint)
		at += helloItemLen
	}
	return out
}

func decodeHello(b []byte) (hello, error) {
	if len(b) < helloHeaderLen {
		return hello{}, fmt.Errorf("%w: hello of %d bytes is shorter than %d", errHandshakeMalformed, len(b), helloHeaderLen)
	}
	count := binary.LittleEndian.Uint32(b[12:16])
	if count > maxHelloMessages {
		return hello{}, fmt.Errorf("%w: hello declares %d messages, more than %d", errHandshakeMalformed, count, maxHelloMessages)
	}
	want := uint64(helloHeaderLen) + uint64(helloItemLen)*uint64(count)
	if uint64(len(b)) != want {
		return hello{}, fmt.Errorf("%w: hello of %d bytes, expected %d for %d messages", errHandshakeMalformed, len(b), want, count)
	}

	h := hello{
		version:     binary.LittleEndian.Uint32(b[0:4]),
		fingerprint: binary.LittleEndian.Uint64(b[4:12]),
		items:       make([]helloItem, 0, count),
	}
	at := helloHeaderLen
	for i := uint32(0); i < count; i++ {
		h.items = append(h.items, helloItem{
			id:          binary.LittleEndian.Uint32(b[at : at+4]),
			fieldCount:  uint32(binary.LittleEndian.Uint16(b[at+4 : at+6])),
			fingerprint: binary.LittleEndian.Uint64(b[at+6 : at+14]),
		})
		at += helloItemLen
	}
	return h, nil
}

type queryItem struct {
	id         uint32
	fieldCount uint32
}

func encodeQuery(items []queryItem) []byte {
	out := make([]byte, queryHeaderLen+queryItemLen*len(items))
	out[0] = verdictQuery
	binary.LittleEndian.PutUint32(out[1:5], uint32(len(items)))
	at := queryHeaderLen
	for _, item := range items {
		binary.LittleEndian.PutUint32(out[at:at+4], item.id)
		binary.LittleEndian.PutUint16(out[at+4:at+6], uint16(item.fieldCount))
		at += queryItemLen
	}
	return out
}

func decodeQuery(b []byte) ([]queryItem, error) {
	if len(b) < queryHeaderLen || b[0] != verdictQuery {
		return nil, fmt.Errorf("%w: not a query payload", errHandshakeMalformed)
	}
	count := binary.LittleEndian.Uint32(b[1:5])
	if count > maxQueryItems {
		return nil, fmt.Errorf("%w: query asks for %d items, more than %d", errHandshakeMalformed, count, maxQueryItems)
	}
	want := uint64(queryHeaderLen) + uint64(queryItemLen)*uint64(count)
	if uint64(len(b)) != want {
		return nil, fmt.Errorf("%w: query of %d bytes, expected %d for %d items", errHandshakeMalformed, len(b), want, count)
	}

	items := make([]queryItem, 0, count)
	at := queryHeaderLen
	for i := uint32(0); i < count; i++ {
		items = append(items, queryItem{
			id:         binary.LittleEndian.Uint32(b[at : at+4]),
			fieldCount: uint32(binary.LittleEndian.Uint16(b[at+4 : at+6])),
		})
		at += queryItemLen
	}
	return items, nil
}

type replyItem struct {
	id          uint32
	fingerprint uint64
}

func encodeQueryReply(items []replyItem) []byte {
	out := make([]byte, replyHeaderLen+replyItemLen*len(items))
	binary.LittleEndian.PutUint32(out[0:4], uint32(len(items)))
	at := replyHeaderLen
	for _, item := range items {
		binary.LittleEndian.PutUint32(out[at:at+4], item.id)
		binary.LittleEndian.PutUint64(out[at+4:at+12], item.fingerprint)
		at += replyItemLen
	}
	return out
}

func decodeQueryReply(b []byte) ([]replyItem, error) {
	if len(b) < replyHeaderLen {
		return nil, fmt.Errorf("%w: query reply of %d bytes is shorter than %d", errHandshakeMalformed, len(b), replyHeaderLen)
	}
	count := binary.LittleEndian.Uint32(b[0:4])
	if count > maxQueryItems {
		return nil, fmt.Errorf("%w: query reply carries %d items, more than %d", errHandshakeMalformed, count, maxQueryItems)
	}
	want := uint64(replyHeaderLen) + uint64(replyItemLen)*uint64(count)
	if uint64(len(b)) != want {
		return nil, fmt.Errorf("%w: query reply of %d bytes, expected %d for %d items", errHandshakeMalformed, len(b), want, count)
	}

	items := make([]replyItem, 0, count)
	at := replyHeaderLen
	for i := uint32(0); i < count; i++ {
		items = append(items, replyItem{
			id:          binary.LittleEndian.Uint32(b[at : at+4]),
			fingerprint: binary.LittleEndian.Uint64(b[at+4 : at+12]),
		})
		at += replyItemLen
	}
	return items, nil
}
