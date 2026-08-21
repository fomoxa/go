package cyclone

import (
	"errors"
	"fmt"
	"time"
)

type role int

const (
	roleClient role = iota
	roleServer
)

type outcome struct {
	frame []byte
	event *Event
}

type session struct {
	role   role
	schema *Schema
	cfg    Config

	state      State
	terminated bool

	startedAt    time.Time
	lastActivity time.Time
	probing      bool
	probeSentAt  time.Time

	helloSeen    bool
	queried      bool
	pendingQuery []queryItem
}

func newSession(r role, schema *Schema, cfg Config) *session {
	return &session{role: r, schema: schema, cfg: cfg, state: StateHandshaking}
}

func (s *session) start(now time.Time) outcome {
	s.startedAt = now
	s.lastActivity = now
	if s.role != roleClient {
		return outcome{}
	}
	f, err := encodeHandshake(encodeHello(s.schema))
	if err != nil {
		return s.terminate(EventHandshakeFailed, VerdictMalformed, err)
	}
	return outcome{frame: f}
}

func (s *session) handleFrame(f frame, now time.Time) outcome {
	if s.state == StateClosed {
		return outcome{}
	}
	s.lastActivity = now
	s.probing = false

	switch f.typ {
	case FrameProbe:
		out := outcome{frame: ackFrame}
		if s.state == StateReady {
			out.event = &Event{Kind: EventProbe}
		}
		return out
	case FrameAck:
		if s.state == StateReady {
			return outcome{event: &Event{Kind: EventAck}}
		}
		return outcome{}
	case FrameData:
		if s.state != StateReady {
			return outcome{}
		}
		payload := make([]byte, len(f.payload))
		copy(payload, f.payload)
		return outcome{event: &Event{Kind: EventMessage, MessageID: f.messageID, Payload: payload}}
	case FrameHandshake:
		if s.state != StateHandshaking {
			return outcome{}
		}
		if s.role == roleClient {
			return s.clientHandshake(f.payload)
		}
		return s.serverHandshake(f.payload)
	default:
		return outcome{}
	}
}

func (s *session) clientHandshake(payload []byte) outcome {
	if len(payload) > 0 && payload[0] == verdictQuery {
		if s.queried {
			return s.failHandshake(fmt.Errorf("%w: server asked a second time", errHandshakeMalformed))
		}
		items, err := decodeQuery(payload)
		if err != nil {
			return s.failHandshake(err)
		}
		s.queried = true

		reply := make([]replyItem, 0, len(items))
		for _, item := range items {
			local, ok := s.schema.message(item.id)
			if !ok {
				return s.failHandshake(fmt.Errorf("%w: query asks about message 0x%08X, which the hello did not declare",
					errHandshakeMalformed, item.id))
			}
			if item.fieldCount < 1 || item.fieldCount >= local.FieldCount() {
				return s.failHandshake(fmt.Errorf("%w: query asks message 0x%08X at field %d, outside 1..%d",
					errHandshakeMalformed, item.id, item.fieldCount, local.FieldCount()-1))
			}
			fingerprint, ok := s.schema.prefix(item.id, item.fieldCount)
			if !ok {
				return s.failHandshake(fmt.Errorf("%w: no prefix fingerprint for message 0x%08X at field %d",
					errHandshakeMalformed, item.id, item.fieldCount))
			}
			reply = append(reply, replyItem{id: item.id, fingerprint: fingerprint})
		}

		f, err := encodeHandshake(encodeQueryReply(reply))
		if err != nil {
			return s.failHandshake(err)
		}
		return outcome{frame: f}
	}

	if len(payload) != 1 || payload[0] > byte(VerdictMalformed) {
		return s.failHandshake(fmt.Errorf("%w: verdict payload is %d bytes", errHandshakeMalformed, len(payload)))
	}
	v := Verdict(payload[0])
	if v == VerdictAccept {
		s.state = StateReady
		return outcome{event: &Event{Kind: EventReady}}
	}
	return s.terminate(EventHandshakeFailed, v, fmt.Errorf("cyclone: handshake refused: %s", v))
}

func (s *session) failHandshake(err error) outcome {
	return s.terminate(EventHandshakeFailed, VerdictMalformed, err)
}

func (s *session) serverHandshake(payload []byte) outcome {
	if !s.helloSeen {
		s.helloSeen = true
		return s.judgeHello(payload)
	}
	if s.pendingQuery != nil {
		return s.judgeQueryReply(payload)
	}
	return s.refuse(VerdictMalformed, fmt.Errorf("%w: a third handshake payload arrived", errHandshakeMalformed))
}

func (s *session) judgeHello(payload []byte) outcome {
	h, err := decodeHello(payload)
	if err != nil {
		return s.refuse(VerdictMalformed, err)
	}
	if h.version != ProtocolVersion {
		return s.refuse(VerdictVersion, fmt.Errorf("cyclone: peer speaks handshake version %d, not %d", h.version, ProtocolVersion))
	}
	if h.fingerprint == s.schema.Fingerprint() {
		return s.accept()
	}

	var need []queryItem
	for _, item := range h.items {
		check, fieldCount := s.schema.checkMessage(item.id, item.fieldCount, item.fingerprint)
		switch check {
		case checkReject:
			return s.refuse(VerdictConflict, fmt.Errorf(
				"cyclone: message 0x%08X: the fields both ends carry do not agree", item.id))
		case checkNeedPrefix:
			need = append(need, queryItem{id: item.id, fieldCount: fieldCount})
		}
	}
	if len(need) == 0 {
		return s.accept()
	}

	f, err := encodeHandshake(encodeQuery(need))
	if err != nil {
		return s.refuse(VerdictMalformed, err)
	}
	s.pendingQuery = need
	return outcome{frame: f}
}

func (s *session) judgeQueryReply(payload []byte) outcome {
	items, err := decodeQueryReply(payload)
	if err != nil {
		return s.refuse(VerdictMalformed, err)
	}
	if len(items) != len(s.pendingQuery) {
		return s.refuse(VerdictMalformed, fmt.Errorf("%w: query reply carries %d items, %d were asked for",
			errHandshakeMalformed, len(items), len(s.pendingQuery)))
	}
	for i, asked := range s.pendingQuery {
		if items[i].id != asked.id {
			return s.refuse(VerdictMalformed, fmt.Errorf("%w: query reply item %d is message 0x%08X, 0x%08X was asked for",
				errHandshakeMalformed, i, items[i].id, asked.id))
		}
		local, ok := s.schema.prefix(asked.id, asked.fieldCount)
		if !ok {
			return s.refuse(VerdictMalformed, fmt.Errorf("%w: no local prefix for message 0x%08X at field %d",
				errHandshakeMalformed, asked.id, asked.fieldCount))
		}
		if items[i].fingerprint != local {
			return s.refuse(VerdictConflict, fmt.Errorf(
				"cyclone: message 0x%08X: the fields both ends carry do not agree", asked.id))
		}
	}
	s.pendingQuery = nil
	return s.accept()
}

func (s *session) accept() outcome {
	f, err := encodeHandshake([]byte{byte(VerdictAccept)})
	if err != nil {
		return s.refuse(VerdictMalformed, err)
	}
	s.state = StateReady
	return outcome{frame: f, event: &Event{Kind: EventReady}}
}

func (s *session) refuse(v Verdict, err error) outcome {
	out := s.terminate(EventHandshakeFailed, v, err)
	if f, encodeErr := encodeHandshake([]byte{byte(v)}); encodeErr == nil {
		out.frame = f
	}
	return out
}

func (s *session) tick(now time.Time) outcome {
	if s.state == StateClosed {
		return outcome{}
	}
	if s.role == roleClient && s.state == StateHandshaking {
		if now.Sub(s.startedAt) >= s.cfg.HandshakeTimeout {
			return s.terminate(EventHandshakeFailed, VerdictMalformed,
				fmt.Errorf("cyclone: no verdict within %s", s.cfg.HandshakeTimeout))
		}
		return outcome{}
	}

	if s.probing {
		if now.Sub(s.probeSentAt) >= s.cfg.HeartbeatTimeout {
			return s.terminate(EventDisconnected, VerdictAccept,
				fmt.Errorf("cyclone: peer did not answer a probe within %s", s.cfg.HeartbeatTimeout))
		}
		return outcome{}
	}

	if now.Sub(s.lastActivity) >= s.silenceWindow() {
		s.probing = true
		s.probeSentAt = now
		return outcome{frame: probeFrame}
	}
	return outcome{}
}

func (s *session) silenceWindow() time.Duration {
	if s.role == roleServer && s.state == StateHandshaking {
		return s.cfg.HandshakeTimeout
	}
	return s.cfg.HeartbeatInterval
}

func (s *session) transportClosed(cause error) outcome {
	if s.terminated || s.state == StateClosed {
		s.state = StateClosed
		return outcome{}
	}
	if cause == nil {
		cause = errors.New("cyclone: peer closed the connection")
	}
	return s.terminate(EventDisconnected, VerdictAccept, cause)
}

func (s *session) close() {
	s.state = StateClosed
	s.terminated = true
}

func (s *session) terminate(kind EventKind, v Verdict, err error) outcome {
	if s.terminated {
		s.state = StateClosed
		return outcome{}
	}
	s.terminated = true
	s.state = StateClosed
	return outcome{event: &Event{Kind: kind, Verdict: v, Err: err}}
}
