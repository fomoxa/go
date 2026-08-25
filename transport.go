package fomoxa

import "fmt"

type Kind int

const (
	KindStream Kind = iota
	KindPacket
)

func (k Kind) String() string {
	switch k {
	case KindStream:
		return "stream"
	case KindPacket:
		return "packet"
	default:
		return fmt.Sprintf("kind %d", int(k))
	}
}

type Status int

const (
	StatusOK Status = iota
	StatusPending
	StatusClosed
	StatusError
	StatusTooLarge
	StatusTooSmall
)

func (s Status) String() string {
	switch s {
	case StatusOK:
		return "ok"
	case StatusPending:
		return "pending"
	case StatusClosed:
		return "closed"
	case StatusError:
		return "error"
	case StatusTooLarge:
		return "too large"
	case StatusTooSmall:
		return "buffer too small"
	default:
		return fmt.Sprintf("status %d", int(s))
	}
}

type Transport interface {
	Kind() Kind
	Send(b []byte) Status
	Receive(buf []byte) (n int, need int, status Status)
	CloseSend()
	Close()
}

type ErrorReporter interface {
	LastError() error
}

func transportError(t Transport) error {
	if r, ok := t.(ErrorReporter); ok {
		return r.LastError()
	}
	return nil
}

type srcStatus int

const (
	srcFrame srcStatus = iota
	srcPending
	srcDropped
	srcClosed
	srcError
)

type frameSource struct {
	t        Transport
	buf      []byte
	baseSize int
	dec      *streamDecoder
}

func newFrameSource(t Transport, bufSize int) *frameSource {
	s := &frameSource{t: t, buf: make([]byte, bufSize), baseSize: bufSize}
	if t.Kind() == KindStream {
		s.dec = &streamDecoder{}
	}
	return s
}

func (s *frameSource) shrink() {
	if len(s.buf) > s.baseSize {
		s.buf = make([]byte, s.baseSize)
	}
	if s.dec != nil {
		s.dec.shrink()
	}
}

func (s *frameSource) next() (frame, srcStatus, error) {
	for {
		if s.dec != nil {
			f, ok, err := s.dec.next()
			if err != nil {
				return frame{}, srcError, err
			}
			if ok {
				return f, srcFrame, nil
			}
		}

		n, need, status := s.t.Receive(s.buf)
		switch status {
		case StatusPending:
			return frame{}, srcPending, nil
		case StatusTooSmall:
			if need <= len(s.buf) {
				need = len(s.buf) * 2
			}
			if need > MaxFrameLen {
				need = MaxFrameLen
			}
			s.buf = make([]byte, need)
			continue
		case StatusClosed:
			return frame{}, srcClosed, transportError(s.t)
		case StatusError:
			err := transportError(s.t)
			if err == nil {
				err = fmt.Errorf("fomoxa: transport failed")
			}
			return frame{}, srcError, err
		case StatusOK:
			if s.dec != nil {
				if n == 0 {
					return frame{}, srcPending, nil
				}
				s.dec.feed(s.buf[:n])
				continue
			}
			f, err := decodePacket(s.buf[:n])
			if err != nil {
				return frame{}, srcDropped, err
			}
			return f, srcFrame, nil
		default:
			return frame{}, srcError, fmt.Errorf("fomoxa: transport returned %s", status)
		}
	}
}
