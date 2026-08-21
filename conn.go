package cyclone

import (
	"fmt"
	"time"
)

type Conn struct {
	transport Transport
	source    *frameSource
	session   *session
	cfg       Config

	peer      PeerID
	outbound  [][]byte
	dead      bool
	deadCause error
	announced bool
	closed    bool
	events    []Event
}

func NewConn(t Transport, schema *Schema, cfg Config) (*Conn, error) {
	if t == nil {
		return nil, fmt.Errorf("cyclone: transport is nil")
	}
	if schema == nil {
		return nil, fmt.Errorf("cyclone: schema is nil")
	}
	if _, err := encodeHandshake(encodeHello(schema)); err != nil {
		return nil, err
	}
	cfg = cfg.normalized()
	return &Conn{
		transport: t,
		source:    newFrameSource(t, cfg.ReadBufferSize),
		session:   newSession(roleClient, schema, cfg),
		cfg:       cfg,
	}, nil
}

func NewPeerConn(t Transport, schema *Schema, cfg Config) (*Conn, error) {
	if t == nil {
		return nil, fmt.Errorf("cyclone: transport is nil")
	}
	if schema == nil {
		return nil, fmt.Errorf("cyclone: schema is nil")
	}
	return newServerConn(t, schema, cfg.normalized(), 0), nil
}

func newServerConn(t Transport, schema *Schema, cfg Config, peer PeerID) *Conn {
	return &Conn{
		transport: t,
		source:    newFrameSource(t, cfg.ReadBufferSize),
		session:   newSession(roleServer, schema, cfg),
		cfg:       cfg,
		peer:      peer,
	}
}

func (c *Conn) State() State { return c.session.state }

func (c *Conn) Peer() PeerID { return c.peer }

func (c *Conn) finished() bool { return c.dead && c.session.terminated }

func (c *Conn) finish() {
	if c.closed {
		return
	}
	c.closed = true
	c.flush()
	c.transport.CloseSend()
	c.transport.Close()
}

func (c *Conn) Transport() Transport { return c.transport }

func (c *Conn) Tick(now time.Time) []Event {
	c.events = nil
	if c.closed {
		return nil
	}

	if !c.announced {
		c.announced = true
		c.events = append(c.events, Event{Kind: EventConnected, Peer: c.peer})
		c.apply(c.session.start(now))
	}

	if !c.dead {
		c.flush()
	}
	if !c.dead {
		c.drain(now)
	}
	if !c.dead {
		c.apply(c.session.tick(now))
	}
	if c.dead {
		c.apply(c.session.transportClosed(c.deadCause))
	}
	return c.events
}

func (c *Conn) Send(messageID uint32, payload []byte) error {
	if c.closed {
		return ErrSessionClosed
	}
	if c.session.state != StateReady {
		return ErrNotReady
	}
	if len(payload) > MaxMessagePayload {
		return fmt.Errorf("%w: %d bytes", ErrPayloadTooLarge, len(payload))
	}
	if len(c.outbound) > 0 {
		return ErrCongested
	}

	f, err := encodeData(messageID, payload)
	if err != nil {
		return err
	}
	switch c.transport.Send(f) {
	case StatusOK:
		return nil
	case StatusPending:
		c.outbound = append(c.outbound, f)
		return nil
	case StatusTooLarge:
		return fmt.Errorf("%w: %d bytes", ErrTransportLimit, len(f))
	case StatusClosed:
		c.markDead(transportError(c.transport))
		return ErrSessionClosed
	default:
		c.markDead(transportError(c.transport))
		return ErrSessionClosed
	}
}

func (c *Conn) Close() error {
	if c.closed {
		return nil
	}
	c.closed = true
	c.session.close()
	c.transport.CloseSend()
	c.transport.Close()
	return nil
}

func (c *Conn) flush() {
	for len(c.outbound) > 0 {
		switch c.transport.Send(c.outbound[0]) {
		case StatusOK, StatusTooLarge:
			c.outbound = append(c.outbound[:0], c.outbound[1:]...)
		case StatusPending:
			return
		default:
			c.markDead(transportError(c.transport))
			return
		}
	}
}

func (c *Conn) drain(now time.Time) {
	budget := c.cfg.MaxFramesPerTick
	for budget > 0 {
		f, status, err := c.source.next()
		switch status {
		case srcFrame:
			c.apply(c.session.handleFrame(f, now))
			budget--
			if c.dead {
				return
			}
		case srcDropped:
			budget--
		case srcPending:
			return
		case srcClosed:
			c.markDead(err)
			return
		default:
			c.markDead(err)
			return
		}
	}
}

func (c *Conn) apply(o outcome) {
	if o.frame != nil {
		c.outbound = append(c.outbound, o.frame)
		c.flush()
	}
	if o.event == nil {
		return
	}

	event := *o.event
	event.Peer = c.peer
	c.events = append(c.events, event)

	if event.Kind == EventHandshakeFailed || event.Kind == EventDisconnected {
		if event.Kind == EventHandshakeFailed && !c.dead {
			c.flush()
			c.transport.CloseSend()
		}
		c.dead = true
	}
}

func (c *Conn) markDead(cause error) {
	if c.dead {
		return
	}
	c.dead = true
	c.deadCause = cause
}
