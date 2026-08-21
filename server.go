package cyclone

import (
	"fmt"
	"net"
	"time"
)

type listener interface {
	poll() ([]Transport, error)
	forget(Transport)
	addr() net.Addr
	close()
}

type Server struct {
	schema   *Schema
	cfg      Config
	listener listener

	peers   map[PeerID]*Conn
	order   []PeerID
	closing []PeerID
	nextID  PeerID

	events []Event
	err    error
	closed bool
}

func newServer(l listener, schema *Schema, cfg Config) *Server {
	return &Server{
		schema:   schema,
		cfg:      cfg,
		listener: l,
		peers:    make(map[PeerID]*Conn),
	}
}

func (s *Server) Addr() net.Addr { return s.listener.addr() }

func (s *Server) Err() error { return s.err }

func (s *Server) Peers() []PeerID {
	out := make([]PeerID, len(s.order))
	copy(out, s.order)
	return out
}

func (s *Server) State(peer PeerID) (State, bool) {
	c, ok := s.peers[peer]
	if !ok {
		return StateClosed, false
	}
	return c.State(), true
}

func (s *Server) Tick(now time.Time) []Event {
	s.events = nil
	if s.closed {
		return nil
	}

	for _, id := range s.closing {
		s.retire(id)
	}
	s.closing = s.closing[:0]

	arrivals, err := s.listener.poll()
	if err != nil {
		s.err = err
	}
	for _, t := range arrivals {
		s.adopt(t)
	}

	for _, id := range s.order {
		c, ok := s.peers[id]
		if !ok {
			continue
		}
		s.events = append(s.events, c.Tick(now)...)
		if c.finished() {
			s.closing = append(s.closing, id)
		}
	}
	return s.events
}

func (s *Server) adopt(t Transport) {
	s.nextID++
	id := s.nextID
	c := newServerConn(t, s.schema, s.cfg, id)
	s.peers[id] = c
	s.order = append(s.order, id)
}

func (s *Server) retire(id PeerID) {
	c, ok := s.peers[id]
	if !ok {
		return
	}
	c.finish()
	s.listener.forget(c.transport)
	delete(s.peers, id)
	for i, other := range s.order {
		if other == id {
			s.order = append(s.order[:i], s.order[i+1:]...)
			break
		}
	}
}

func (s *Server) Send(peer PeerID, messageID uint32, payload []byte) error {
	c, ok := s.peers[peer]
	if !ok {
		return fmt.Errorf("%w: %d", ErrUnknownPeer, peer)
	}
	return c.Send(messageID, payload)
}

func (s *Server) Disconnect(peer PeerID) {
	c, ok := s.peers[peer]
	if !ok {
		return
	}
	_ = c.Close()
	s.listener.forget(c.transport)
	delete(s.peers, peer)
	for i, other := range s.order {
		if other == peer {
			s.order = append(s.order[:i], s.order[i+1:]...)
			break
		}
	}
}

func (s *Server) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	for _, id := range append([]PeerID(nil), s.order...) {
		if c, ok := s.peers[id]; ok {
			_ = c.Close()
		}
		delete(s.peers, id)
	}
	s.order = nil
	s.closing = nil
	s.listener.close()
	return nil
}
