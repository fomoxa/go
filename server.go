package cyclone

import (
	"errors"
	"net"
	"time"
)

// ConnectionID identifies one accepted connection for the lifetime of a
// Server. Never reused, even after the connection it named disconnects -
// stable enough to key a map of per-player state on.
type ConnectionID uint64

// ServerEventKind identifies what a ServerEvent carries.
type ServerEventKind int

const (
	ServerClientConnected ServerEventKind = iota
	ServerClientDisconnected
	ServerMessageReceived
	ServerPongReceived
)

// ServerEvent is what Server.Poll hands back.
type ServerEvent struct {
	Kind ServerEventKind
	ID   ConnectionID
	// Message is valid when Kind == ServerMessageReceived.
	Message Message
}

type serverSlot struct {
	id         ConnectionID
	connection *Connection
}

// Server listens for and manages many client connections - the Go
// counterpart of Cyclone.Unity's CycloneServer.
type Server struct {
	listener    net.Listener
	incoming    chan net.Conn
	connections []serverSlot
	nextID      uint64
	running     bool

	heartbeatInterval time.Duration
	heartbeatTimeout  time.Duration
}

// NewServer creates a server with the default 5s/15s heartbeat - call
// Start to begin listening.
func NewServer() *Server {
	return NewServerWithHeartbeat(5*time.Second, 15*time.Second)
}

// NewServerWithHeartbeat is like NewServer, but with a heartbeat
// interval/timeout applied to every connection this server accepts.
func NewServerWithHeartbeat(heartbeatInterval, heartbeatTimeout time.Duration) *Server {
	return &Server{heartbeatInterval: heartbeatInterval, heartbeatTimeout: heartbeatTimeout}
}

func (s *Server) IsRunning() bool {
	return s.running
}

// Addr is the address actually bound, once Start has succeeded - in
// particular, the real port the OS chose when addr asked for port 0.
// Binding to port 0 and reading it back here is the robust way to pick a
// port for a test or an ephemeral server: no fixed port number can
// collide with something else already using it.
func (s *Server) Addr() net.Addr {
	if s.listener == nil {
		return nil
	}
	return s.listener.Addr()
}

// Start binds addr and starts accepting connections on a background
// goroutine - see the package doc comment for why this package uses a
// goroutine instead of async I/O. Blocks only long enough to bind; it does
// not wait for a connection.
func (s *Server) Start(addr string) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	s.listener = listener
	s.incoming = make(chan net.Conn, 16)
	s.running = true

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return // the listener was closed (Stop) or errored fatally
			}
			select {
			case s.incoming <- conn:
			default:
				// The caller isn't polling fast enough to keep up with
				// accepts; refuse this one rather than block the accept
				// loop (and every other pending connection) forever.
				_ = conn.Close()
			}
		}
	}()

	return nil
}

// Stop disconnects every open connection and stops accepting new ones.
func (s *Server) Stop() {
	s.running = false
	if s.listener != nil {
		_ = s.listener.Close()
		s.listener = nil
	}
	for _, slot := range s.connections {
		slot.connection.Disconnect()
	}
	s.connections = nil
}

func (s *Server) ConnectionCount() int {
	return len(s.connections)
}

func (s *Server) ConnectionIDs() []ConnectionID {
	ids := make([]ConnectionID, len(s.connections))
	for i, slot := range s.connections {
		ids[i] = slot.id
	}
	return ids
}

func (s *Server) SendTo(id ConnectionID, message Message) error {
	for _, slot := range s.connections {
		if slot.id == id {
			return slot.connection.Send(message)
		}
	}
	return errors.New("cyclone: no connection with that id")
}

func (s *Server) Broadcast(message Message) {
	for _, slot := range s.connections {
		if slot.connection.IsConnected() {
			_ = slot.connection.Send(message)
		}
	}
}

// Poll accepts any waiting connections, then polls every open one - never
// blocks. Call this once a tick/frame.
func (s *Server) Poll() []ServerEvent {
	var events []ServerEvent

	if s.incoming != nil {
	drainIncoming:
		for {
			select {
			case conn := <-s.incoming:
				connection := NewConnection(conn, s.heartbeatInterval, s.heartbeatTimeout)
				id := ConnectionID(s.nextID)
				s.nextID++
				s.connections = append(s.connections, serverSlot{id: id, connection: connection})
				events = append(events, ServerEvent{Kind: ServerClientConnected, ID: id})
			default:
				break drainIncoming
			}
		}
	}

	var disconnected map[ConnectionID]bool
	for _, slot := range s.connections {
		for _, event := range slot.connection.Poll() {
			switch event.Kind {
			case ConnMessageReceived:
				events = append(events, ServerEvent{Kind: ServerMessageReceived, ID: slot.id, Message: event.Message})
			case ConnPongReceived:
				events = append(events, ServerEvent{Kind: ServerPongReceived, ID: slot.id})
			case ConnDisconnected:
				events = append(events, ServerEvent{Kind: ServerClientDisconnected, ID: slot.id})
				if disconnected == nil {
					disconnected = make(map[ConnectionID]bool)
				}
				disconnected[slot.id] = true
			case ConnPingReceived:
			}
		}
	}

	if len(disconnected) > 0 {
		remaining := s.connections[:0]
		for _, slot := range s.connections {
			if !disconnected[slot.id] {
				remaining = append(remaining, slot)
			}
		}
		s.connections = remaining
	}

	return events
}
