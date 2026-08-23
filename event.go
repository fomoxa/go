package fomoxa

import "fmt"

type EventKind int

const (
	EventConnected EventKind = iota
	EventReady
	EventHandshakeFailed
	EventMessage
	EventProbe
	EventAck
	EventDisconnected
)

func (k EventKind) String() string {
	switch k {
	case EventConnected:
		return "connected"
	case EventReady:
		return "ready"
	case EventHandshakeFailed:
		return "handshake failed"
	case EventMessage:
		return "message"
	case EventProbe:
		return "probe"
	case EventAck:
		return "ack"
	case EventDisconnected:
		return "disconnected"
	default:
		return fmt.Sprintf("event %d", int(k))
	}
}

type PeerID uint64

type Event struct {
	Kind      EventKind
	Peer      PeerID
	MessageID uint32
	Payload   []byte
	Verdict   Verdict
	Err       error
}

type State int

const (
	StateHandshaking State = iota
	StateReady
	StateClosed
)

func (s State) String() string {
	switch s {
	case StateHandshaking:
		return "handshaking"
	case StateReady:
		return "ready"
	case StateClosed:
		return "closed"
	default:
		return fmt.Sprintf("state %d", int(s))
	}
}
