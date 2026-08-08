// Package cyclone is a minimal, platform-agnostic Go runtime for Cyclone.
// The Go counterpart of cyclone-unity, cyclone-godot and cyclone-rust - same
// wire format (Cyclone's frame: Magic + MessageId + PayloadLength +
// Payload), same heartbeat, same idea, with nothing tying it to a specific
// engine or platform.
//
// V1 responsibilities:
//   - TCP transport (net.Conn, no external dependency)
//   - Message framing
//   - MessageId + PayloadLength
//   - Payload bounded decoding
//   - Generated codec registration
//   - Typed message handlers
//
// Generated codecs are expected to be produced by cyclonec.
//
// # A goroutine, not an event loop
//
// This package depends on nothing but the standard library - the same "no
// dependencies, nothing here could need one" rule cyclonec itself follows.
// A blocking net.Conn.Read cannot simply be selected on between Poll calls
// the way cyclone-godot's poll-based engine sockets can, so Connection
// spawns one background goroutine per connection that blocks on reads and
// forwards decoded events through a channel - the same shape
// cyclone-rust's background thread has, using Go's own concurrency
// primitives instead of std::thread/mpsc. Poll only ever drains that
// channel and ticks the heartbeat, so it never blocks and is safe to call
// once a tick/frame from a single goroutine, the same shape Cyclone.Unity's
// Pump() and cyclone-godot's poll() both have.
//
// # Naming: no Cyclone prefix
//
// Cyclone.Unity, cyclone-godot and cyclone-rust all name their types
// CycloneClient, CycloneServer, CycloneConnection, ... Go convention
// (flagged by go vet/staticcheck as a "stutter") is that a package name
// already qualifies its exports, so this package spells them Client,
// Server, Connection instead - cyclone.Client, not cyclone.CycloneClient.
// Same shape, same behavior, Go-idiomatic names.
//
// # Generics, unlike cyclone-godot - as a function, not a method
//
// GDScript has no generics, so cyclone-godot's On takes two Callables and
// loses compile-time type checking. Go has generics, but - unlike Rust or
// C# - does not allow a method to introduce its own type parameter beyond
// its receiver's, so the typed registration helper here is the
// package-level function On[T], not a Client method:
//
//	cyclone.On(client, playerEdgeID, decodePlayer, func(p Player) { ... })
package cyclone

import (
	"net"
	"sync/atomic"
	"time"
)

// ConnectionEventKind identifies what a ConnectionEvent carries.
type ConnectionEventKind int

const (
	ConnMessageReceived ConnectionEventKind = iota
	ConnPingReceived
	ConnPongReceived
	// ConnDisconnected fires at most once per Connection: the peer
	// disconnected, the socket errored, or Disconnect was called.
	ConnDisconnected
)

// ConnectionEvent is what Connection.Poll hands back - the Go counterpart
// of Cyclone.Unity's MessageReceived/PingReceived/PongReceived/Disconnected
// events.
type ConnectionEvent struct {
	Kind ConnectionEventKind
	// Message is valid when Kind == ConnMessageReceived.
	Message Message
}

// rawEventKind and rawEvent are what the background reader goroutine sends,
// before Ping/Pong have been intercepted by Poll.
type rawEventKind int

const (
	rawMessage rawEventKind = iota
	rawPing
	rawPong
	rawDisconnected
)

type rawEvent struct {
	kind    rawEventKind
	message Message
}

// Connection wraps one net.Conn, doing frame buffering and heartbeat on it -
// used by both Client (one connection) and Server (one per accepted peer),
// the same split Cyclone.Unity's CycloneConnection has and for the same
// reason: client and server share everything past "how the socket was
// obtained".
type Connection struct {
	conn      net.Conn
	heartbeat *heartbeat
	events    chan rawEvent
	connected atomic.Bool

	disconnectedReported bool
}

// NewConnection takes ownership of an already-connected conn and starts its
// background reader goroutine.
func NewConnection(conn net.Conn, heartbeatInterval, heartbeatTimeout time.Duration) *Connection {
	c := &Connection{
		conn:      conn,
		heartbeat: newHeartbeat(heartbeatInterval, heartbeatTimeout),
		// Buffered generously so a caller's Poll cadence does not have to
		// race the network; see readLoop's own docs for what happens if a
		// caller stops polling entirely.
		events: make(chan rawEvent, 256),
	}
	c.connected.Store(true)
	go c.readLoop()
	return c
}

func (c *Connection) IsConnected() bool {
	return c.connected.Load()
}

// Send writes message immediately - not queued, not batched. net.Conn's
// own contract (see the standard library docs) allows this to run
// concurrently with the background reader goroutine's reads safely.
func (c *Connection) Send(message Message) error {
	_, err := c.conn.Write(encodeFrame(message))
	return err
}

func (c *Connection) Disconnect() {
	c.connected.Store(false)
	_ = c.conn.Close()
}

// Poll drains whatever the background reader goroutine has queued since the
// last call, replies to any Ping with a Pong, and ticks the heartbeat.
// Never blocks. Call this once a tick/frame - see the package doc comment
// for why nothing here needs the caller to run its own goroutine per
// connection.
func (c *Connection) Poll() []ConnectionEvent {
	var events []ConnectionEvent

drain:
	for {
		select {
		case raw, ok := <-c.events:
			if !ok {
				break drain
			}
			switch raw.kind {
			case rawMessage:
				events = append(events, ConnectionEvent{Kind: ConnMessageReceived, Message: raw.message})
			case rawPing:
				events = append(events, ConnectionEvent{Kind: ConnPingReceived})
				_ = c.Send(Message{ID: PongID})
			case rawPong:
				c.heartbeat.markPong()
				events = append(events, ConnectionEvent{Kind: ConnPongReceived})
			case rawDisconnected:
				c.connected.Store(false)
				if !c.disconnectedReported {
					c.disconnectedReported = true
					events = append(events, ConnectionEvent{Kind: ConnDisconnected})
				}
				break drain
			}
		default:
			break drain
		}
	}

	if c.IsConnected() {
		if c.heartbeat.isTimeout() {
			c.Disconnect()
			if !c.disconnectedReported {
				c.disconnectedReported = true
				events = append(events, ConnectionEvent{Kind: ConnDisconnected})
			}
		} else if c.heartbeat.shouldPing() {
			_ = c.Send(Message{ID: PingID})
		}
	}

	return events
}

// readLoop runs on its own goroutine for the lifetime of one connection:
// blocks on reads, extracts frames from the accumulated buffer (resyncing
// on the magic bytes the same way the sibling SDKs' own read loops do), and
// forwards decoded messages - never touching anything the caller's own
// goroutine owns except through c.events and c.connected.
//
// If the caller stops calling Poll entirely, c.events (bounded, buffered)
// eventually fills; further sends fall back to a non-blocking attempt that
// simply stops this goroutine rather than leaking it blocked forever - a
// caller that never polls again was never going to see those events
// anyway.
func (c *Connection) readLoop() {
	var buffer []byte
	readBuf := make([]byte, 64*1024)

readLoop:
	for {
		n, err := c.conn.Read(readBuf)
		if n > 0 {
			buffer = append(buffer, readBuf[:n]...)

			for {
				frame, rest, status := extractFrame(buffer)
				if status == frameFatal {
					break readLoop // the peer violated the size limit: drop the connection
				}
				if status == frameIncomplete {
					break // not a full frame yet
				}
				buffer = rest

				message, ok := tryDecodeFrame(frame)
				if !ok {
					break readLoop // internally inconsistent: never guess, just stop
				}

				var event rawEvent
				switch message.ID {
				case PingID:
					event = rawEvent{kind: rawPing}
				case PongID:
					event = rawEvent{kind: rawPong}
				default:
					event = rawEvent{kind: rawMessage, message: message}
				}

				select {
				case c.events <- event:
				default:
					return // see this function's own doc comment
				}
			}
		}
		if err != nil {
			break
		}
	}

	c.connected.Store(false)
	select {
	case c.events <- rawEvent{kind: rawDisconnected}:
	default:
	}
}

// frameStatus is extractFrame's three-way result: a complete frame, not
// enough data yet, or a fatal violation of the wire format (never
// confused with "not enough data yet" - see readLoop, which treats them
// very differently).
type frameStatus int

const (
	frameComplete frameStatus = iota
	frameIncomplete
	frameFatal
)

// extractFrame finds and removes one complete frame from the front of
// buffer, resyncing on the magic bytes if garbage precedes them.
func extractFrame(buffer []byte) (frame []byte, rest []byte, status frameStatus) {
	if len(buffer) < headerSize {
		return nil, buffer, frameIncomplete
	}

	magicIndex := findMagic(buffer)
	if magicIndex < 0 {
		return nil, keepPossibleMagicPrefix(buffer), frameIncomplete
	}
	if magicIndex > 0 {
		buffer = buffer[magicIndex:]
	}
	if len(buffer) < headerSize {
		return nil, buffer, frameIncomplete
	}

	payloadLength := int(buffer[magicSize+messageIDSize]) |
		int(buffer[magicSize+messageIDSize+1])<<8 |
		int(buffer[magicSize+messageIDSize+2])<<16 |
		int(buffer[magicSize+messageIDSize+3])<<24

	if payloadLength > maxPayloadLength {
		return nil, nil, frameFatal
	}

	frameLength := headerSize + payloadLength
	if len(buffer) < frameLength {
		return nil, buffer, frameIncomplete
	}

	frame = make([]byte, frameLength)
	copy(frame, buffer[:frameLength])
	return frame, buffer[frameLength:], frameComplete
}

func findMagic(buffer []byte) int {
	for i := 0; i+1 < len(buffer); i++ {
		if buffer[i] == magic1 && buffer[i+1] == magic2 {
			return i
		}
	}
	return -1
}

func keepPossibleMagicPrefix(buffer []byte) []byte {
	if len(buffer) == 0 {
		return buffer
	}
	last := buffer[len(buffer)-1]
	if last == magic1 {
		return []byte{last}
	}
	return nil
}
