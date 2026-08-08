package cyclone

import (
	"errors"
	"net"
	"time"
)

// ClientEventKind identifies what a ClientEvent carries.
type ClientEventKind int

const (
	ClientConnected ClientEventKind = iota
	// ClientMessageReceived fires whether or not an On handler was also
	// registered for the message's id; both this and the matching
	// handler(s), if any, always run.
	ClientMessageReceived
	ClientPongReceived
	ClientDisconnected
)

// ClientEvent is what Client.Poll hands back.
type ClientEvent struct {
	Kind ClientEventKind
	// Message is valid when Kind == ClientMessageReceived.
	Message Message
}

// Client is one client connection, plus typed message routing - the Go
// counterpart of Cyclone.Unity's CycloneClient.
type Client struct {
	connection *Connection
	handlers   map[uint32][]func([]byte)
}

// NewClient creates a client with no connection yet - call Connect.
func NewClient() *Client {
	return &Client{handlers: make(map[uint32][]func([]byte))}
}

// Connect dials addr and blocks until the OS completes (or rejects) the TCP
// handshake - there is no non-blocking connect here, the same way there is
// no async runtime in this package at all (see the package doc comment).
// Returns once connected; the background reader goroutine and heartbeat
// start immediately after.
func (c *Client) Connect(addr string, heartbeatInterval, heartbeatTimeout time.Duration) error {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return err
	}
	c.connection = NewConnection(conn, heartbeatInterval, heartbeatTimeout)
	return nil
}

func (c *Client) IsConnected() bool {
	return c.connection != nil && c.connection.IsConnected()
}

func (c *Client) Send(message Message) error {
	if c.connection == nil {
		return errors.New("cyclone: client is not connected")
	}
	return c.connection.Send(message)
}

func (c *Client) Disconnect() {
	if c.connection != nil {
		c.connection.Disconnect()
	}
}

// registerRaw is what the package-level generic function On calls into -
// see the package doc comment for why On cannot be a method of Client.
func (c *Client) registerRaw(messageID uint32, handler func([]byte)) {
	c.handlers[messageID] = append(c.handlers[messageID], handler)
}

// On registers a typed handler for messageID on client.
//
// This is a package-level function, not a Client method, because Go does
// not allow a method to introduce its own type parameter beyond its
// receiver's - see the package doc comment. decode is func([]byte) T,
// typically a one-line adapter that builds a generated codec's Reader and
// calls its Decode; handler is func(T). Multiple handlers on one message id
// all run, in registration order, and nothing here recovers a panic either
// one raises.
func On[T any](client *Client, messageID uint32, decode func([]byte) T, handler func(T)) {
	client.registerRaw(messageID, func(payload []byte) {
		handler(decode(payload))
	})
}

// Poll drains the underlying connection, dispatches On handlers, and
// returns the raw events - never blocks. Call this once a tick/frame; see
// the package doc comment for why nothing here needs the caller to run
// its own goroutine.
func (c *Client) Poll() []ClientEvent {
	if c.connection == nil {
		return nil
	}

	wasConnected := c.connection.IsConnected()
	rawEvents := c.connection.Poll()

	var events []ClientEvent
	if !wasConnected && c.connection.IsConnected() {
		events = append(events, ClientEvent{Kind: ClientConnected})
	}

	for _, event := range rawEvents {
		switch event.Kind {
		case ConnMessageReceived:
			for _, handler := range c.handlers[event.Message.ID] {
				handler(event.Message.Payload)
			}
			events = append(events, ClientEvent{Kind: ClientMessageReceived, Message: event.Message})
		case ConnPongReceived:
			events = append(events, ClientEvent{Kind: ClientPongReceived})
		case ConnDisconnected:
			events = append(events, ClientEvent{Kind: ClientDisconnected})
		case ConnPingReceived:
			// Internal to the heartbeat; nothing client-visible about it.
		}
	}

	return events
}
