package cyclone

import "errors"

var (
	ErrNotReady        = errors.New("cyclone: session is not ready")
	ErrCongested       = errors.New("cyclone: outbound frame is still in flight")
	ErrPayloadTooLarge = errors.New("cyclone: payload exceeds the 16 MiB message limit")
	ErrTransportLimit  = errors.New("cyclone: frame exceeds what the transport can carry")
	ErrSessionClosed   = errors.New("cyclone: session is closed")
	ErrUnknownPeer     = errors.New("cyclone: no such peer")
	ErrUnsupported     = errors.New("cyclone: non-blocking sockets are not implemented on this platform")
)
