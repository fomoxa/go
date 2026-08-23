package fomoxa

import "errors"

var (
	ErrNotReady        = errors.New("fomoxa: session is not ready")
	ErrCongested       = errors.New("fomoxa: outbound frame is still in flight")
	ErrPayloadTooLarge = errors.New("fomoxa: payload exceeds the 16 MiB message limit")
	ErrTransportLimit  = errors.New("fomoxa: frame exceeds what the transport can carry")
	ErrSessionClosed   = errors.New("fomoxa: session is closed")
	ErrUnknownPeer     = errors.New("fomoxa: no such peer")
	ErrUnsupported     = errors.New("fomoxa: non-blocking sockets are not implemented on this platform")
)
