//go:build !unix

package cyclone

import "net"

func newTCPTransport(*net.TCPConn) (Transport, error) { return nil, ErrUnsupported }

func newUDPTransport(*net.UDPConn) (Transport, error) { return nil, ErrUnsupported }

func newTCPListener(*net.TCPListener, Config) (listener, error) { return nil, ErrUnsupported }

func newUDPListener(*net.UDPConn, Config) (listener, error) { return nil, ErrUnsupported }
