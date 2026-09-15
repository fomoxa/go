//go:build windows

package fomoxa

import (
	"net"
	"syscall"

	"golang.org/x/sys/windows"
)

type socketStream struct {
	handle windows.Handle
}

func (s *socketStream) read(p []byte) (int, error) { return socketRecv(s.handle, p) }

func (s *socketStream) write(p []byte) (int, error) { return socketSend(s.handle, p) }

func (s *socketStream) closeWrite() { _ = windows.Shutdown(s.handle, windows.SHUT_WR) }

func (s *socketStream) close() {
	if s.handle == windows.InvalidHandle {
		return
	}
	_ = windows.Closesocket(s.handle)
	s.handle = windows.InvalidHandle
}

type tcpListener struct {
	listener *net.TCPListener
	raw      syscall.RawConn
	budget   int
}

func newTCPListener(l *net.TCPListener, cfg Config) (listener, error) {
	raw, err := l.SyscallConn()
	if err != nil {
		return nil, err
	}
	if err := prepareSocket(raw); err != nil {
		return nil, err
	}
	return &tcpListener{listener: l, raw: raw, budget: cfg.MaxFramesPerTick}, nil
}

func (l *tcpListener) poll() ([]Transport, error) {
	var arrivals []Transport
	for i := 0; i < l.budget; i++ {
		handle, err := rawAccept(l.raw)
		if err != nil {
			if wouldBlock(err) || peerRefused(err) {
				return arrivals, nil
			}
			return arrivals, err
		}

		t, err := adoptSocket(handle)
		if err != nil {
			_ = windows.Closesocket(handle)
			continue
		}
		arrivals = append(arrivals, t)
	}
	return arrivals, nil
}

func adoptSocket(handle windows.Handle) (Transport, error) {
	if err := setNonblocking(handle); err != nil {
		return nil, err
	}
	_ = windows.SetsockoptInt(handle, windows.IPPROTO_TCP, windows.TCP_NODELAY, 1)
	return &tcpTransport{sock: &socketStream{handle: handle}}, nil
}

func (l *tcpListener) forget(Transport) {}

func (l *tcpListener) addr() net.Addr { return l.listener.Addr() }

func (l *tcpListener) close() { _ = l.listener.Close() }
