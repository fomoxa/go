//go:build unix

package fomoxa

import (
	"errors"
	"fmt"
	"net"
	"os"
	"syscall"
)

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
	return &tcpListener{listener: l, raw: raw, budget: cfg.MaxFramesPerTick}, nil
}

func (l *tcpListener) poll() ([]Transport, error) {
	var arrivals []Transport
	for i := 0; i < l.budget; i++ {
		fd, err := rawAccept(l.raw)
		if err != nil {
			if wouldBlock(err) || errors.Is(err, syscall.ECONNABORTED) {
				return arrivals, nil
			}
			return arrivals, err
		}
		if fd < 0 {
			return arrivals, nil
		}

		conn, err := adoptTCP(fd)
		if err != nil {
			continue
		}
		t, err := newTCPTransport(conn)
		if err != nil {
			_ = conn.Close()
			continue
		}
		arrivals = append(arrivals, t)
	}
	return arrivals, nil
}

func adoptTCP(fd int) (*net.TCPConn, error) {
	if err := syscall.SetNonblock(fd, true); err != nil {
		_ = syscall.Close(fd)
		return nil, err
	}
	file := os.NewFile(uintptr(fd), "fomoxa-tcp")
	conn, err := net.FileConn(file)
	_ = file.Close()
	if err != nil {
		return nil, err
	}
	tcp, ok := conn.(*net.TCPConn)
	if !ok {
		_ = conn.Close()
		return nil, fmt.Errorf("fomoxa: accepted connection is %T, not TCP", conn)
	}
	return tcp, nil
}

func (l *tcpListener) forget(Transport) {}

func (l *tcpListener) addr() net.Addr { return l.listener.Addr() }

func (l *tcpListener) close() { _ = l.listener.Close() }
