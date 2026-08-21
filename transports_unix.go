//go:build unix

package cyclone

import (
	"errors"
	"fmt"
	"net"
	"os"
	"syscall"
)

const (
	maxDatagram        = 65535
	maxDatagramPayload = 65507
)

type tcpTransport struct {
	conn    *net.TCPConn
	raw     syscall.RawConn
	backlog []byte
	closed  bool
	err     error
}

func newTCPTransport(conn *net.TCPConn) (Transport, error) {
	raw, err := conn.SyscallConn()
	if err != nil {
		return nil, err
	}
	_ = conn.SetNoDelay(true)
	return &tcpTransport{conn: conn, raw: raw}, nil
}

func (t *tcpTransport) Kind() Kind { return KindStream }

func (t *tcpTransport) LastError() error { return t.err }

func (t *tcpTransport) Send(b []byte) Status {
	if t.closed {
		return StatusClosed
	}
	if len(t.backlog) > 0 {
		if status := t.pushBacklog(); status != StatusOK {
			return status
		}
		if len(t.backlog) > 0 {
			return StatusPending
		}
	}

	n, err := rawWrite(t.raw, b)
	if n == len(b) {
		return StatusOK
	}
	if err != nil && !wouldBlock(err) {
		return t.fail(err)
	}
	if n <= 0 {
		return StatusPending
	}
	t.backlog = append(t.backlog, b[n:]...)
	return StatusOK
}

func (t *tcpTransport) pushBacklog() Status {
	for len(t.backlog) > 0 {
		n, err := rawWrite(t.raw, t.backlog)
		if n > 0 {
			t.backlog = append(t.backlog[:0], t.backlog[n:]...)
		}
		if err != nil {
			if wouldBlock(err) {
				return StatusOK
			}
			return t.fail(err)
		}
		if n == 0 {
			return StatusOK
		}
	}
	return StatusOK
}

func (t *tcpTransport) Receive(buf []byte) (int, int, Status) {
	if t.closed {
		return 0, 0, StatusClosed
	}
	if len(t.backlog) > 0 {
		if status := t.pushBacklog(); status != StatusOK {
			return 0, 0, status
		}
	}

	n, err := rawRead(t.raw, buf)
	if err != nil {
		if wouldBlock(err) {
			return 0, 0, StatusPending
		}
		return 0, 0, t.fail(err)
	}
	if n == 0 {
		t.closed = true
		return 0, 0, StatusClosed
	}
	return n, 0, StatusOK
}

func (t *tcpTransport) fail(err error) Status {
	t.closed = true
	t.err = err
	return StatusError
}

func (t *tcpTransport) CloseSend() {
	if t.closed {
		return
	}
	t.pushBacklog()
	_ = t.conn.CloseWrite()
}

func (t *tcpTransport) Close() {
	t.closed = true
	_ = t.conn.Close()
}

type udpTransport struct {
	conn      *net.UDPConn
	raw       syscall.RawConn
	scratch   []byte
	have      int
	hasPacket bool
	closed    bool
	err       error
}

func newUDPTransport(conn *net.UDPConn) (Transport, error) {
	raw, err := conn.SyscallConn()
	if err != nil {
		return nil, err
	}
	return &udpTransport{conn: conn, raw: raw, scratch: make([]byte, maxDatagram)}, nil
}

func (u *udpTransport) Kind() Kind { return KindPacket }

func (u *udpTransport) LastError() error { return u.err }

func (u *udpTransport) Send(b []byte) Status {
	if u.closed {
		return StatusClosed
	}
	if len(b) > maxDatagramPayload {
		return StatusTooLarge
	}
	n, err := rawWrite(u.raw, b)
	if err != nil {
		if wouldBlock(err) {
			return StatusPending
		}
		if errors.Is(err, syscall.EMSGSIZE) {
			return StatusTooLarge
		}
		u.closed = true
		u.err = err
		return StatusError
	}
	if n != len(b) {
		return StatusTooLarge
	}
	return StatusOK
}

func (u *udpTransport) Receive(buf []byte) (int, int, Status) {
	if !u.hasPacket {
		if u.closed {
			return 0, 0, StatusClosed
		}
		n, err := rawRead(u.raw, u.scratch)
		if err != nil {
			if wouldBlock(err) {
				return 0, 0, StatusPending
			}
			if errors.Is(err, syscall.ECONNREFUSED) {
				return 0, 0, StatusPending
			}
			u.closed = true
			u.err = err
			return 0, 0, StatusError
		}
		u.have = n
		u.hasPacket = true
	}

	if u.have > len(buf) {
		return 0, u.have, StatusTooSmall
	}
	copy(buf, u.scratch[:u.have])
	n := u.have
	u.have = 0
	u.hasPacket = false
	return n, 0, StatusOK
}

func (u *udpTransport) CloseSend() {}

func (u *udpTransport) Close() {
	u.closed = true
	_ = u.conn.Close()
}

type udpPeer struct {
	owner  *udpListener
	addr   syscall.Sockaddr
	key    string
	inbox  [][]byte
	closed bool
}

func (p *udpPeer) Kind() Kind { return KindPacket }

func (p *udpPeer) Send(b []byte) Status {
	if p.closed || p.owner == nil {
		return StatusClosed
	}
	if len(b) > maxDatagramPayload {
		return StatusTooLarge
	}
	err := rawSendto(p.owner.raw, b, p.addr)
	if err != nil {
		if wouldBlock(err) {
			return StatusPending
		}
		if errors.Is(err, syscall.EMSGSIZE) {
			return StatusTooLarge
		}
		return StatusError
	}
	return StatusOK
}

func (p *udpPeer) Receive(buf []byte) (int, int, Status) {
	if len(p.inbox) == 0 {
		if p.closed {
			return 0, 0, StatusClosed
		}
		return 0, 0, StatusPending
	}
	packet := p.inbox[0]
	if len(packet) > len(buf) {
		return 0, len(packet), StatusTooSmall
	}
	copy(buf, packet)
	p.inbox = append(p.inbox[:0], p.inbox[1:]...)
	return len(packet), 0, StatusOK
}

func (p *udpPeer) CloseSend() {}

func (p *udpPeer) Close() {
	p.closed = true
	p.inbox = nil
	if p.owner != nil {
		p.owner.forget(p)
		p.owner = nil
	}
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
	file := os.NewFile(uintptr(fd), "cyclone-tcp")
	conn, err := net.FileConn(file)
	_ = file.Close()
	if err != nil {
		return nil, err
	}
	tcp, ok := conn.(*net.TCPConn)
	if !ok {
		_ = conn.Close()
		return nil, fmt.Errorf("cyclone: accepted connection is %T, not TCP", conn)
	}
	return tcp, nil
}

func (l *tcpListener) forget(Transport) {}

func (l *tcpListener) addr() net.Addr { return l.listener.Addr() }

func (l *tcpListener) close() { _ = l.listener.Close() }

type udpListener struct {
	conn    *net.UDPConn
	raw     syscall.RawConn
	scratch []byte
	peers   map[string]*udpPeer
	cfg     Config
}

func newUDPListener(conn *net.UDPConn, cfg Config) (listener, error) {
	raw, err := conn.SyscallConn()
	if err != nil {
		return nil, err
	}
	return &udpListener{
		conn:    conn,
		raw:     raw,
		scratch: make([]byte, maxDatagram),
		peers:   make(map[string]*udpPeer),
		cfg:     cfg,
	}, nil
}

func (l *udpListener) poll() ([]Transport, error) {
	var arrivals []Transport
	for i := 0; i < l.cfg.MaxDatagramsPerTick; i++ {
		n, from, err := rawRecvfrom(l.raw, l.scratch)
		if err != nil {
			if wouldBlock(err) || errors.Is(err, syscall.ECONNREFUSED) {
				return arrivals, nil
			}
			return arrivals, err
		}
		if from == nil {
			continue
		}

		key := sockaddrKey(from)
		peer, known := l.peers[key]
		if !known {
			peer = &udpPeer{owner: l, addr: from, key: key}
			l.peers[key] = peer
			arrivals = append(arrivals, peer)
		}
		if len(peer.inbox) >= l.cfg.MaxPeerBacklog {
			continue
		}
		packet := make([]byte, n)
		copy(packet, l.scratch[:n])
		peer.inbox = append(peer.inbox, packet)
	}
	return arrivals, nil
}

func (l *udpListener) forget(t Transport) {
	if peer, ok := t.(*udpPeer); ok {
		delete(l.peers, peer.key)
	}
}

func (l *udpListener) addr() net.Addr { return l.conn.LocalAddr() }

func (l *udpListener) close() {
	l.peers = make(map[string]*udpPeer)
	_ = l.conn.Close()
}

func sockaddrKey(sa syscall.Sockaddr) string {
	switch a := sa.(type) {
	case *syscall.SockaddrInet4:
		return fmt.Sprintf("4:%d.%d.%d.%d:%d", a.Addr[0], a.Addr[1], a.Addr[2], a.Addr[3], a.Port)
	case *syscall.SockaddrInet6:
		return fmt.Sprintf("6:%x:%d:%d", a.Addr, a.Port, a.ZoneId)
	default:
		return fmt.Sprintf("?:%v", sa)
	}
}
