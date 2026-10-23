//go:build unix || windows

package fomoxa

import (
	"crypto/rand"
	"encoding/binary"
	"net"
	"syscall"
)

const (
	udpHeader          = 8
	maxDatagram        = 65535
	maxDatagramPayload = 65507 - udpHeader
)

type streamSocket interface {
	read(p []byte) (int, error)
	write(p []byte) (int, error)
	closeWrite()
	close()
}

type netStream struct {
	conn *net.TCPConn
	raw  syscall.RawConn
}

func (s netStream) read(p []byte) (int, error) { return rawRead(s.raw, p) }

func (s netStream) write(p []byte) (int, error) { return rawWrite(s.raw, p) }

func (s netStream) closeWrite() { _ = s.conn.CloseWrite() }

func (s netStream) close() { _ = s.conn.Close() }

type tcpTransport struct {
	sock    streamSocket
	backlog []byte
	closed  bool
	err     error
}

func newTCPTransport(conn *net.TCPConn) (Transport, error) {
	raw, err := conn.SyscallConn()
	if err != nil {
		return nil, err
	}
	if err := prepareSocket(raw); err != nil {
		return nil, err
	}
	_ = conn.SetNoDelay(true)
	return &tcpTransport{sock: netStream{conn: conn, raw: raw}}, nil
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

	n, err := t.sock.write(b)
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
		n, err := t.sock.write(t.backlog)
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

	n, err := t.sock.read(buf)
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
	t.sock.closeWrite()
}

func (t *tcpTransport) Close() {
	t.closed = true
	t.sock.close()
}

type udpTransport struct {
	conn      *net.UDPConn
	raw       syscall.RawConn
	scratch   []byte
	out       []byte
	session   uint64
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
	if err := prepareSocket(raw); err != nil {
		return nil, err
	}
	return &udpTransport{conn: conn, raw: raw, scratch: make([]byte, maxDatagram), out: make([]byte, maxDatagram)}, nil
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
	binary.LittleEndian.PutUint64(u.out, u.session)
	copy(u.out[udpHeader:], b)
	total := udpHeader + len(b)
	n, err := rawWrite(u.raw, u.out[:total])
	if err != nil {
		if wouldBlock(err) {
			return StatusPending
		}
		if messageTooLarge(err) {
			return StatusTooLarge
		}
		u.closed = true
		u.err = err
		return StatusError
	}
	if n != total {
		return StatusTooLarge
	}
	return StatusOK
}

func (u *udpTransport) Receive(buf []byte) (int, int, Status) {
	for !u.hasPacket {
		if u.closed {
			return 0, 0, StatusClosed
		}
		n, err := rawRead(u.raw, u.scratch)
		if err != nil {
			if wouldBlock(err) {
				return 0, 0, StatusPending
			}
			if peerRefused(err) {
				return 0, 0, StatusPending
			}
			u.closed = true
			u.err = err
			return 0, 0, StatusError
		}
		if n <= udpHeader {
			continue
		}
		received := binary.LittleEndian.Uint64(u.scratch)
		if received == 0 || (u.session != 0 && received != u.session) {
			continue
		}
		u.session = received
		u.have = n - udpHeader
		u.hasPacket = true
	}

	if u.have > len(buf) {
		return 0, u.have, StatusTooSmall
	}
	copy(buf, u.scratch[udpHeader:udpHeader+u.have])
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

// How many distinct source addresses one endpoint will track at once.
const maxTrackedPeers = 1024

type udpPeer struct {
	owner   *udpListener
	addr    sockaddr
	key     string
	session uint64
	inbox   [][]byte
	closed  bool
}

func (p *udpPeer) Kind() Kind { return KindPacket }

func (p *udpPeer) Send(b []byte) Status {
	if p.closed || p.owner == nil {
		return StatusClosed
	}
	if len(b) > maxDatagramPayload {
		return StatusTooLarge
	}
	datagram := make([]byte, udpHeader+len(b))
	binary.LittleEndian.PutUint64(datagram, p.session)
	copy(datagram[udpHeader:], b)
	err := rawSendto(p.owner.raw, datagram, p.addr)
	if err != nil {
		if wouldBlock(err) {
			return StatusPending
		}
		if messageTooLarge(err) {
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

type udpListener struct {
	conn     *net.UDPConn
	raw      syscall.RawConn
	scratch  []byte
	peers    map[string]*udpPeer
	sessions map[uint64]*udpPeer
	cfg      Config
}

func newUDPListener(conn *net.UDPConn, cfg Config) (listener, error) {
	raw, err := conn.SyscallConn()
	if err != nil {
		return nil, err
	}
	if err := prepareSocket(raw); err != nil {
		return nil, err
	}
	return &udpListener{
		conn:     conn,
		raw:      raw,
		scratch:  make([]byte, maxDatagram),
		peers:    make(map[string]*udpPeer),
		sessions: make(map[uint64]*udpPeer),
		cfg:      cfg,
	}, nil
}

func (l *udpListener) poll() ([]Transport, error) {
	var arrivals []Transport
	for i := 0; i < l.cfg.MaxDatagramsPerTick; i++ {
		n, from, err := rawRecvfrom(l.raw, l.scratch)
		if err != nil {
			if wouldBlock(err) || peerRefused(err) {
				return arrivals, nil
			}
			return arrivals, err
		}
		if from == nil {
			continue
		}

		if n <= udpHeader {
			continue
		}
		session := binary.LittleEndian.Uint64(l.scratch)
		key := sockaddrKey(from)
		var peer *udpPeer
		if session != 0 {
			owner, ok := l.sessions[session]
			if !ok {
				continue
			}
			if owner.key != key {
				if _, taken := l.peers[key]; taken {
					continue
				}
				delete(l.peers, owner.key)
				owner.addr = from
				owner.key = key
				l.peers[key] = owner
			}
			peer = owner
		} else {
			known, ok := l.peers[key]
			if ok {
				peer = known
			} else {
				// A UDP port hears from anyone, so an unbounded peer table is a
				// stream of unknown source addresses away from exhausting memory.
				// Past the ceiling a new address is treated exactly like a
				// stranger's packet: dropped silently, running sessions untouched
				// (01 §10).
				if len(l.peers) >= maxTrackedPeers {
					continue
				}
				peer = &udpPeer{owner: l, addr: from, key: key, session: l.newSession()}
				l.peers[key] = peer
				l.sessions[peer.session] = peer
				arrivals = append(arrivals, peer)
			}
		}
		if len(peer.inbox) >= l.cfg.MaxPeerBacklog {
			// The oldest goes, not the newest: a real-time peer is better
			// served by fresh data, and transport may not read the payload to
			// decide otherwise (01 §6, §12).
			peer.inbox = peer.inbox[1:]
		}
		packet := make([]byte, n-udpHeader)
		copy(packet, l.scratch[udpHeader:n])
		peer.inbox = append(peer.inbox, packet)
	}
	return arrivals, nil
}

func (l *udpListener) forget(t Transport) {
	if peer, ok := t.(*udpPeer); ok {
		delete(l.peers, peer.key)
		delete(l.sessions, peer.session)
	}
}

func (l *udpListener) newSession() uint64 {
	var bytes [udpHeader]byte
	for {
		if _, err := rand.Read(bytes[:]); err != nil {
			continue
		}
		candidate := binary.LittleEndian.Uint64(bytes[:])
		if _, taken := l.sessions[candidate]; candidate != 0 && !taken {
			return candidate
		}
	}
}

func (l *udpListener) addr() net.Addr { return l.conn.LocalAddr() }

func (l *udpListener) close() {
	l.peers = make(map[string]*udpPeer)
	l.sessions = make(map[uint64]*udpPeer)
	_ = l.conn.Close()
}
