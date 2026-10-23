//go:build unix || windows

package fomoxa

import (
	"bytes"
	"encoding/binary"
	"net"
	"testing"
	"time"
)

func udpDatagram(session uint64, frame ...byte) []byte {
	out := make([]byte, udpHeader+len(frame))
	binary.LittleEndian.PutUint64(out, session)
	copy(out[udpHeader:], frame)
	return out
}

func udpSocket(t *testing.T) *net.UDPConn {
	t.Helper()
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func udpTestListener(t *testing.T) (listener, *net.UDPAddr) {
	t.Helper()
	l, err := newUDPListener(udpSocket(t), DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(l.close)
	return l, l.addr().(*net.UDPAddr)
}

func pollUDP(t *testing.T, l listener) []Transport {
	t.Helper()
	time.Sleep(50 * time.Millisecond)
	arrivals, err := l.poll()
	if err != nil {
		t.Fatal(err)
	}
	return arrivals
}

func udpReceive(p Transport) ([]byte, Status) {
	buf := make([]byte, 64)
	n, _, status := p.Receive(buf)
	return buf[:n], status
}

func readSession(t *testing.T, conn *net.UDPConn) (uint64, int) {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	buf := make([]byte, 64)
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatal(err)
	}
	return binary.LittleEndian.Uint64(buf), n
}

func dialRaw(t *testing.T, to *net.UDPAddr) *net.UDPConn {
	t.Helper()
	conn, err := net.DialUDP("udp", nil, to)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func TestUDPPeerKeepsItsSessionWhenItsAddressChanges(t *testing.T) {
	l, target := udpTestListener(t)
	before := dialRaw(t, target)
	after := dialRaw(t, target)

	if _, err := before.Write(udpDatagram(0, 1, 2)); err != nil {
		t.Fatal(err)
	}
	arrivals := pollUDP(t, l)
	if len(arrivals) != 1 {
		t.Fatalf("arrivals = %d, want 1", len(arrivals))
	}
	peer := arrivals[0].(*udpPeer)
	if peer.session == 0 {
		t.Fatal("the server assigned id 0")
	}
	if got, status := udpReceive(peer); status != StatusOK || !bytes.Equal(got, []byte{1, 2}) {
		t.Fatalf("first frame = %v %v", got, status)
	}

	if status := peer.Send([]byte{9}); status != StatusOK {
		t.Fatalf("send = %v", status)
	}
	session, n := readSession(t, before)
	if session != peer.session || n != udpHeader+1 {
		t.Fatalf("reply = id %d, %d bytes", session, n)
	}

	if _, err := after.Write(udpDatagram(session, 3)); err != nil {
		t.Fatal(err)
	}
	pollUDP(t, l)
	if got, status := udpReceive(peer); status != StatusOK || !bytes.Equal(got, []byte{3}) {
		t.Fatalf("frame from the new address = %v %v", got, status)
	}
	if status := peer.Send([]byte{10}); status != StatusOK {
		t.Fatalf("send = %v", status)
	}
	if moved, _ := readSession(t, after); moved != session {
		t.Fatalf("the reply did not reach the new address")
	}
}

func TestUDPServerDropsUnknownIdsAndShortDatagrams(t *testing.T) {
	l, target := udpTestListener(t)
	first := dialRaw(t, target)
	second := dialRaw(t, target)

	if _, err := first.Write(udpDatagram(0, 1)); err != nil {
		t.Fatal(err)
	}
	peer := pollUDP(t, l)[0].(*udpPeer)
	udpReceive(peer)
	home := peer.key

	_, _ = first.Write(make([]byte, udpHeader))
	_, _ = second.Write(udpDatagram(peer.session^1, 5))
	pollUDP(t, l)
	if _, status := udpReceive(peer); status != StatusPending {
		t.Fatalf("a dropped datagram reached the peer: %v", status)
	}
	if peer.key != home {
		t.Fatal("an unknown id moved a peer")
	}

	_, _ = second.Write(udpDatagram(0, 6))
	other := pollUDP(t, l)[0].(*udpPeer)
	taken := other.key
	_, _ = first.Write(udpDatagram(other.session, 7))
	pollUDP(t, l)
	if other.key != taken || peer.key != home {
		t.Fatal("a peer was moved onto an address another peer holds")
	}
}

func TestUDPClientKeepsTheFirstIdItReceives(t *testing.T) {
	server := udpSocket(t)
	conn, err := net.DialUDP("udp", nil, server.LocalAddr().(*net.UDPAddr))
	if err != nil {
		t.Fatal(err)
	}
	client, err := newUDPTransport(conn)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if status := client.Send([]byte{1}); status != StatusOK {
		t.Fatalf("send = %v", status)
	}
	request := make([]byte, 64)
	_ = server.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	n, from, err := server.ReadFromUDP(request)
	if err != nil {
		t.Fatal(err)
	}
	if n != udpHeader+1 || binary.LittleEndian.Uint64(request) != 0 {
		t.Fatalf("first datagram = %d bytes, id %d", n, binary.LittleEndian.Uint64(request))
	}

	_, _ = server.WriteToUDP(udpDatagram(0, 2), from)
	_, _ = server.WriteToUDP(udpDatagram(11, 3), from)
	_, _ = server.WriteToUDP(udpDatagram(22, 4), from)
	time.Sleep(50 * time.Millisecond)

	if got, status := udpReceive(client); status != StatusOK || !bytes.Equal(got, []byte{3}) {
		t.Fatalf("first accepted frame = %v %v", got, status)
	}
	if _, status := udpReceive(client); status != StatusPending {
		t.Fatalf("a frame with another id was delivered: %v", status)
	}

	client.Send([]byte{8})
	_ = server.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	if _, _, err := server.ReadFromUDP(request); err != nil {
		t.Fatal(err)
	}
	if binary.LittleEndian.Uint64(request) != 11 {
		t.Fatalf("the client sent id %d, want 11", binary.LittleEndian.Uint64(request))
	}
}
