//go:build unix

package fomoxa

import (
	"errors"
	"fmt"
	"syscall"
)

type sockaddr = syscall.Sockaddr

func wouldBlock(err error) bool {
	return errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EINTR)
}

func messageTooLarge(err error) bool { return errors.Is(err, syscall.EMSGSIZE) }

func peerRefused(err error) bool { return errors.Is(err, syscall.ECONNREFUSED) }

func prepareSocket(syscall.RawConn) error { return nil }

func rawRead(rc syscall.RawConn, p []byte) (int, error) {
	var n int
	var err error
	if ctl := rc.Read(func(fd uintptr) bool {
		n, err = syscall.Read(int(fd), p)
		return true
	}); ctl != nil {
		return 0, ctl
	}
	if n < 0 {
		n = 0
	}
	return n, err
}

func rawWrite(rc syscall.RawConn, p []byte) (int, error) {
	var n int
	var err error
	if ctl := rc.Write(func(fd uintptr) bool {
		n, err = syscall.Write(int(fd), p)
		return true
	}); ctl != nil {
		return 0, ctl
	}
	if n < 0 {
		n = 0
	}
	return n, err
}

func rawRecvfrom(rc syscall.RawConn, p []byte) (int, sockaddr, error) {
	var n int
	var from sockaddr
	var err error
	if ctl := rc.Read(func(fd uintptr) bool {
		n, from, err = syscall.Recvfrom(int(fd), p, 0)
		return true
	}); ctl != nil {
		return 0, nil, ctl
	}
	if n < 0 {
		n = 0
	}
	return n, from, err
}

func rawSendto(rc syscall.RawConn, p []byte, to sockaddr) error {
	var err error
	if ctl := rc.Write(func(fd uintptr) bool {
		err = syscall.Sendto(int(fd), p, 0, to)
		return true
	}); ctl != nil {
		return ctl
	}
	return err
}

func rawAccept(rc syscall.RawConn) (int, error) {
	fd := -1
	var err error
	if ctl := rc.Control(func(listenFD uintptr) {
		fd, _, err = syscall.Accept(int(listenFD))
	}); ctl != nil {
		return -1, ctl
	}
	return fd, err
}

func sockaddrKey(sa sockaddr) string {
	switch a := sa.(type) {
	case *syscall.SockaddrInet4:
		return fmt.Sprintf("4:%d.%d.%d.%d:%d", a.Addr[0], a.Addr[1], a.Addr[2], a.Addr[3], a.Port)
	case *syscall.SockaddrInet6:
		return fmt.Sprintf("6:%x:%d:%d", a.Addr, a.Port, a.ZoneId)
	default:
		return fmt.Sprintf("?:%v", sa)
	}
}
