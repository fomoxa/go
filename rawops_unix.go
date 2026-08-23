//go:build unix

package fomoxa

import (
	"errors"
	"syscall"
)

func wouldBlock(err error) bool {
	return errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EINTR)
}

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

func rawRecvfrom(rc syscall.RawConn, p []byte) (int, syscall.Sockaddr, error) {
	var n int
	var from syscall.Sockaddr
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

func rawSendto(rc syscall.RawConn, p []byte, to syscall.Sockaddr) error {
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
