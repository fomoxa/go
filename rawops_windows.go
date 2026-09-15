//go:build windows

package fomoxa

import (
	"errors"
	"fmt"
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
)

type sockaddr = windows.Sockaddr

const fionbio = windows.IOC_IN | 4<<16 | 'f'<<8 | 126

var procAccept = windows.NewLazySystemDLL("ws2_32.dll").NewProc("accept")

func wouldBlock(err error) bool { return errors.Is(err, windows.WSAEWOULDBLOCK) }

func messageTooLarge(err error) bool { return errors.Is(err, windows.WSAEMSGSIZE) }

func peerRefused(err error) bool { return errors.Is(err, windows.WSAECONNRESET) }

func prepareSocket(rc syscall.RawConn) error {
	var err error
	if ctl := rc.Control(func(fd uintptr) {
		err = setNonblocking(windows.Handle(fd))
	}); ctl != nil {
		return ctl
	}
	return err
}

func setNonblocking(handle windows.Handle) error {
	enabled := uint32(1)
	var returned uint32
	return windows.WSAIoctl(handle, fionbio, (*byte)(unsafe.Pointer(&enabled)), uint32(unsafe.Sizeof(enabled)), nil, 0, &returned, nil, 0)
}

func socketRecv(handle windows.Handle, p []byte) (int, error) {
	buf := windows.WSABuf{Len: uint32(len(p)), Buf: unsafe.SliceData(p)}
	var n, flags uint32
	err := windows.WSARecv(handle, &buf, 1, &n, &flags, nil, nil)
	return int(n), err
}

func socketSend(handle windows.Handle, p []byte) (int, error) {
	buf := windows.WSABuf{Len: uint32(len(p)), Buf: unsafe.SliceData(p)}
	var n uint32
	err := windows.WSASend(handle, &buf, 1, &n, 0, nil, nil)
	return int(n), err
}

func rawRead(rc syscall.RawConn, p []byte) (int, error) {
	var n int
	var err error
	if ctl := rc.Read(func(fd uintptr) bool {
		n, err = socketRecv(windows.Handle(fd), p)
		return true
	}); ctl != nil {
		return 0, ctl
	}
	return n, err
}

func rawWrite(rc syscall.RawConn, p []byte) (int, error) {
	var n int
	var err error
	if ctl := rc.Write(func(fd uintptr) bool {
		n, err = socketSend(windows.Handle(fd), p)
		return true
	}); ctl != nil {
		return 0, ctl
	}
	return n, err
}

func rawRecvfrom(rc syscall.RawConn, p []byte) (int, sockaddr, error) {
	var n int
	var from sockaddr
	var err error
	if ctl := rc.Read(func(fd uintptr) bool {
		n, from, err = windows.Recvfrom(windows.Handle(fd), p, 0)
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
		err = windows.Sendto(windows.Handle(fd), p, 0, to)
		return true
	}); ctl != nil {
		return ctl
	}
	return err
}

func rawAccept(rc syscall.RawConn) (windows.Handle, error) {
	handle := windows.InvalidHandle
	var err error
	if ctl := rc.Control(func(listenFD uintptr) {
		accepted, _, callErr := procAccept.Call(listenFD, 0, 0)
		if windows.Handle(accepted) == windows.InvalidHandle {
			err = callErr
			return
		}
		handle = windows.Handle(accepted)
	}); ctl != nil {
		return windows.InvalidHandle, ctl
	}
	return handle, err
}

func sockaddrKey(sa sockaddr) string {
	switch a := sa.(type) {
	case *windows.SockaddrInet4:
		return fmt.Sprintf("4:%d.%d.%d.%d:%d", a.Addr[0], a.Addr[1], a.Addr[2], a.Addr[3], a.Port)
	case *windows.SockaddrInet6:
		return fmt.Sprintf("6:%x:%d:%d", a.Addr, a.Port, a.ZoneId)
	default:
		return fmt.Sprintf("?:%v", sa)
	}
}
