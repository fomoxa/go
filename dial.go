package fomoxa

import "net"

func DialTCP(address string, schema *Schema, cfg Config) (*Conn, error) {
	addr, err := net.ResolveTCPAddr("tcp", address)
	if err != nil {
		return nil, err
	}
	conn, err := net.DialTCP("tcp", nil, addr)
	if err != nil {
		return nil, err
	}
	transport, err := newTCPTransport(conn)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	c, err := NewConn(transport, schema, cfg)
	if err != nil {
		transport.Close()
		return nil, err
	}
	return c, nil
}

func DialUDP(address string, schema *Schema, cfg Config) (*Conn, error) {
	addr, err := net.ResolveUDPAddr("udp", address)
	if err != nil {
		return nil, err
	}
	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		return nil, err
	}
	transport, err := newUDPTransport(conn)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	c, err := NewConn(transport, schema, cfg)
	if err != nil {
		transport.Close()
		return nil, err
	}
	return c, nil
}

func ListenTCP(address string, schema *Schema, cfg Config) (*Server, error) {
	cfg = cfg.normalized()
	addr, err := net.ResolveTCPAddr("tcp", address)
	if err != nil {
		return nil, err
	}
	socket, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return nil, err
	}
	l, err := newTCPListener(socket, cfg)
	if err != nil {
		_ = socket.Close()
		return nil, err
	}
	return newServer(l, schema, cfg), nil
}

func ListenUDP(address string, schema *Schema, cfg Config) (*Server, error) {
	cfg = cfg.normalized()
	addr, err := net.ResolveUDPAddr("udp", address)
	if err != nil {
		return nil, err
	}
	socket, err := net.ListenUDP("udp", addr)
	if err != nil {
		return nil, err
	}
	l, err := newUDPListener(socket, cfg)
	if err != nil {
		_ = socket.Close()
		return nil, err
	}
	return newServer(l, schema, cfg), nil
}
