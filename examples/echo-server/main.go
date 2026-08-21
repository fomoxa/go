package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	cyclone "github.com/cyclone-protocol/cyclone-go"
	"github.com/cyclone-protocol/cyclone-go/examples/echoschema"
)

func main() {
	address := flag.String("listen", "127.0.0.1:7788", "address to listen on")
	network := flag.String("network", "tcp", "tcp or udp")
	flag.Parse()

	schema := echoschema.New()
	var server *cyclone.Server
	var err error
	switch *network {
	case "tcp":
		server, err = cyclone.ListenTCP(*address, schema, cyclone.DefaultConfig())
	case "udp":
		server, err = cyclone.ListenUDP(*address, schema, cyclone.DefaultConfig())
	default:
		log.Fatalf("unknown network %q", *network)
	}
	if err != nil {
		log.Fatal(err)
	}
	defer server.Close()

	fmt.Printf("cyclone echo server on %s %s\n", *network, server.Addr())

	tick := time.NewTicker(16 * time.Millisecond)
	defer tick.Stop()
	for now := range tick.C {
		for _, event := range server.Tick(now) {
			switch event.Kind {
			case cyclone.EventConnected:
				fmt.Printf("peer %d connected\n", event.Peer)
			case cyclone.EventReady:
				fmt.Printf("peer %d ready\n", event.Peer)
			case cyclone.EventMessage:
				fmt.Printf("peer %d sent %d bytes on message 0x%08X\n", event.Peer, len(event.Payload), event.MessageID)
				if err := server.Send(event.Peer, echoschema.ReplyMessageID, event.Payload); err != nil {
					fmt.Printf("peer %d echo failed: %v\n", event.Peer, err)
				}
			case cyclone.EventHandshakeFailed:
				fmt.Printf("peer %d refused (%s): %v\n", event.Peer, event.Verdict, event.Err)
			case cyclone.EventDisconnected:
				fmt.Printf("peer %d gone: %v\n", event.Peer, event.Err)
			}
		}
	}
}
