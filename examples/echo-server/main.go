package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	fomoxa "github.com/fomoxa/cyclone-go"
	"github.com/fomoxa/cyclone-go/examples/echoschema"
)

func main() {
	address := flag.String("listen", "127.0.0.1:7788", "address to listen on")
	network := flag.String("network", "tcp", "tcp or udp")
	flag.Parse()

	schema := echoschema.New()
	var server *fomoxa.Server
	var err error
	switch *network {
	case "tcp":
		server, err = fomoxa.ListenTCP(*address, schema, fomoxa.DefaultConfig())
	case "udp":
		server, err = fomoxa.ListenUDP(*address, schema, fomoxa.DefaultConfig())
	default:
		log.Fatalf("unknown network %q", *network)
	}
	if err != nil {
		log.Fatal(err)
	}
	defer server.Close()

	fmt.Printf("fomoxa echo server on %s %s\n", *network, server.Addr())

	tick := time.NewTicker(16 * time.Millisecond)
	defer tick.Stop()
	for now := range tick.C {
		for _, event := range server.Tick(now) {
			switch event.Kind {
			case fomoxa.EventConnected:
				fmt.Printf("peer %d connected\n", event.Peer)
			case fomoxa.EventReady:
				fmt.Printf("peer %d ready\n", event.Peer)
			case fomoxa.EventMessage:
				fmt.Printf("peer %d sent %d bytes on message 0x%08X\n", event.Peer, len(event.Payload), event.MessageID)
				if err := server.Send(event.Peer, echoschema.ReplyMessageID, event.Payload); err != nil {
					fmt.Printf("peer %d echo failed: %v\n", event.Peer, err)
				}
			case fomoxa.EventHandshakeFailed:
				fmt.Printf("peer %d refused (%s): %v\n", event.Peer, event.Verdict, event.Err)
			case fomoxa.EventDisconnected:
				fmt.Printf("peer %d gone: %v\n", event.Peer, event.Err)
			}
		}
	}
}
