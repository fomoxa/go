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
	address := flag.String("connect", "127.0.0.1:7788", "address to connect to")
	network := flag.String("network", "tcp", "tcp or udp")
	flag.Parse()

	schema := echoschema.New()
	var conn *cyclone.Conn
	var err error
	switch *network {
	case "tcp":
		conn, err = cyclone.DialTCP(*address, schema, cyclone.DefaultConfig())
	case "udp":
		conn, err = cyclone.DialUDP(*address, schema, cyclone.DefaultConfig())
	default:
		log.Fatalf("unknown network %q", *network)
	}
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	tick := time.NewTicker(16 * time.Millisecond)
	defer tick.Stop()

	nextSend := time.Now()
	counter := 0
	for now := range tick.C {
		for _, event := range conn.Tick(now) {
			switch event.Kind {
			case cyclone.EventConnected:
				fmt.Println("pipe open, waiting for the verdict")
			case cyclone.EventReady:
				fmt.Println("ready")
			case cyclone.EventMessage:
				fmt.Printf("echo of %d bytes on message 0x%08X: %q\n", len(event.Payload), event.MessageID, event.Payload)
			case cyclone.EventHandshakeFailed:
				log.Fatalf("handshake refused (%s): %v", event.Verdict, event.Err)
			case cyclone.EventDisconnected:
				log.Fatalf("disconnected: %v", event.Err)
			}
		}

		if conn.State() == cyclone.StateReady && now.After(nextSend) {
			counter++
			payload := []byte(fmt.Sprintf("hello %d", counter))
			switch err := conn.Send(echoschema.EchoMessageID, payload); {
			case err == nil:
				nextSend = now.Add(time.Second)
			default:
				fmt.Printf("send skipped: %v\n", err)
			}
		}
	}
}
