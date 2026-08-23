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
	address := flag.String("connect", "127.0.0.1:7788", "address to connect to")
	network := flag.String("network", "tcp", "tcp or udp")
	flag.Parse()

	schema := echoschema.New()
	var conn *fomoxa.Conn
	var err error
	switch *network {
	case "tcp":
		conn, err = fomoxa.DialTCP(*address, schema, fomoxa.DefaultConfig())
	case "udp":
		conn, err = fomoxa.DialUDP(*address, schema, fomoxa.DefaultConfig())
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
			case fomoxa.EventConnected:
				fmt.Println("pipe open, waiting for the verdict")
			case fomoxa.EventReady:
				fmt.Println("ready")
			case fomoxa.EventMessage:
				fmt.Printf("echo of %d bytes on message 0x%08X: %q\n", len(event.Payload), event.MessageID, event.Payload)
			case fomoxa.EventHandshakeFailed:
				log.Fatalf("handshake refused (%s): %v", event.Verdict, event.Err)
			case fomoxa.EventDisconnected:
				log.Fatalf("disconnected: %v", event.Err)
			}
		}

		if conn.State() == fomoxa.StateReady && now.After(nextSend) {
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
