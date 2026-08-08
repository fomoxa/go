// Standalone server example - a real, separate OS process. Run this and
// examples/client as two different processes to see them talk over a real
// TCP socket, not a simulated one in a single process.
//
//	go run ./examples/server
//	go run ./examples/client   # in another terminal, once this prints "listening"
package main

import (
	"fmt"
	"time"

	cyclone "github.com/cyclone-protocol/cyclone-go"
	"github.com/cyclone-protocol/cyclone-go/examples/shared"
)

const (
	port       = "9321"
	playerEdge = 1
)

func main() {
	server := cyclone.NewServer()
	if err := server.Start("127.0.0.1:" + port); err != nil {
		panic(fmt.Sprintf("bind 127.0.0.1:%s - is something else already using this port? %v", port, err))
	}

	fmt.Printf("cyclone-go server example listening on port %s\n", port)

	for {
		for _, event := range server.Poll() {
			switch event.Kind {
			case cyclone.ServerClientConnected:
				fmt.Println("client connected - broadcasting a Player")

				outgoing := shared.Player{HP: 100, Name: "sensor-1"}
				writer := shared.NewWriter()
				shared.PlayerEdgeCodec{}.Encode(writer, &outgoing)

				_ = server.SendTo(event.ID, cyclone.Message{ID: playerEdge, Payload: writer.Bytes()})
			case cyclone.ServerClientDisconnected:
				fmt.Println("client disconnected")
			}
		}
		time.Sleep(16 * time.Millisecond)
	}
}
