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
	port        = "9321"
	playerEdge  = 1
	playerInput = 2
)

func decodePlayerInput(payload []byte) shared.PlayerInput {
	reader := shared.NewReader(payload)
	var value shared.PlayerInput
	if err := (shared.PlayerInputClientCodec{}).Decode(reader, &value); err != nil {
		panic(err) // a malformed frame from a peer speaking the same protocol is not expected here
	}
	return value
}

func main() {
	server := cyclone.NewServer()
	if err := server.Start("127.0.0.1:" + port); err != nil {
		panic(fmt.Sprintf("bind 127.0.0.1:%s - is something else already using this port? %v", port, err))
	}

	fmt.Printf("cyclone-go server example listening on port %s\n", port)

	cyclone.OnServer(server, playerInput, decodePlayerInput, func(id cyclone.ConnectionID, payload shared.PlayerInput) {
		fmt.Printf("received PlayerInput { X = %d, Z = %q } from connection %d\n", payload.X, payload.Z, id)
	})

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
			case cyclone.ServerMessageReceived:
				fmt.Printf("message received from connection %d\n", event.ID)
			}
		}
		time.Sleep(16 * time.Millisecond)
	}
}
