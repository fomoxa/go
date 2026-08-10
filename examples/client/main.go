// Standalone client example - a real, separate OS process. See
// examples/server's own header for how to run both together.
//
//	go run ./examples/client
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

// decodePlayer is the one-line adapter cyclone.On needs: the project's own
// generated Reader is bridged from []byte -> shared.Player here, the same
// seam Cyclone.Unity's CycloneDecoder<T>, cyclone-godot's decode Callable,
// and cyclone-rust's decode closure all are.
func decodePlayer(payload []byte) shared.Player {
	reader := shared.NewReader(payload)
	var value shared.Player
	if err := (shared.PlayerEdgeCodec{}).Decode(reader, &value); err != nil {
		panic(err) // a malformed frame from a peer speaking the same protocol is not expected here
	}
	return value
}

func main() {
	client := cyclone.NewClient()

	cyclone.OnClient(client, playerEdge, decodePlayer, func(player shared.Player) {
		fmt.Printf("received Player { hp = %d, name = %q }\n", player.HP, player.Name)
	})

	fmt.Printf("cyclone-go client example connecting to 127.0.0.1:%s\n", port)
	if err := client.Connect("127.0.0.1:"+port, 5*time.Second, 15*time.Second); err != nil {
		panic(fmt.Sprintf("connect to 127.0.0.1:%s - is the server example running? %v", port, err))
	}
	fmt.Println("connected to server, waiting for Player message...")

	reportedConnected := false
	go func() {
		time.Sleep(4 * time.Second)
		fmt.Println("go func")

		outgoing := shared.PlayerInput{X: 42, Z: "hello"}
		writer := shared.NewWriter()
		shared.PlayerInputClientCodec{}.Encode(writer, &outgoing)

		err := client.Send(cyclone.Message{ID: playerInput, Payload: writer.Bytes()})
		if err != nil {
			panic(fmt.Sprintf("send message: %v", err))
		}
	}()
	for {
		for _, event := range client.Poll() {
			switch event.Kind {
			case cyclone.ClientConnected:
				if !reportedConnected {
					reportedConnected = true
					fmt.Println("connected to server")

					outgoing := shared.PlayerInput{X: 42, Z: "hello"}
					writer := shared.NewWriter()
					shared.PlayerInputClientCodec{}.Encode(writer, &outgoing)

					_ = client.Send(cyclone.Message{ID: playerInput, Payload: writer.Bytes()})
				}
			case cyclone.ClientDisconnected:
				fmt.Println("disconnected")
				return
			}
		}
		time.Sleep(16 * time.Millisecond)
	}
}
