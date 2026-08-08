# cyclone-go

Minimal, platform-agnostic Go runtime for Cyclone. The Go counterpart of
[cyclone-unity](https://github.com/cyclone-protocol/cyclone-unity),
[cyclone-godot](https://github.com/cyclone-protocol/cyclone-godot) and
[cyclone-rust](https://github.com/cyclone-protocol/cyclone-rust) - same wire
format (Cyclone's frame: `Magic + MessageId + PayloadLength + Payload`), same
heartbeat, same idea, with nothing tying it to a specific engine or platform.

V1 responsibilities:
- TCP transport (`net`, standard library only)
- Message framing
- MessageId + PayloadLength
- Payload bounded decoding
- Generated codec registration
- Typed message handlers

Generated codecs are expected to be produced by `cyclonec`.

## A goroutine, not an event loop

This package depends on nothing but the standard library - the same "no
dependencies, nothing here could need one" rule `cyclonec` itself follows.
A blocking `net.Conn.Read` cannot simply be selected on between `Poll` calls
the way cyclone-godot's poll-based engine sockets can, so `Connection` spawns
one background goroutine per connection that blocks on reads and forwards
decoded events through a channel - the same shape cyclone-rust's background
thread has, using Go's own concurrency primitives instead of
`std::thread`/`mpsc`. `Poll` only ever drains that channel and ticks the
heartbeat, so it never blocks and is safe to call once a tick/frame from a
single goroutine, the same shape Cyclone.Unity's `Pump()` and cyclone-godot's
`poll()` both have.

## Naming: no `Cyclone` prefix

Cyclone.Unity, cyclone-godot and cyclone-rust all name their types
`CycloneClient`, `CycloneServer`, `CycloneConnection`, ... Go convention
(flagged by `go vet`/`staticcheck` as a "stutter") is that a package name
already qualifies its exports, so this package spells them `Client`,
`Server`, `Connection` instead - `cyclone.Client`, not
`cyclone.CycloneClient`. Same shape, same behavior, Go-idiomatic names.

## Generics, unlike cyclone-godot - as a function, not a method

GDScript has no generics, so cyclone-godot's `on()` takes two `Callable`s and
loses compile-time type checking. Go has generics, but - unlike Rust or C# -
does not allow a method to introduce its own type parameter beyond its
receiver's, so the typed registration helper here is the package-level
function `On[T]`, not a `Client` method:

```go
func decodePlayer(payload []byte) Player {
    reader := NewReader(payload)
    var value Player
    if err := (PlayerEdgeCodec{}).Decode(reader, &value); err != nil {
        panic(err)
    }
    return value
}

cyclone.On(client, playerEdgeID, decodePlayer, func(p Player) {
    fmt.Println(p.HP)
})
```

`decode` is `func([]byte) T`; `handler` is `func(T)`. Multiple handlers on
one message id all run, in registration order, and nothing here recovers a
panic either one raises.

## Usage

```go
server := cyclone.NewServer()
server.Start("0.0.0.0:9000")

client := cyclone.NewClient()
cyclone.On(client, playerEdgeID, decodePlayer, func(p Player) { fmt.Println(p.HP) })
client.Connect("127.0.0.1:9000", 5*time.Second, 15*time.Second)

for {
    for _, event := range server.Poll() { /* cyclone.ServerClientConnected, ... */ }
    for _, event := range client.Poll() { /* cyclone.ClientMessageReceived, ... */ }
    time.Sleep(16 * time.Millisecond)
}
```

## Picking a port: `Addr()`

`Server.Start` accepts port `0` to let the OS pick a free port, readable back
via `Addr()`. This is the robust way to get a port for a test or an
ephemeral server - no fixed port number can collide with something else
already using it (see the test suite, and cyclone-godot's own README for the
real bug a hardcoded port caused there).

## Layout

```
cyclone-go/
├── protocol.go               Message, system message ids, frame encode/decode
├── heartbeat.go               internal heartbeat state
├── connection.go              Connection - background reader goroutine + Poll
├── client.go                  Client - typed On[T]
├── server.go                  Server
├── *_test.go                  unit tests (frame, heartbeat)
├── connection_integration_test.go   real TCP integration tests
└── examples/
    ├── shared/                a cyclonec-generated codec (from player.go's cyclone: comments)
    ├── server/                standalone, runnable server (its own process)
    └── client/                standalone, runnable client (its own process)
```

## Running the tests

```
go test ./... -race
```

10 unit tests (frame encode/decode, heartbeat timing) + 4 integration tests
against real sockets (connect, a real Ping/Pong exchange observed within a
timeout, disconnect, broadcast to multiple clients) - all pass under the
race detector.

## Running the examples

Two separate OS processes, talking over a real TCP socket on localhost -
confirmed working this way:

```
go run ./examples/server   # terminal A
go run ./examples/client   # terminal B, once A prints "listening"
```

Terminal A:
```
cyclone-go server example listening on port 9321
client connected - broadcasting a Player
client disconnected
```

Terminal B:
```
cyclone-go client example connecting to 127.0.0.1:9321
connected to server
received Player { hp = 100, name = "sensor-1" }
```

## License

Apache-2.0
