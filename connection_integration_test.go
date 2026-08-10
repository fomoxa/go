package cyclone

import (
	"testing"
	"time"
)

// pumpUntil calls done in a loop - done is responsible for polling both
// sides itself - until it returns true or timeout elapses.
func pumpUntil(t *testing.T, timeout time.Duration, done func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if done() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return false
}

func TestClientAndServerRoundTripAMessage(t *testing.T) {
	server := NewServer()
	// Port 0: the OS picks a free port, read back via Addr() - avoids the
	// entire class of "the port I hardcoded happens to already be in use"
	// bug the cyclone-godot SDK hit during its own testing.
	if err := server.Start("127.0.0.1:0"); err != nil {
		t.Fatalf("server.Start: %v", err)
	}
	defer server.Stop()

	client := NewClient()
	if err := client.Connect(server.Addr().String(), 5*time.Second, 15*time.Second); err != nil {
		t.Fatalf("client.Connect: %v", err)
	}
	defer client.Disconnect()

	received := make(chan []byte, 1)
	OnClient(client, 99, func(payload []byte) []byte { return payload }, func(payload []byte) {
		received <- payload
	})

	connected := pumpUntil(t, 5*time.Second, func() bool {
		server.Poll()
		client.Poll()
		return server.ConnectionCount() > 0
	})
	if !connected {
		t.Fatal("server never saw the connection")
	}

	server.Broadcast(Message{ID: 99, Payload: []byte("hello")})

	var receivedViaEvent []byte
	done := pumpUntil(t, 5*time.Second, func() bool {
		server.Poll()
		for _, event := range client.Poll() {
			if event.Kind == ClientMessageReceived {
				receivedViaEvent = event.Message.Payload
			}
		}
		return receivedViaEvent != nil
	})
	if !done {
		t.Fatal("client never received the broadcast message")
	}
	if string(receivedViaEvent) != "hello" {
		t.Errorf("received via event = %q, want %q", receivedViaEvent, "hello")
	}

	select {
	case payload := <-received:
		if string(payload) != "hello" {
			t.Errorf("received via On handler = %q, want %q", payload, "hello")
		}
	default:
		t.Error("On handler never ran")
	}
}

func TestARealPingPongHeartbeatExchangeHappens(t *testing.T) {
	server := NewServer()
	if err := server.Start("127.0.0.1:0"); err != nil {
		t.Fatalf("server.Start: %v", err)
	}
	defer server.Stop()

	// A short interval so a real Ping/Pong round trip happens inside this
	// test's own timeout, not just something inspected in source.
	client := NewClient()
	if err := client.Connect(server.Addr().String(), 30*time.Millisecond, 5*time.Second); err != nil {
		t.Fatalf("client.Connect: %v", err)
	}
	defer client.Disconnect()

	sawPong := pumpUntil(t, 5*time.Second, func() bool {
		server.Poll()
		for _, event := range client.Poll() {
			if event.Kind == ClientPongReceived {
				return true
			}
		}
		return false
	})
	if !sawPong {
		t.Fatal("no Pong arrived within the timeout")
	}
}

func TestDisconnectIsObservedByBothSides(t *testing.T) {
	server := NewServer()
	if err := server.Start("127.0.0.1:0"); err != nil {
		t.Fatalf("server.Start: %v", err)
	}
	defer server.Stop()

	client := NewClient()
	if err := client.Connect(server.Addr().String(), 5*time.Second, 15*time.Second); err != nil {
		t.Fatalf("client.Connect: %v", err)
	}

	pumpUntil(t, 5*time.Second, func() bool {
		server.Poll()
		client.Poll()
		return server.ConnectionCount() > 0
	})

	client.Disconnect()

	serverSawIt := pumpUntil(t, 5*time.Second, func() bool {
		client.Poll()
		server.Poll()
		return server.ConnectionCount() == 0
	})
	if !serverSawIt {
		t.Fatal("server never noticed the client disconnecting")
	}
}

func TestBroadcastReachesEveryConnectedClient(t *testing.T) {
	server := NewServer()
	if err := server.Start("127.0.0.1:0"); err != nil {
		t.Fatalf("server.Start: %v", err)
	}
	defer server.Stop()

	clientA := NewClient()
	clientB := NewClient()
	addr := server.Addr().String()
	if err := clientA.Connect(addr, 5*time.Second, 15*time.Second); err != nil {
		t.Fatalf("clientA.Connect: %v", err)
	}
	defer clientA.Disconnect()
	if err := clientB.Connect(addr, 5*time.Second, 15*time.Second); err != nil {
		t.Fatalf("clientB.Connect: %v", err)
	}
	defer clientB.Disconnect()

	bothConnected := pumpUntil(t, 5*time.Second, func() bool {
		server.Poll()
		clientA.Poll()
		clientB.Poll()
		return server.ConnectionCount() == 2
	})
	if !bothConnected {
		t.Fatalf("server saw %d connections, want 2", server.ConnectionCount())
	}

	server.Broadcast(Message{ID: 7, Payload: []byte("to-all")})

	aGot, bGot := false, false
	pumpUntil(t, 5*time.Second, func() bool {
		server.Poll()
		for _, event := range clientA.Poll() {
			if event.Kind == ClientMessageReceived && string(event.Message.Payload) == "to-all" {
				aGot = true
			}
		}
		for _, event := range clientB.Poll() {
			if event.Kind == ClientMessageReceived && string(event.Message.Payload) == "to-all" {
				bGot = true
			}
		}
		return aGot && bGot
	})

	if !aGot || !bGot {
		t.Errorf("broadcast did not reach both clients: a=%v b=%v", aGot, bGot)
	}
}
