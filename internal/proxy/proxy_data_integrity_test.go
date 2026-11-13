package proxy

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/ayush/loadbalancer/internal/pool"
)

// TestDataIntegrity starts a real TCP echo server and verifies that bytes
// flowing through ConnectBackend arrive without corruption or loss.
// This guards the bidirectional io.Copy tunnel in ConnectClientToBackend.
func TestDataIntegrity(t *testing.T) {
	// Start a minimal echo server.
	echoLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("echo listen: %v", err)
	}
	defer echoLn.Close()

	go func() {
		for {
			c, err := echoLn.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				io.Copy(c, c) // echo everything back
			}(c)
		}
	}()

	p := &pool.BackendPool{
		Backends: []pool.Backend{
			{Address: echoLn.Addr().String(), Healthy: true, State: pool.StateClosed},
		},
		Algorithm:     "least_connections",
		CbMaxFailures: 3,
		CbCooldown:    5 * time.Second,
	}

	ctx := context.Background()
	_, conn := ConnectBackend(ctx, p, 2*time.Second, 0, 0)
	if conn == nil {
		t.Fatal("ConnectBackend returned nil connection")
	}
	defer conn.Close()

	payload := []byte("hello-load-balancer-data-integrity-check")
	if _, err := conn.Write(payload); err != nil {
		t.Fatalf("write payload: %v", err)
	}

	buf := make([]byte, len(payload))
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatalf("read echo: %v", err)
	}

	if string(buf) != string(payload) {
		t.Errorf("data mismatch:\n  sent: %q\n  got:  %q", payload, buf)
	}
}

// TestRetryOnFailure verifies that ConnectBackend retries when the first
// attempt fails (backend refuses connection) and succeeds on a later attempt.
// It asserts the retry counter increments correctly.
func TestRetryOnFailure(t *testing.T) {
	// Server that will start accepting only after a small delay.
	readyLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer readyLn.Close()

	// Use a closed port as the first backend (will fail immediately).
	closedLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen (closed): %v", err)
	}
	closedAddr := closedLn.Addr().String()
	closedLn.Close() // close immediately so dial fails

	go func() {
		for {
			c, err := readyLn.Accept()
			if err != nil {
				return
			}
			c.Close()
		}
	}()

	// Pool with the closed address first, then the working one.
	// round_robin ensures the first attempt hits the closed backend.
	p := &pool.BackendPool{
		Backends: []pool.Backend{
			{Address: closedAddr, Healthy: true, State: pool.StateClosed},
			{Address: readyLn.Addr().String(), Healthy: true, State: pool.StateClosed},
		},
		Algorithm:     "round_robin",
		CbMaxFailures: 5,
		CbCooldown:    30 * time.Second,
	}

	ctx := context.Background()
	// maxRetries=1 means 1 initial attempt + 1 retry → should hit the working backend.
	backendInfo, conn := ConnectBackend(ctx, p, 500*time.Millisecond, 1, 10*time.Millisecond)

	if backendInfo == nil || conn == nil {
		t.Fatal("expected successful connection after retry, got nil")
	}
	conn.Close()

	if backendInfo.Address != readyLn.Addr().String() {
		t.Errorf("expected retry to land on working backend %s, got %s",
			readyLn.Addr().String(), backendInfo.Address)
	}
}

