package proxy

import (
	"LB/internal/pool"
	"net"
	"testing"
	"time"
)

func TestCircuitBreaker_Integration(t *testing.T) {
	server1, _ := net.Listen("tcp", "127.0.0.1:0")
	server2, _ := net.Listen("tcp", "127.0.0.1:0")

	defer server1.Close()

	go func() {
		for {
			conn, err := server1.Accept()
			if err != nil {
				return
			}
			conn.Close()
		}
	}()

	lbPool := &pool.BackendPool{
		Backends: []pool.Backend{
			{Address: server1.Addr().String(), Healthy: true, State: pool.StateClosed},
			{Address: server2.Addr().String(), Healthy: true, State: pool.StateClosed},
		},
		Algorithm:     "round_robin",
		CbMaxFailures: 2,
		CbCooldown:    5 * time.Second,
	}

	server2.Close()
	for i := 0; i < 5; i++ {
		_, conn := ConnectBackend(lbPool, 100*time.Millisecond, 0, 0)
		if conn != nil {
			conn.Close()
		}
		time.Sleep(10 * time.Millisecond)
	}

	for _, b := range lbPool.Backends {
		if b.Address == server2.Addr().String() {
			if b.State != pool.StateOpen {
				t.Errorf("Expected crashed server to be OPEN, but got %s", b.State)
			}
		} else {
			if b.State != pool.StateClosed {
				t.Errorf("Expected healthy server to remain CLOSED, but got %s", b.State)
			}
		}
	}
}
