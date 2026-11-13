package pool

import (
	"sync"
	"testing"
	"time"
)

// TestHalfOpenLatch verifies that when multiple Open backends become eligible
// (cooldown has elapsed), only exactly ONE is promoted to HalfOpen per
// AcquireBackend call. If this regresses, all backends get simultaneously
// promoted and a struggling backend gets flooded with probes.
func TestHalfOpenLatch(t *testing.T) {
	pastCooldown := time.Now().Add(-10 * time.Second)
	p := &BackendPool{
		Backends: []Backend{
			{Address: "b1:9000", Healthy: true, State: StateOpen, TripTime: pastCooldown},
			{Address: "b2:9000", Healthy: true, State: StateOpen, TripTime: pastCooldown},
			{Address: "b3:9000", Healthy: true, State: StateOpen, TripTime: pastCooldown},
		},
		Algorithm:     "least_connections",
		CbMaxFailures: 3,
		CbCooldown:    5 * time.Second,
	}

	b := p.AcquireBackend()
	if b == nil {
		t.Fatal("AcquireBackend returned nil — expected one HalfOpen backend to be selected")
	}
	p.ReleaseBackend(b)

	halfOpenCount := 0
	for i := range p.Backends {
		if p.Backends[i].State == StateHalfOpen {
			halfOpenCount++
		}
	}
	if halfOpenCount != 1 {
		t.Errorf("latch broken: expected exactly 1 HalfOpen backend, got %d", halfOpenCount)
	}
}

func TestBackendPool_Concurrency(t *testing.T) {
	// Initialize a test pool with two healthy dummy backends
	p := &BackendPool{
		Backends: []Backend{
			{Address: "server1", Healthy: true, State: StateClosed},
			{Address: "server2", Healthy: true, State: StateClosed},
		},
		Algorithm: "least_connections",
	}

	var wg sync.WaitGroup
	routines := 100

	// Fire 100 concurrent requests at the pool
	for i := 0; i < routines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			backend := p.AcquireBackend()
			if backend != nil {
				// Simulate some network delay
				time.Sleep(2 * time.Millisecond)
				p.ReleaseBackend(backend)
			}
		}()
	}

	// Wait for all 100 requests to finish
	wg.Wait()

	// Verify the math is perfectly zeroed out
	for _, b := range p.Backends {
		if b.ActiveConnections != 0 {
			t.Errorf("Race condition detected! %s has %d leftover connections", b.Address, b.ActiveConnections)
		}
	}
}

// BenchmarkAcquireRelease measures AcquireBackend + ReleaseBackend throughput
// under parallel contention — the hot path for every accepted TCP connection.
// Run with: go test -bench=BenchmarkAcquireRelease -benchtime=5s ./internal/pool
func BenchmarkAcquireRelease(b *testing.B) {
	p := &BackendPool{
		Backends: []Backend{
			{Address: "b1:9000", Healthy: true, State: StateClosed},
			{Address: "b2:9000", Healthy: true, State: StateClosed},
			{Address: "b3:9000", Healthy: true, State: StateClosed},
			{Address: "b4:9000", Healthy: true, State: StateClosed},
		},
		Algorithm:     "least_connections",
		CbMaxFailures: 10,
		CbCooldown:    5 * time.Second,
	}

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			backend := p.AcquireBackend()
			if backend != nil {
				p.ReleaseBackend(backend)
			}
		}
	})
}
