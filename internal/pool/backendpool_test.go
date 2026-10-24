package pool

import (
	"sync"
	"testing"
	"time"
)

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
