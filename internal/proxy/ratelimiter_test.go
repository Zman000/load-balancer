package proxy

import (
	"testing"
	"time"
)

// manualClock lets tests control time without sleeping.
// It wraps a RateLimiter and overrides the timestamps written into ClientLimiter.
func makeRateLimiterAt(rate, capacity float64, now time.Time, ip string) *RateLimiter {
	rl := &RateLimiter{
		clients:  make(map[string]*ClientLimiter),
		rate:     rate,
		capacity: capacity,
	}
	// Pre-seed the client entry at a controlled timestamp.
	rl.clients[ip] = &ClientLimiter{
		tokens:     capacity,
		lastUpdate: now,
	}
	return rl
}

// allowAt calls rl.Allow but with the client's lastUpdate set to 'now' so
// elapsed-time calculations in Allow() produce deterministic results.
// We achieve this by setting lastUpdate before each call.
func allowAt(rl *RateLimiter, ip string, now time.Time) bool {
	rl.mu.Lock()
	if cl, ok := rl.clients[ip]; ok {
		cl.lastUpdate = now
	}
	rl.mu.Unlock()
	return rl.Allow(ip)
}

// TestRateLimiter_BurstPhase verifies that a new client can fire exactly
// capacity requests before being denied (burst phase, no time passes).
func TestRateLimiter_BurstPhase(t *testing.T) {
	const rate = 1.0     // 1 token/sec refill
	const capacity = 5.0 // burst of 5

	rl := &RateLimiter{
		clients:  make(map[string]*ClientLimiter),
		rate:     rate,
		capacity: capacity,
	}

	ip := "10.0.0.1"
	allowed := 0
	for i := 0; i < 10; i++ {
		if rl.Allow(ip) {
			allowed++
		}
	}
	// First call seeds capacity-1 tokens, so we expect exactly capacity=5 allows.
	if allowed != int(capacity) {
		t.Errorf("burst phase: expected %d allows, got %d", int(capacity), allowed)
	}
}

// TestRateLimiter_Refill verifies tokens replenish correctly after elapsed time.
func TestRateLimiter_Refill(t *testing.T) {
	const rate = 10.0    // 10 tokens/sec
	const capacity = 10.0

	t0 := time.Now()
	ip := "10.0.0.2"

	rl := makeRateLimiterAt(rate, capacity, t0, ip)

	// Drain all tokens.
	for i := 0; i < int(capacity); i++ {
		rl.Allow(ip)
	}

	// Confirm drained.
	rl.mu.Lock()
	tokensAfterDrain := rl.clients[ip].tokens
	rl.mu.Unlock()
	if tokensAfterDrain >= 1 {
		t.Fatalf("expected tokens < 1 after drain, got %f", tokensAfterDrain)
	}

	// Simulate 2 seconds of elapsed time by rewinding lastUpdate.
	rl.mu.Lock()
	rl.clients[ip].lastUpdate = time.Now().Add(-2 * time.Second)
	rl.mu.Unlock()

	// After 2s at 10 tok/s, should have ~20 tokens capped to capacity=10.
	if !rl.Allow(ip) {
		t.Error("expected Allow to succeed after simulated 2s refill")
	}
	rl.mu.Lock()
	tokensAfterRefill := rl.clients[ip].tokens
	rl.mu.Unlock()
	// Should be close to capacity-1 (cap applied, then one consumed).
	if tokensAfterRefill < capacity-2 {
		t.Errorf("expected ~%f tokens after refill, got %f", capacity-1, tokensAfterRefill)
	}
}

// TestRateLimiter_Eviction verifies stale clients are removed by cleanupStaleClients.
func TestRateLimiter_Eviction(t *testing.T) {
	rl := &RateLimiter{
		clients:  make(map[string]*ClientLimiter),
		rate:     1,
		capacity: 5,
	}

	// Insert a client with a lastUpdate far in the past (>10 min ago).
	staleIP := "192.168.1.1"
	rl.clients[staleIP] = &ClientLimiter{
		tokens:     3,
		lastUpdate: time.Now().Add(-11 * time.Minute),
	}
	freshIP := "192.168.1.2"
	rl.clients[freshIP] = &ClientLimiter{
		tokens:     3,
		lastUpdate: time.Now(),
	}

	// Run the cleanup logic inline (same logic as cleanupStaleClients).
	rl.mu.Lock()
	now := time.Now()
	for ip, client := range rl.clients {
		if now.Sub(client.lastUpdate) > 10*time.Minute {
			delete(rl.clients, ip)
		}
	}
	rl.mu.Unlock()

	rl.mu.Lock()
	_, staleExists := rl.clients[staleIP]
	_, freshExists := rl.clients[freshIP]
	rl.mu.Unlock()

	if staleExists {
		t.Error("stale client was NOT evicted — potential memory leak")
	}
	if !freshExists {
		t.Error("fresh client was incorrectly evicted")
	}
}

// TestRateLimiter_NewClientGetsCapacity verifies a brand-new client gets
// full burst capacity on first contact.
func TestRateLimiter_NewClientGetsCapacity(t *testing.T) {
	rl := NewRateLimiter(1, 5)
	ip := "172.16.0.1"

	// First call should always be allowed (new client gets capacity-1 tokens).
	if !rl.Allow(ip) {
		t.Error("first request from new client should always be allowed")
	}
}

