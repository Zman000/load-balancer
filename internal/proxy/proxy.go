package proxy

import (
	"LB/internal/pool"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type ClientLimiter struct {
	tokens     float64
	lastUpdate time.Time
}

type RateLimiter struct {
	mu       sync.Mutex
	clients  map[string]*ClientLimiter
	rate     float64
	capacity float64
}

type copyResult struct {
	err   error
	bytes int64
}

type idleConn struct {
	net.Conn
	timeout time.Duration
}

func (c *idleConn) Read(b []byte) (int, error) {
	c.Conn.SetReadDeadline(time.Now().Add(c.timeout))
	return c.Conn.Read(b)
}

func (c *idleConn) Write(b []byte) (int, error) {
	c.Conn.SetWriteDeadline(time.Now().Add(c.timeout))
	return c.Conn.Write(b)
}

var (
	requestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "lb_req_total",
		Help: "Total number of requests routed per backend",
	}, []string{"backend", "status"})

	requestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "lb_req_duration_seconds",
		Help: "Histogram of request latencies",
	}, []string{"backend"})
)

func NewRateLimiter(rate, capacity float64) *RateLimiter {
	if capacity < 1 {
		capacity = 1 // Prevent negative token initialization
	}
	rl := &RateLimiter{
		clients:  make(map[string]*ClientLimiter),
		rate:     rate,
		capacity: capacity,
	}

	go rl.cleanupStaleClients()

	return rl
}

func (rl *RateLimiter) cleanupStaleClients() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		rl.mu.Lock()
		now := time.Now()
		for ip, client := range rl.clients {
			if now.Sub(client.lastUpdate) > 10*time.Minute {
				delete(rl.clients, ip)
			}
		}
		rl.mu.Unlock()
	}
}

func (rl *RateLimiter) Allow(ip string) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now()
	client, exists := rl.clients[ip]
	if !exists {
		// New client gets full capacity minus the current request
		rl.clients[ip] = &ClientLimiter{
			tokens:     rl.capacity - 1,
			lastUpdate: now,
		}
		return true
	}

	//replenish tokens
	elapsed := now.Sub(client.lastUpdate).Seconds()
	client.tokens += elapsed * rl.rate
	if client.tokens > rl.capacity {
		client.tokens = rl.capacity
	}
	client.lastUpdate = now

	//check if allowed
	if client.tokens >= 1 {
		client.tokens--
		return true
	}
	return false
}

func ConnectBackend(pool *pool.BackendPool, connectionTimeout time.Duration, maxRetries int, initialBackoff time.Duration) (*pool.Backend, net.Conn) {
	backoff := initialBackoff

	for attempt := 0; attempt <= maxRetries; attempt++ {
		backendInfo := pool.AcquireBackend()

		if backendInfo == nil {
			return nil, nil
		}

		conn, err := net.DialTimeout("tcp", backendInfo.Address, connectionTimeout)
		if err == nil {
			pool.RecordSuccess(backendInfo)
			return backendInfo, conn
		}

		slog.Error("backend unreachable", "backend", backendInfo.Address, "error", err)

		pool.ReleaseBackend(backendInfo)
		pool.RecordFailure(backendInfo)

		if attempt < maxRetries {
			slog.Warn("connection failed. retrying in ", "backoff", backoff)
			time.Sleep(backoff)
			backoff *= 2
		}
		// pool.SetHealth(backendInfo, false)
	}
	return nil, nil
}

func ConnectClientToBackend(conn net.Conn, pool *pool.BackendPool, timeout time.Duration, maxRetries int, initialBackoff time.Duration, idleTimeout time.Duration) {
	// Record the timestamp at accept/start
	startTime := time.Now()

	defer conn.Close()

	// fmt.Println("Forwarding to:", backendInfo.Address)

	backendInfo, backend := ConnectBackend(pool, timeout, maxRetries, initialBackoff)

	if backendInfo == nil {
		// fmt.Printf("%s,NONE,0,0,error_no_backend\n", time.Now().Format(time.RFC3339))
		slog.Error("no backend available", "status", "error_no_backend")
		return
	}

	defer pool.ReleaseBackend(backendInfo)
	defer backend.Close()

	activeClient := &idleConn{Conn: conn, timeout: idleTimeout}
	activeBackend := &idleConn{Conn: backend, timeout: idleTimeout}

	//use struct channel to get both error and byte count
	res := make(chan copyResult, 2)

	go func() {
		n, err := io.Copy(activeClient, activeBackend)
		if tcp, ok := activeClient.Conn.(*net.TCPConn); ok {
			if closeErr := tcp.CloseWrite(); err == nil {
				err = closeErr
			}
		}
		res <- copyResult{err: err, bytes: n}
	}()

	go func() {
		n, err := io.Copy(activeBackend, activeClient)
		if tcp, ok := activeBackend.Conn.(*net.TCPConn); ok {
			if closeErr := tcp.CloseWrite(); err == nil {
				err = closeErr
			}
		}
		res <- copyResult{err: err, bytes: n}
	}()

	var totalBytes int64
	status := "success"

	for i := 0; i < 2; i++ {
		result := <-res
		totalBytes += result.bytes
		if result.err != nil && result.err != io.EOF {
			status = "error"
		}
	}

	duration := time.Since(startTime).Milliseconds()
	durationSec := float64(duration) / 1000.0
	// timestamp := time.Now().Format(time.RFC3339)

	// Output CSV line: timestamp,backend,duration_ms,bytes,status
	// fmt.Printf("%s,%s,%d,%d,%s\n", timestamp, backendInfo.Address, duration, totalBytes, status)
	requestsTotal.WithLabelValues(backendInfo.Address, status).Inc()
	requestDuration.WithLabelValues(backendInfo.Address).Observe(float64(durationSec))
}
