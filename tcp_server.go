package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"gopkg.in/yaml.v3"
)

type Server struct {
	ListenAddress  string `yaml:"listen_address"`
	MaxConnections int    `yaml:"max_connections"`
}
type LoadBalancer struct {
	Algorithm         string `yaml:"algorithm"`
	ConnectionTimeout string `yaml:"connection_timeout"`
}
type HealthCheck struct {
	Enabled  bool   `yaml:"enabled"`
	Timeout  string `yaml:"timeout"`
	Interval string `yaml:"interval"`
}
type PrintStatus struct {
	Enabled bool `yaml:"enabled"`
}
type RateDecider struct {
	RequestsPerSecond int `yaml:"requests_per_sec"`
	BurstCapacity     int `yaml:"burst_cap"`
}
type CircuitBreakerConfig struct {
	MaxFailures int    `yaml:"max_failures"`
	Cooldown    string `yaml:"cooldown"`
}
type RetryConfig struct {
	MaxRetries     int    `yaml:"max_retries"`
	InitialBackoff string `yaml:"initial_backoff"`
}
type Config struct {
	Server         Server               `yaml:"server"`
	LoadBalancer   LoadBalancer         `yaml:"load_balancer"`
	HealthCheck    HealthCheck          `yaml:"health_check"`
	PrintStatus    PrintStatus          `yaml:"print_status"`
	RateDecider    RateDecider          `yaml:"rate_limit"`
	CircuitBreaker CircuitBreakerConfig `yaml:"circuit_breaker"`
	Retry          RetryConfig          `yaml:"retry"`
	Backends       []string             `yaml:"backends"`
}

type Backend struct {
	address           string
	activeConnections int
	healthy           bool
	failures          int
	state             BreakerState
	tripTime          time.Time
}

type BackendPool struct {
	mu            sync.Mutex
	backends      []Backend
	roundRobinIdx int
	algorithm     string
	cbMaxFailures int
	cbCooldown    time.Duration
}

type ClientHandler struct {
	mu sync.Mutex
}

type copyResult struct {
	err   error
	bytes int64
}

type BreakerState string

const (
	StateClosed   BreakerState = "closed"
	StateOpen     BreakerState = "open"
	StateHalfOpen BreakerState = "half-open"
)

func (p *BackendPool) RecordSuccess(backend *Backend) {
	p.mu.Lock()
	defer p.mu.Unlock()
	backend.failures = 0
	backend.state = StateClosed
}

func (p *BackendPool) RecordFailure(backend *Backend) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if backend.state == StateOpen {
		return
	}

	backend.failures++
	if backend.failures >= p.cbMaxFailures {
		backend.state = StateOpen
		backend.tripTime = time.Now()
		fmt.Printf("Circuit breaker tripped OPEN for %s\n", backend.address)
	}
}

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

func NewRateLimiter(rate, capacity float64) *RateLimiter {
	if capacity < 1 {
		capacity = 1 // Prevent negative token initialization
	}
	return &RateLimiter{
		clients:  make(map[string]*ClientLimiter),
		rate:     rate,
		capacity: capacity,
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

func loadConfig(filename string) (Config, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return Config{}, err
	}

	var config Config

	err = yaml.Unmarshal(data, &config)
	if err != nil {
		return Config{}, err
	}

	return config, nil
}

func connectBackend(pool *BackendPool, connectionTimeout time.Duration, maxRetries int, initialBackoff time.Duration) (*Backend, net.Conn) {
	backoff := initialBackoff

	for attempt := 0; attempt <= maxRetries; attempt++ {
		backendInfo := pool.AcquireBackend()

		if backendInfo == nil {
			return nil, nil
		}

		conn, err := net.DialTimeout("tcp", backendInfo.address, connectionTimeout)
		if err == nil {
			pool.RecordSuccess(backendInfo)
			return backendInfo, conn
		}

		fmt.Printf("Backend %s is unreachable : %v\n", backendInfo.address, err)

		pool.ReleaseBackend(backendInfo)
		pool.RecordFailure(backendInfo)

		if attempt < maxRetries {
			fmt.Printf("Connection failed. Retrying in %v...\n", backoff)
			time.Sleep(backoff)
			backoff *= 2
		}
		// pool.SetHealth(backendInfo, false)
	}
	return nil, nil
}

func connectClientToBackend(conn net.Conn, pool *BackendPool, timeout time.Duration, maxRetries int, initialBackoff time.Duration) {
	// Record the timestamp at accept/start
	startTime := time.Now()

	defer conn.Close()

	// fmt.Println("Forwarding to:", backendInfo.address)

	backendInfo, backend := connectBackend(pool, timeout, maxRetries, initialBackoff)

	if backendInfo == nil {
		fmt.Printf("%s,NONE,0,0,error_no_backend\n", time.Now().Format(time.RFC3339))
		return
	}

	defer pool.ReleaseBackend(backendInfo)
	defer backend.Close()

	// Add deadline to this connection (5 min)
	deadline := time.Now().Add(5 * time.Minute)
	conn.SetDeadline(deadline)
	backend.SetDeadline(deadline)

	//use struct channel to get both error and byte count
	res := make(chan copyResult, 2)

	go func() {
		n, err := io.Copy(conn, backend)
		if tcp, ok := conn.(*net.TCPConn); ok {
			if closeErr := tcp.CloseWrite(); err == nil {
				err = closeErr
			}
		}
		res <- copyResult{err: err, bytes: n}
	}()

	go func() {
		n, err := io.Copy(backend, conn)
		if tcp, ok := backend.(*net.TCPConn); ok {
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
	timestamp := time.Now().Format(time.RFC3339)

	// Output CSV line: timestamp,backend,duration_ms,bytes,status
	fmt.Printf("%s,%s,%d,%d,%s\n", timestamp, backendInfo.address, duration, totalBytes, status)
}

func loadBackends(config Config) ([]Backend, error) {

	var backends []Backend
	for _, backend := range config.Backends {
		addr := strings.TrimSpace(backend)
		if addr == "" {
			continue
		}
		backends = append(backends, Backend{
			address:           addr,
			activeConnections: 0,
			healthy:           true,
			state:             StateClosed,
		})
	}
	if len(backends) == 0 {
		return nil, fmt.Errorf("no active backend found")
	}

	return backends, nil
}

func activeHealthCheck(p *BackendPool, interval time.Duration, timeout time.Duration, shutdown <-chan struct{}) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			for i := range p.backends {
				backend := &p.backends[i]
				conn, err := net.DialTimeout("tcp", backend.address, timeout)
				healthy := err == nil

				if conn != nil {
					conn.Close()
				}

				p.SetHealth(backend, healthy)
			}
		case <-shutdown:

			return
		}
	}
}

func (p *BackendPool) AcquireBackend() *Backend {
	p.mu.Lock()
	defer p.mu.Unlock()

	isEligible := func(b *Backend) bool {
		if !b.healthy {
			return false
		}
		if b.state == StateOpen {
			if time.Since(b.tripTime) > p.cbCooldown {
				b.state = StateHalfOpen
				fmt.Printf("Circuit breaker HALF-OPEN for %s\n", b.address)
				return true
			}
			return false
		}
		return true
	}
	var backend *Backend
	if p.algorithm == "round_robin" {
		for i := 0; i < len(p.backends); i++ {
			idx := (p.roundRobinIdx + i) % len(p.backends)
			if isEligible(&p.backends[idx]) {
				backend = &p.backends[idx]
				p.roundRobinIdx = (idx + 1) % len(p.backends)
				break
			}
		}
	} else {
		for i := range p.backends {
			if !isEligible(&p.backends[i]) {
				continue
			}

			if backend == nil || backend.activeConnections > p.backends[i].activeConnections {
				backend = &p.backends[i]
			}
		}
	}

	if backend == nil {
		return nil
	}

	backend.activeConnections += 1
	return backend
}

func (p *BackendPool) ReleaseBackend(backend *Backend) {
	p.mu.Lock()
	defer p.mu.Unlock()
	backend.activeConnections--
}

func (p *BackendPool) SetHealth(backend *Backend, health bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	backend.healthy = health
}

// recieve only channel
func (p *BackendPool) PrintStatus(shutdown <-chan struct{}) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			p.mu.Lock()

			fmt.Println("\n--- Backend Status ---")
			for _, backend := range p.backends {
				if backend.healthy {
					fmt.Printf("%s -> %d active connections\n", backend.address, backend.activeConnections)
				} else {
					fmt.Printf("%s -> Unhealthy\n", backend.address)
				}
			}

			p.mu.Unlock()
		case <-shutdown:
			return
		}

	}
}

func main() {

	filename := "config.yaml"
	config, err := loadConfig(filename)
	if err != nil {
		fmt.Println("Error loading config:", err)
		panic(err)
	}

	maxConnections := config.Server.MaxConnections
	limiter := NewRateLimiter(float64(config.RateDecider.RequestsPerSecond), float64(config.RateDecider.BurstCapacity))

	cbCooldown, err := time.ParseDuration(config.CircuitBreaker.Cooldown)
	if err != nil {
		panic(err)
	}

	intitialBackoff, err := time.ParseDuration(config.Retry.InitialBackoff)
	if err != nil {
		panic(err)
	}
	backends, err := loadBackends(config)
	if err != nil {
		fmt.Println("No backend configured")
		panic(err)
	}

	pool := BackendPool{
		backends:      backends,
		algorithm:     config.LoadBalancer.Algorithm,
		cbMaxFailures: config.CircuitBreaker.MaxFailures,
		cbCooldown:    cbCooldown,
	}

	connectionTimeout, err := time.ParseDuration(config.LoadBalancer.ConnectionTimeout)
	if err != nil {
		panic(err)
	}

	connectionSemaphore := make(chan struct{}, maxConnections)

	activeConns := make(map[net.Conn]struct{})
	var activeConnsMu sync.Mutex

	//load balancer's listen
	lbListener, err := net.Listen("tcp", config.Server.ListenAddress)
	if err != nil {
		fmt.Println("Error on load balancer listen")
		panic(err)
	}
	defer lbListener.Close()
	fmt.Println("Server listening on", config.Server.ListenAddress)

	var wg sync.WaitGroup
	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signalChannel)

	shutdown := make(chan struct{})

	//active health checks at intervals
	if config.HealthCheck.Enabled {
		interval, err := time.ParseDuration(config.HealthCheck.Interval)
		if err != nil {
			panic(err)
		}

		timeout, err := time.ParseDuration(config.HealthCheck.Timeout)
		if err != nil {
			panic(err)
		}

		go activeHealthCheck(&pool, interval, timeout, shutdown)

	}

	if config.PrintStatus.Enabled {
		//function to test client connnections using loadTest file
		go pool.PrintStatus(shutdown)
	}

	// Wait for the shutdown signal and then close the listener.
	go func() {
		<-signalChannel

		fmt.Println("\nShutdown requested")

		close(shutdown)
		lbListener.Close()
	}()

	for {
		conn, err := lbListener.Accept()
		// fmt.Println("Client connected ", conn.RemoteAddr())
		if err != nil {
			select {
			case <-shutdown:
				goto shutdown
			default:
				fmt.Println("Error on accept :", err)
				continue
			}
		}

		clientIP := ""
		if tcpAddr, ok := conn.RemoteAddr().(*net.TCPAddr); ok {
			clientIP = tcpAddr.IP.String()
		}

		if clientIP != "" && !limiter.Allow(clientIP) {
			fmt.Printf("Rate limit exceeded for %s\n", clientIP)
			conn.Write([]byte("HTTP/1.1 429 Too Many Requests\r\n\r\n"))
			conn.Close()
			continue
		}

		select {
		case connectionSemaphore <- struct{}{}:

		default:
			//maximum number of connection reached
			fmt.Println("Reached connection limit")
			conn.Close()
			continue
		}

		activeConnsMu.Lock()
		activeConns[conn] = struct{}{}
		activeConnsMu.Unlock()

		wg.Add(1)
		go func() {
			defer wg.Done()
			// Return the semaphore slot.
			defer func() {
				<-connectionSemaphore
			}()
			defer func() {
				activeConnsMu.Lock()
				delete(activeConns, conn)
				activeConnsMu.Unlock()
			}()
			connectClientToBackend(conn, &pool, connectionTimeout, config.Retry.MaxRetries, intitialBackoff)
		}()
	}

shutdown:
	fmt.Println("Stopped accepting new connections")
	fmt.Println("Waiting for active connections to finish...")
	//create context for 30 seconds
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	//channel to signal when all goroutines are finished (on their own)
	waitCh := make(chan struct{})
	go func() {
		wg.Wait()
		close(waitCh)
	}()

	select {
	case <-waitCh:
		fmt.Println("All active connections finished gracefully. Server stopped.")
	case <-ctx.Done():
		fmt.Println("Shutdown timeout reached. Force closing all connections")
		activeConnsMu.Lock()
		for conn := range activeConns {
			conn.Close()
		}
		activeConnsMu.Unlock()
	}

}
