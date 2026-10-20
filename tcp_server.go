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
	ListenAddress string `yaml:"listen_address"`
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
type Config struct {
	Server       Server       `yaml:"server"`
	LoadBalancer LoadBalancer `yaml:"load_balancer"`
	HealthCheck  HealthCheck  `yaml:"health_check"`
	PrintStatus  PrintStatus  `yaml:"print_status"`
	Backends     []string     `yaml:"backends"`
}

type Backend struct {
	address           string
	activeConnections int
	healthy           bool
}

type BackendPool struct {
	mu            sync.Mutex
	backends      []Backend
	roundRobinIdx int
	algorithm     string
}

type ClientHandler struct {
	mu sync.Mutex
}

type copyResult struct {
	err   error
	bytes int64
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

func connectBackend(pool *BackendPool, connectionTimeout time.Duration) (*Backend, net.Conn) {
	for {
		backendInfo := pool.AcquireBackend()

		if backendInfo == nil {
			return nil, nil
		}

		conn, err := net.DialTimeout("tcp", backendInfo.address, connectionTimeout)
		if err == nil {
			return backendInfo, conn
		}

		fmt.Printf("Backend %s is unreachable : %v\n", backendInfo.address, err)

		pool.ReleaseBackend(backendInfo)
		pool.SetHealth(backendInfo, false)
	}
}

func connectClientToBackend(conn net.Conn, pool *BackendPool, timeout time.Duration) {
	// Record the timestamp at accept/start
	startTime := time.Now()

	defer conn.Close()

	// fmt.Println("Forwarding to:", backendInfo.address)

	backendInfo, backend := connectBackend(pool, timeout)

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

	var backend *Backend
	if p.algorithm == "round_robin" {
		for i := 0; i < len(p.backends); i++ {
			idx := (p.roundRobinIdx + i) % len(p.backends)
			if p.backends[idx].healthy {
				backend = &p.backends[idx]
				p.roundRobinIdx = (idx + 1) % len(p.backends)
				break
			}
		}
	} else {
		for i := range p.backends {
			if !p.backends[i].healthy {
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

	const maxConnections = 500

	filename := "config.yaml"
	config, err := loadConfig(filename)
	if err != nil {
		fmt.Println("Error loading config:", err)
		panic(err)
	}

	backends, err := loadBackends(config)
	if err != nil {
		fmt.Println("No backend configured")
		panic(err)
	}

	pool := BackendPool{
		backends:  backends,
		algorithm: config.LoadBalancer.Algorithm,
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
			connectClientToBackend(conn, &pool, connectionTimeout)
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
