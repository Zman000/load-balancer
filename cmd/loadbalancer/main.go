package main

import (
	"LB/internal/config"
	"LB/internal/pool"
	"LB/internal/proxy"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type ClientHandler struct {
	mu sync.Mutex
}

type copyResult struct {
	err   error
	bytes int64
}

func loadBackends(config config.Config) ([]pool.Backend, error) {

	var backends []pool.Backend
	for _, backend := range config.Backends {
		addr := strings.TrimSpace(backend)
		if addr == "" {
			continue
		}
		backends = append(backends, pool.Backend{
			Address:           addr,
			ActiveConnections: 0,
			Healthy:           true,
			State:             pool.StateClosed,
		})
	}
	if len(backends) == 0 {
		return nil, fmt.Errorf("no active backend found")
	}

	return backends, nil
}

func activeHealthCheck(p *pool.BackendPool, interval time.Duration, timeout time.Duration, shutdown <-chan struct{}) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			for i := range p.Backends {
				backend := &p.Backends[i]
				conn, err := net.DialTimeout("tcp", backend.Address, timeout)
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

func main() {

	filename := "config.yaml"
	config, err := config.Load(filename)
	if err != nil {
		slog.Error("Error loading config", "error", err)
		panic(err)
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	maxConnections := config.Server.MaxConnections
	limiter := proxy.NewRateLimiter(float64(config.RateDecider.RequestsPerSecond), float64(config.RateDecider.BurstCapacity))

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
		slog.Error("no backend configured", "error", err)
		panic(err)
	}

	lbPool := pool.BackendPool{ // Added pool prefix and renamed variable to avoid package clash
		Backends:      backends,
		Algorithm:     config.LoadBalancer.Algorithm,
		CbMaxFailures: config.CircuitBreaker.MaxFailures,
		CbCooldown:    cbCooldown,
	}

	go func() {
		http.Handle("/metrics", promhttp.Handler())

		http.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
			status := lbPool.GetStatusSnapshot()
			w.Header().Set("Content-Type", "application/json")
			encoder := json.NewEncoder(w)
			encoder.SetIndent("", " ")
			encoder.Encode(status)
		})
		slog.Info("observability server listening on", "port", "2112", "Routes: /metrics, /status)")
		if err := http.ListenAndServe(":2112", nil); err != nil {
			slog.Warn("Observability server failed ", "error", err)
		}
	}()

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
		slog.Error("error on load balancer listen", "error", err)
		panic(err)
	}
	defer lbListener.Close()
	slog.Info("server listening on", "address", config.Server.ListenAddress)

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

		go activeHealthCheck(&lbPool, interval, timeout, shutdown)

	}

	// if config.PrintStatus.Enabled {
	// 	//function to test client connnections using loadTest file
	// 	go pool.PrintStatus(shutdown)
	// }

	// Wait for the shutdown signal and then close the listener.
	go func() {
		<-signalChannel

		slog.Info("shutdown requested")

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
				slog.Warn("error on accept", "error", err)
				continue
			}
		}

		clientIP := ""
		if tcpAddr, ok := conn.RemoteAddr().(*net.TCPAddr); ok {
			clientIP = tcpAddr.IP.String()
		}

		if clientIP != "" && !limiter.Allow(clientIP) {
			slog.Warn("rate limit exceeded", "client_ip", clientIP)
			conn.Write([]byte("HTTP/1.1 429 Too Many Requests\r\n\r\n"))
			conn.Close()
			continue
		}

		select {
		case connectionSemaphore <- struct{}{}:

		default:
			//maximum number of connection reached
			slog.Warn("reached connection limit")
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
			proxy.ConnectClientToBackend(conn, &lbPool, connectionTimeout, config.Retry.MaxRetries, intitialBackoff)
		}()
	}

shutdown:
	slog.Info("stopped accepting new connections")
	slog.Info("Waiting for active connections to finish...")
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
		slog.Info("All active connections finished gracefully. Server stopped.")
	case <-ctx.Done():
		slog.Info("Shutdown timeout reached. Force closing all connections")
		activeConnsMu.Lock()
		for conn := range activeConns {
			conn.Close()
		}
		activeConnsMu.Unlock()
	}

}
