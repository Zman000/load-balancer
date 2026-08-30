package main

import (
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v3"
)

const configFile = "backends.conf"

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
	mu       sync.Mutex
	backends []Backend
}

type ClientHandler struct {
	mu sync.Mutex
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
	defer conn.Close()

	// fmt.Println("Forwarding to:", backendInfo.address)

	backendInfo, backend := connectBackend(pool, timeout)

	if backendInfo == nil {
		fmt.Println("No healthy backend available")
		return
	}

	defer pool.ReleaseBackend(backendInfo)
	defer backend.Close()

	//use io functions to handle concurrent reading/writing
	res := make(chan error, 2)
	go func() {
		_, err := io.Copy(conn, backend)
		if tcp, ok := conn.(*net.TCPConn); ok {
			if closeErr := tcp.CloseWrite(); err == nil {
				err = closeErr
			}
		}
		res <- err
	}()

	go func() {
		_, err := io.Copy(backend, conn)
		if tcp, ok := backend.(*net.TCPConn); ok {
			if closeErr := tcp.CloseWrite(); err == nil {
				err = closeErr
			}
		}
		res <- err
	}()

	<-res
	<-res

}

func loadBackends(config Config) ([]Backend, error) {
	// file, err := os.Open(filename)
	// if err != nil {
	// 	fmt.Println("Error on loading config file")
	// 	return nil, err
	// }
	// defer file.Close()

	var backends []Backend

	// scanner := bufio.NewScanner(file)

	// for scanner.Scan() {
	// 	addr := strings.TrimSpace(scanner.Text())
	// 	if addr == "" {
	// 		continue
	// 	}
	// 	backends = append(backends, Backend{
	// 		address:           addr,
	// 		activeConnections: 0,
	// 		healthy:           true,
	// 	})
	// }
	// if err := scanner.Err(); err != nil {
	// 	return nil, err
	// }

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

func activeHealthCheck(p *BackendPool, interval time.Duration, timeout time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		for i := range p.backends {
			backend := &p.backends[i]
			conn, err := net.DialTimeout("tcp", backend.address, timeout)
			healthy := err == nil

			if conn != nil {
				conn.Close()
			}

			p.SetHealth(backend, healthy)
		}
		<-ticker.C
	}
}

func (p *BackendPool) AcquireBackend() *Backend {
	p.mu.Lock()
	defer p.mu.Unlock()

	var backend *Backend

	for i := range p.backends {
		if !p.backends[i].healthy {
			continue
		}

		if backend == nil || backend.activeConnections > p.backends[i].activeConnections {
			backend = &p.backends[i]
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

func (p *BackendPool) PrintStatus() {
	for {
		time.Sleep(1 * time.Second)

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
	}
}

func main() {

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
		backends: backends,
	}

	if config.HealthCheck.Enabled {
		interval, err := time.ParseDuration(config.HealthCheck.Interval)
		if err != nil {
			panic(err)
		}

		timeout, err := time.ParseDuration(config.HealthCheck.Timeout)
		if err != nil {
			panic(err)
		}

		go activeHealthCheck(&pool, interval, timeout)

	}

	connectionTimeout, err := time.ParseDuration(config.LoadBalancer.ConnectionTimeout)
	if err != nil {
		panic(err)
	}

	if config.PrintStatus.Enabled {
		//function to test client connnections using loadTest file
		go pool.PrintStatus()
	}

	//load balancer's listen
	lb_listener, err := net.Listen("tcp", config.Server.ListenAddress)
	if err != nil {
		fmt.Println("Error on load balancer listen")
		panic(err)
	}

	defer lb_listener.Close()

	fmt.Println("Server listening on", config.Server.ListenAddress)

	for {
		conn, err := lb_listener.Accept()
		if err != nil {
			fmt.Println("Error on accept")
			continue
		}
		// fmt.Println("Client connected ", conn.RemoteAddr())

		go connectClientToBackend(conn, &pool, connectionTimeout)
	}

}
