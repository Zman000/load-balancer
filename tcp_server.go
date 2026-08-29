package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

const configFile = "backends.conf"

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

func connectBackend(pool *BackendPool) (*Backend, net.Conn) {
	for {
		backendInfo := pool.AcquireBackend()

		if backendInfo == nil {
			return nil, nil
		}

		conn, err := net.DialTimeout("tcp", backendInfo.address, 1*time.Second)
		if err == nil {
			return backendInfo, conn
		}

		fmt.Printf("Backend %s is unreachable : %v\n", backendInfo.address, err)

		pool.ReleaseBackend(backendInfo)
		pool.SetHealth(backendInfo, false)
	}
}

func connectClientToBackend(conn net.Conn, pool *BackendPool) {
	defer conn.Close()

	// fmt.Println("Forwarding to:", backendInfo.address)

	backendInfo, backend := connectBackend(pool)

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

func loadBackends(filename string) ([]Backend, error) {
	file, err := os.Open(filename)
	if err != nil {
		fmt.Println("Error on loading config file")
		return nil, err
	}
	defer file.Close()

	var backends []Backend

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		addr := strings.TrimSpace(scanner.Text())
		if addr == "" {
			continue
		}
		backends = append(backends, Backend{
			address:           addr,
			activeConnections: 0,
			healthy:           true,
		})
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}

	if len(backends) == 0 {
		panic("no active backend found")
	}

	return backends, nil
}

func activeHealthCheck(p *BackendPool) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		for i := range p.backends {
			backend := &p.backends[i]
			conn, err := net.DialTimeout("tcp", backend.address, 1*time.Second)
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

	backends, err := loadBackends(configFile)
	if err != nil {
		fmt.Println("No backend configured")
		panic(err)
	}

	pool := BackendPool{
		backends: backends,
	}

	go activeHealthCheck(&pool)
	//function to test client connnections using loadTest file
	go pool.PrintStatus()

	//load balancer's listen
	lb_listener, err := net.Listen("tcp", "localhost:8080")
	if err != nil {
		fmt.Println("Error on load balancer listen")
		panic(err)
	}

	defer lb_listener.Close()

	fmt.Println("Server listening on port :8080")

	for {
		conn, err := lb_listener.Accept()
		if err != nil {
			fmt.Println("Error on accept")
			continue
		}
		// fmt.Println("Client connected ", conn.RemoteAddr())

		go connectClientToBackend(conn, &pool)
	}

}
