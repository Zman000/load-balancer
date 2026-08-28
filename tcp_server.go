package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
)

const configFile = "backends.conf"

type Backend struct {
	address           string
	activeConnections int
}

type BackendPool struct {
	mu       sync.Mutex
	backends []Backend
}

type ClientHandler struct {
	mu sync.Mutex
}

func connectClientToBackend(conn net.Conn, pool *BackendPool) {
	defer conn.Close()

	backendInfo := pool.AcquireBackend()
	defer pool.ReleaseBackend(backendInfo)

	fmt.Println("Forwarding to:", backendInfo.address)

	//backend server, just a connection b/w load balancer and backend
	//errors with a single client should not break whole server
	backend, err := net.Dial("tcp", backendInfo.address)
	if err != nil {
		fmt.Println("Error on connecting to backend")
		return
	}
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

func (p *BackendPool) AcquireBackend() *Backend {
	p.mu.Lock()
	defer p.mu.Unlock()

	// minConnection := p.backends[0].activeConnections
	backend := &p.backends[0]
	for i := 1; i < len(p.backends); i++ {
		if backend.activeConnections > p.backends[i].activeConnections {
			backend = &p.backends[i]
		}
	}

	backend.activeConnections += 1
	return backend
}

func (p *BackendPool) ReleaseBackend(backend *Backend) {
	p.mu.Lock()
	defer p.mu.Unlock()
	backend.activeConnections--
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
		fmt.Println("Client connected ", conn.RemoteAddr())

		go connectClientToBackend(conn, &pool)
	}

}
