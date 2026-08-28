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
	address string
}

type BackendPool struct {
	mu       sync.Mutex
	backends []Backend
	current  int
}

func connectClientToBackend(conn net.Conn, pool *BackendPool) {
	defer conn.Close()

	backendInfo := pool.Next()

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
		err := scanner.Err()
		if err != nil {
			return nil, err
		}

		addr := strings.TrimSpace(scanner.Text())
		if addr == "" {
			continue
		}
		backends = append(backends, Backend{
			address: addr,
		})
	}

	if len(backends) == 0 {
		panic("no active backend found")
	}

	return backends, nil
}

func (p *BackendPool) Next() Backend {
	p.mu.Lock()
	defer p.mu.Unlock()

	backend := p.backends[p.current]
	p.current = (p.current + 1) % len(p.backends)

	return backend
}

func main() {

	backends, err := loadBackends(configFile)

	pool := BackendPool{
		backends: backends,
		current:  0,
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
			panic(err)
		}
		fmt.Println("Client connected ", conn.LocalAddr().String())

		go connectClientToBackend(conn, &pool)
	}

}
