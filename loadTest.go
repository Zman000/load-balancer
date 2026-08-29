package main

import (
	"fmt"
	"io"
	"math/rand"
	"net"
	"sync"
	"time"
)

const (
	numClients = 100
)

func client(id int, wg *sync.WaitGroup) {
	defer wg.Done()

	conn, err := net.Dial("tcp", "localhost:8080")
	if err != nil {
		fmt.Printf("Client %d: connection failed: %v\n", id, err)
		return
	}
	defer conn.Close()

	// Send a small request.
	message := fmt.Sprintf("Hello from client %d", id)

	_, err = conn.Write([]byte(message))
	if err != nil {
		fmt.Printf("Client %d: write failed: %v\n", id, err)
		return
	}

	// waiting for random time to make connection feel alive
	delay := time.Duration(rand.Intn(5000)+1000) * time.Millisecond

	fmt.Printf("Client %d: connected, staying for %v\n", id, delay)

	time.Sleep(delay)

	tcpConn := conn.(*net.TCPConn)

	//TCP half close from client side
	err = tcpConn.CloseWrite()
	if err != nil {
		fmt.Printf("Client %d: CloseWrite failed: %v\n", id, err)
		return
	}

	buffer := make([]byte, 4096)

	n, err := conn.Read(buffer)
	if err != nil && err != io.EOF {
		fmt.Printf("Client %d: read failed: %v\n", id, err)
		return
	}

	if n > 0 {
		fmt.Printf("Client %d: response: %s\n",
			id, string(buffer[:n]))
	}
}

func main() {

	var wg sync.WaitGroup

	wg.Add(numClients)

	for i := 1; i <= numClients; i++ {
		go client(i, &wg)

		time.Sleep(10 * time.Millisecond)
	}

	wg.Wait()

	fmt.Println("All clients finished.")
}
