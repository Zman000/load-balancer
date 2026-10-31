package main

import (
	"flag"
	"fmt"
	"math/rand"
	"net"
	"time"
)

var delay time.Duration

func handleClient(conn net.Conn, baseDelay, jitter time.Duration) {
	defer conn.Close()

	// Drain the TCP receive buffer to prevent an OS RST upon closing
	buf := make([]byte, 2048)
	conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	conn.Read(buf)
	// Calculate realistic distributed latency
	actualDelay := baseDelay
	if jitter > 0 {
		randomJitter := time.Duration(rand.Int63n(int64(jitter)))
		actualDelay += randomJitter
	}

	// The delay happens here, completely isolated to this specific client
	time.Sleep(actualDelay)

	// Simulate successful response
	conn.Write([]byte("HTTP/1.1 200 OK\r\n\r\nBackend Response"))
}

func main() {
	port := flag.String("port", "9000", "Port to listen on")
	delay := flag.Duration("delay", 0, "Artificial delay (e.g., 500ms)")
	jitter := flag.Duration("jitter", 50*time.Millisecond, "Maximum random jitter to add")
	flag.Parse()

	listener, err := net.Listen("tcp", ":"+*port)
	if err != nil {
		fmt.Println("backend Error on listen")
		panic(err)
	}

	defer listener.Close()

	fmt.Printf("Backend listening on port %s with delay %v (jitter: %v)\n", *port, *delay, *jitter)

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Backend error on accept")
			panic(err)
		}

		//for allowing prometheus to detect active connection, slows down overall speed
		// time.Sleep(200 * time.Millisecond)

		go handleClient(conn, *delay, *jitter)
	}

}
