package main

import (
	"flag"
	"fmt"
	"io"
	"net"
	"time"
)

var delay time.Duration

func handleClient(conn net.Conn) {
	defer conn.Close()

	n, err := io.Copy(io.Discard, conn)
	if err != nil {
		fmt.Println("Error on reading from client")
		return
	}
	// Apply artificial latency if configured
	if delay > 0 {
		time.Sleep(delay)
	}

	confirmation := []byte(fmt.Sprintf("Recieved %d bytes", n))
	_, err = conn.Write(confirmation)
	if err != nil {
		fmt.Println("Error on write:", err)
		return
	}
	// fmt.Println(confirmation)
}

func main() {
	port := flag.String("port", "8081", "Port to listen on")
	delayFlag := flag.Duration("delay", 0, "Artificial delay (e.g., 500ms)")
	flag.Parse()

	delay = *delayFlag
	listener, err := net.Listen("tcp", ":"+*port)
	if err != nil {
		fmt.Println("backend Error on listen")
		panic(err)
	}

	defer listener.Close()

	fmt.Printf("Backend listening on port %s with delay %v\n", *port, *delayFlag)

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Backend error on accept")
			panic(err)
		}

		go handleClient(conn)
	}

}
