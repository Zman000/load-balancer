package main

import (
	"fmt"
	"net"
)

func handleClient(conn net.Conn) {
	defer conn.Close()

	//backend server, just a connection b/w load balancer and backend
	//errors with a single client should not break whole server
	backend, err := net.Dial("tcp", "localhost:9000")
	if err != nil {
		fmt.Println("Error on connecting to backend")
		return
	}
	defer backend.Close()

	buffer := make([]byte, 4096)

	n, err := conn.Read(buffer)
	if err != nil {
		fmt.Println("Error on read")
		return
	}

	_, err = backend.Write(buffer[:n])
	if err != nil {
		fmt.Println("Error on writing to backend")
		return
	}

	n, err = backend.Read(buffer)
	if err != nil {
		fmt.Println("Error on reading from backend")
		return
	}

	_, err = conn.Write(buffer[:n])
	if err != nil {
		fmt.Println("Error on writing to client connection")
		return
	}
}

func main() {
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

		go handleClient(conn)
	}

}
