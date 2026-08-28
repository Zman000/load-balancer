package main

import (
	"fmt"
	"io"
	"net"
	"os"
)

func handleClient(conn net.Conn) {
	defer conn.Close()

	n, err := io.Copy(io.Discard, conn)
	if err != nil {
		fmt.Println("Error on reading from client")
		return
	}

	confirmation := []byte(fmt.Sprintf("Recieved %d bytes", n))
	_, err = conn.Write(confirmation)
	if err != nil {
		fmt.Println("Error on write:", err)
		return
	}
}

func main() {
	port := os.Args[1]

	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		fmt.Println("backend Error on listen")
		panic(err)
	}

	defer listener.Close()

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Backend error on accept")
			panic(err)
		}

		go handleClient(conn)
	}

}
