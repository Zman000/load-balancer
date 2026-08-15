package main

import (
	"fmt"
	"net"
)

func handleClient(conn net.Conn) {
	defer conn.Close()

	buffer := make([]byte, 1026)
	n, err := conn.Read(buffer)
	// fmt.Println(string(buffer[:n]))
	if err != nil {
		fmt.Println("Error on read")
		panic(err)
	}
	message := buffer[:n]

	fmt.Println("Received:", string(message))

	confirmation := ([]byte("Recieved"))
	_, err = conn.Write(confirmation)
	if err != nil {
		fmt.Println("Error on write:", err)
		return
	}
}

func main() {
	listener, err := net.Listen("tcp", "localhost:9000")
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
