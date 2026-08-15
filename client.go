package main

import (
	"fmt"
	"net"
)

func main() {
	conn, err := net.Dial("tcp", "localhost:8080")
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	str := "Hello! Sending msg from client"

	_, err = conn.Write([]byte(str))
	if err != nil {
		panic(err)
	}

	buffer := make([]byte, 4096)

	n, err := conn.Read(buffer)
	if err != nil {
		panic(err)
	}

	fmt.Println("Response:", string(buffer[:n]))
}
