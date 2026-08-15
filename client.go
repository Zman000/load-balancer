package main

import (
	"fmt"
	"io"
	"net"
	"os"
)

func main() {
	conn, err := net.Dial("tcp", "localhost:8080")
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	file, err := os.Open("data.txt")
	if err != nil {
		panic(err)
	}
	defer file.Close()

	_, err = io.Copy(conn, file)
	if err != nil {
		panic(err)
	}

	fmt.Println("File sent successfully")

	buffer := make([]byte, 4096)

	n, err := conn.Read(buffer)
	if err != nil {
		panic(err)
	}

	fmt.Println("Response:", string(buffer[:n]))
}
