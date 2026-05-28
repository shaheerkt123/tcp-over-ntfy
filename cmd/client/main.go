package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
)

func main() {
	Port := ":8080"

	listener, err := net.Listen("tcp", Port)
	if err != nil {
		log.Fatalf("Failed to listen on Port %v: %v\n", Port, err)
	}
	defer listener.Close()
	fmt.Printf("Server is listening on Port %v...\n", Port)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Fatalf("Failed to accept connection: %v\n", err)
			continue
		}
		go sendClientConnection(conn)
	}
}

func sendClientConnection(conn net.Conn) {
	ClientTopic := "https://ntfy.sh/tcp-over-ntfy-client"
	defer conn.Close()
	fmt.Printf("New connection from: %s\n", conn.RemoteAddr().String())
	connectionBuffer := make([]byte, 1024)
	for {
		n, err := conn.Read(connectionBuffer)
		if n > 0 {
			resp, postErr := http.Post(ClientTopic, "application/octet-stream",
				bytes.NewReader(connectionBuffer[:n]))
			if postErr != nil {
				fmt.Printf("Error sending the data to ntfy: %v\n", err)
			}
			closeErr := resp.Body.Close()
			if closeErr != nil {
				fmt.Printf("Error closing http Post: %v\n", err)
			}
		}
		if err != nil {
			if err == io.EOF {
				fmt.Println("Client closed connection gracefully")
			} else {
				fmt.Printf("Network error occurred: %v\n", err)
			}
			return
		}
	}
}
