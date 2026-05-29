package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"time"
)

type NtfyResponse struct {
	Event   string `json:"event"`
	Message string `json:"message"`
}

func main() {
	Port := ":8080"
	ClientTopic := "https://ntfy.sh/tcp-over-ntfy-client/json"
	ServerTopic := "https://ntfy.sh/tcp-over-ntfy-server"

	resp, err := http.Get(ClientTopic)
	if err != nil {
		log.Fatalf("Failed to connect to ntfy: %v\n", err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			fmt.Printf("Error closing ntfy client connection: %v\n", err)
		}
	}()

	fmt.Println("listening for events from ntfy...")
	scanner := bufio.NewScanner(resp.Body)

	conn, err := net.DialTimeout("tcp", fmt.Sprintf("localhost%s", Port), time.Second*5)
	if err != nil {
		log.Fatalf("Failed to connect to port %s: %v\n", Port, err)
	}
	defer conn.Close()

	go sendServerConnectiontoNtfy(conn, ServerTopic)

	for scanner.Scan() {
		if len(scanner.Bytes()) == 0 {
			continue
		}

		var ntfyResponse NtfyResponse
		if err := json.Unmarshal(scanner.Bytes(), &ntfyResponse); err != nil {
			fmt.Printf("Error parsing json from ntfy: %v\n", err)
			continue
		}

		if ntfyResponse.Event == "message" {
			if _, err := conn.Write([]byte(ntfyResponse.Message)); err != nil {
				fmt.Printf("Failed to send data to local server: %v\n", err)
			}
		}
	}
	if err := scanner.Err(); err != nil {
		fmt.Printf("Stream Error: %v\n", err)
	}
}

func sendServerConnectiontoNtfy(conn net.Conn, ServerTopic string) {
	connectionBuffer := make([]byte, 1024)
	for {
		n, err := conn.Read(connectionBuffer)
		if n > 0 {
			resp, postErr := http.Post(ServerTopic, "application/octet-stream",
				bytes.NewReader(connectionBuffer[:n]))
			if postErr != nil {
				fmt.Printf("Error sending the data to ntfy: %v\n", err)
			}
			if err := resp.Body.Close(); err != nil {
				fmt.Printf("Error closing http Post: %v\n", err)
			}
		}
		if err != nil {
			if err == io.EOF {
				fmt.Println("Server closed connection gracefully")
			} else {
				fmt.Printf("Network error occurred: %v\n", err)
			}
			return
		}
	}
}
