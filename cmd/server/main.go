package main

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"time"
)

type NtfyResponse struct {
	Event   string `json:"event"`
	Message string `json:"message"`
}

func main() {
	Port := ":8080"
	ClientTopic := "https://ntfy.sh/tcp-over-ntfy-client"
	ServerTopic := "https://ntfy.sh/tcp-over-ntfy-server"
	IndexTopic := "https://ntfy.sh/tcp-over-ntfy-index"

	resp, err := http.Get(fmt.Sprintf("%s/json?cache=no", IndexTopic))
	if err != nil {
		fmt.Printf("Error connecting to ntfy index: %v\n", err)
		return
	}
	defer resp.Body.Close()
	scanner := bufio.NewScanner(resp.Body)
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
			index := ntfyResponse.Message
			go handleConnection(
				fmt.Sprintf("%s-%s/json?cache=no", ClientTopic, index),
				fmt.Sprintf("%s-%s", ServerTopic, index),
				Port)
		}
	}
	if err := scanner.Err(); err != nil {
		fmt.Printf("Stream Error: %v\n", err)
	}
}

func handleConnection(listenTopic string, publishTopic string, port string) {
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("localhost%s", port), time.Second*5)
	if err != nil {
		log.Fatalf("Failed to connect to port %s: %v\n", port, err)
	}
	fmt.Println("successfully connected to port", port)

	done := make(chan struct{})

	go func() {
		sendLocalServerDataToNtfy(conn, publishTopic)
		select {
		case <-done:
			return
		default:
			close(done)
		}
	}()
	go func() {
		recieveRemoteClientDataFromNtfy(conn, listenTopic)
		select {
		case <-done:
			return
		default:
			close(done)
		}
	}()
	<-done
	conn.Close()
}

func sendLocalServerDataToNtfy(conn net.Conn, ServerTopic string) {
	connectionBuffer := make([]byte, 1024)
	for {
		n, err := conn.Read(connectionBuffer)
		if n > 0 {
			fmt.Printf("local server data: %v\n", string(connectionBuffer[:n]))
			resp, postErr := http.Post(ServerTopic, "application/octet-stream",
				strings.NewReader(base64.StdEncoding.EncodeToString(connectionBuffer[:n])))
			if postErr != nil {
				fmt.Printf("Error sending data to ntfy: %v\n", err)
			}
			if err := resp.Body.Close(); err != nil {
				fmt.Printf("Error closing http Post: %v\n", err)
			}
			fmt.Println("successfully sent local server data to ntfy")
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

func recieveRemoteClientDataFromNtfy(conn net.Conn, ClientTopic string) {
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
			convertedToByte, err := base64.StdEncoding.DecodeString(ntfyResponse.Message)
			if err != nil {
				fmt.Printf("Error converting base64 to bytes: %v\n", err)
				continue
			}
			n, err := conn.Write(convertedToByte)
			if err != nil {
				fmt.Printf("Failed to send data to local server: %v\n", err)
			}
			fmt.Printf("Message length: %v, Sent: %v\n", len([]byte(ntfyResponse.Message)), n)
			fmt.Printf("remote client request: %v\n", (ntfyResponse.Message))
		}
	}
	if err := scanner.Err(); err != nil {
		fmt.Printf("Stream Error: %v\n", err)
	}
}
