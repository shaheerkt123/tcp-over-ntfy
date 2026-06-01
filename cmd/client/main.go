package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
)

type NtfyResponse struct {
	Event   string `json:"event"`
	Message string `json:"message"`
}

func main() {
	Port := ":8081"
	ClientTopic := "https://ntfy.sh/tcp-over-ntfy-client"
	ServerTopic := "https://ntfy.sh/tcp-over-ntfy-server/json"

	listener, err := net.Listen("tcp", Port)
	if err != nil {
		log.Fatalf("Failed to listen on Port %v: %v\n", Port, err)
	}
	defer listener.Close()
	fmt.Printf("Server is listening on Port %v...\n", Port)

	for {
		localClientConnection, err := listener.Accept()
		if err != nil {
			log.Fatalf("Failed to accept connection: %v\n", err)
			continue
		}
		fmt.Printf("New connection from: %s\n", localClientConnection.RemoteAddr().String())

		defer localClientConnection.Close()
		go sendLocalClientDataToNtfy(localClientConnection, ServerTopic)
		go recieveRemoteServerDataFromNtfy(localClientConnection, ClientTopic)
	}
}

func sendLocalClientDataToNtfy(conn net.Conn, ClientTopic string) {
	connectionBuffer := make([]byte, 1024)
	for {
		n, err := conn.Read(connectionBuffer)
		fmt.Printf("local client data: %v\n", string(connectionBuffer[:n]))
		if n > 0 {
			resp, postErr := http.Post(ClientTopic, "application/octet-stream", strings.NewReader(fmt.Sprintf("%v", connectionBuffer[:n])))
			if postErr != nil {
				fmt.Printf("Error sending the local client data to ntfy: %v\n", err)
			}
			if err := resp.Body.Close(); err != nil {
				fmt.Printf("Error closing http Post: %v\n", err)
			}
		}
		if err != nil {
			if err == io.EOF {
				fmt.Println("local Client closed connection gracefully")
			} else {
				fmt.Printf("Network error occurred: %v\n", err)
			}
			return
		}
	}
}

func recieveRemoteServerDataFromNtfy(conn net.Conn, ServerTopic string) {
	resp, err := http.Get(ServerTopic)
	if err != nil {
		log.Fatalf("Failed to connect to ntfy: %v\n", err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			fmt.Printf("Error closing ntfy connection: %v\n", err)
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
			ntfyServerDataBytes, err := asciiToByte(ntfyResponse.Message)
			if err != nil {
				fmt.Printf("Error converting data to Byte: %v\n", err)
				continue
			}
			fmt.Printf("remote server data: %v\n", string(ntfyServerDataBytes))
			if _, err := conn.Write(ntfyServerDataBytes); err != nil {
				fmt.Printf("Failed to send data to client: %v\n", err)
				continue
			}
		}
	}
	if err := scanner.Err(); err != nil {
		fmt.Printf("Stream Error: %v\n", err)
	}
}

func asciiToByte(ascii string) ([]byte, error) {
	cleaned := strings.Trim(ascii, "[]")
	fields := strings.Fields(cleaned)
	result := make([]byte, len(fields))
	for i, field := range fields {
		val, err := strconv.Atoi(field)
		if err != nil {
			return nil, fmt.Errorf("error converting '%s' at index %d to uint8: %w", field, i, err)
		}
		if val < 0 || val > 255 {
			return nil, fmt.Errorf("value '%d' at index %d out of uint8 range", val, i)
		}

		result[i] = byte(val)
	}
	return result, nil
}
