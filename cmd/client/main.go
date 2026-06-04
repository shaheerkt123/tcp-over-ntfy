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
	ServerTopic := "https://ntfy.sh/tcp-over-ntfy-server"
	IndexTopic := "https://ntfy.sh/tcp-over-ntfy-index"

	listener, err := net.Listen("tcp", Port)
	if err != nil {
		log.Fatalf("Failed to listen on Port %v: %v\n", Port, err)
	}
	defer listener.Close()
	fmt.Printf("Server is listening on Port %v...\n", Port)

	index := 0
	for {
		localClientConnection, err := listener.Accept()
		if err != nil {
			fmt.Printf("Failed to accept connection: %v\n", err)
			continue
		}
		http.Post(IndexTopic, "text/plain", strings.NewReader(strconv.Itoa(index)))
		fmt.Printf("New connection from: %s\n", localClientConnection.RemoteAddr().String())

		go handleConnection(localClientConnection,
			fmt.Sprintf("%s-%d/json?cache=no", ServerTopic, index),
			fmt.Sprintf("%s-%d", ClientTopic, index))

		index++
	}
}

func handleConnection(localClientConnection net.Conn, listenTopic string, publishTopic string) {
	done := make(chan struct{})

	go func() {
		recieveRemoteServerDataFromNtfy(localClientConnection, listenTopic)
		select {
		case <-done:
			return
		default:
			close(done)
		}
	}()
	go func() {
		sendLocalClientDataToNtfy(localClientConnection, publishTopic)
		select {
		case <-done:
			return
		default:
			close(done)
		}
	}()

	<-done
	localClientConnection.Close()
}

func sendLocalClientDataToNtfy(localClientConnection net.Conn, ClientTopic string) {
	connectionBuffer := make([]byte, 1024)
	for {
		n, err := localClientConnection.Read(connectionBuffer)
		fmt.Printf("local client data: %v\n", string(connectionBuffer[:n]))
		if n > 0 {
			resp, postErr := http.Post(ClientTopic, "application/octet-stream",
				strings.NewReader(base64.StdEncoding.EncodeToString(connectionBuffer[:n])))
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

func recieveRemoteServerDataFromNtfy(localClientConnection net.Conn, ServerTopic string) {
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
		go sendScannerDataTolocalClient(scanner, localClientConnection)
	}
	if err := scanner.Err(); err != nil {
		fmt.Printf("Stream Error: %v\n", err)
	}
}

func sendScannerDataTolocalClient(scanner *bufio.Scanner, localClientConnection net.Conn) {
	if len(scanner.Bytes()) == 0 {
		return
	}

	var ntfyResponse NtfyResponse
	if err := json.Unmarshal(scanner.Bytes(), &ntfyResponse); err != nil {
		fmt.Printf("Error parsing json from ntfy: %v\n", err)
		return
	}

	if ntfyResponse.Event == "message" {
		ntfyServerDataBytes, err := base64.StdEncoding.DecodeString(ntfyResponse.Message)
		if err != nil {
			fmt.Printf("Error converting base64 to Bytes: %v\n", err)
			return
		}
		fmt.Printf("remote server data: %v\n", string(ntfyServerDataBytes))
		if _, err := localClientConnection.Write(ntfyServerDataBytes); err != nil {
			fmt.Printf("Failed to send data to local client: %v\n", err)
			return
		}
	}
}
