package main

import (
	"bufio"
	"fmt"
	"net"
)


// Process All commands for MINI-REDIS Here
func processMiniRedisClientRequest(request string) string {
	return "Mini Redis is Connected."
}

func miniRedisClientConnection(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	for {

		// reads mini-redis client request
		request, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("Error reading mini-redis Client request:", err)
			return
		}

		// Process the request
		response := processMiniRedisClientRequest(request)

		// Send the response back to the client
		_, err = writer.WriteString(response + "\n")
		if err != nil {
			fmt.Println("Error sending response to mini-redis Client:", err)
			return
		}

		// ensuring that the response is sent to the client immediately instead of waiting for the buffer
		err = writer.Flush()
		if err != nil {
			fmt.Println("Error flushing writer:", err)
			return
		}
	}
}

func main() {
	
	// Start a Mini-Redis server listening on port 7777
	listener, err := net.Listen("tcp", ":7777")
	if err != nil {
		fmt.Println("Error starting the mini-redis server:", err)
		return
	}

	defer listener.Close()
	fmt.Println("mini-redis server listening on port 7777...")

	for {

		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting mini-redis Client connection:", err)
			return
		}

		go miniRedisClientConnection(conn)
	}
}
