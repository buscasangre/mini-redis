package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
)

func main() {
	// Connect to the mini-redis server
	conn, err := net.Dial("tcp", "localhost:7777")
	if err != nil {
		fmt.Println("Error connecting to the server:", err)
		return
	}

	fmt.Println("connected to mini-redis server on port 7777")
	defer conn.Close()

	// Create a new reader to read user input
	reader := bufio.NewReader(os.Stdin)

	for {
		// Read user input from the console
		fmt.Print("mini-redis >>")
		command, _ := reader.ReadString('\n')

		// Send the command to the server
		_, err = conn.Write([]byte(command))
		if err != nil {
			fmt.Println("Error sending command:", err)
			return
		}

		// Read the response from the mini-redis server
		response, err := bufio.NewReader(conn).ReadString('\n')
		if err != nil {
			fmt.Println("Error receiving response:", err)
			return
		}

		// response from mini-redis server
		fmt.Println("Response:", strings.TrimSpace(response))
	}
}
