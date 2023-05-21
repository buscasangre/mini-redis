package main

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

type KeyValue struct {
	Key        string      `json:"key"`
	Value      interface{} `json:"value"`
	ExpiryTime time.Time   `json:"expiry_time"`
}

type MiniRedis struct {
	data    map[string]KeyValue
	expires map[string]time.Time
	mutex   sync.Mutex
}

func NewMiniRedis() *MiniRedis {
	return &MiniRedis{
		data:    make(map[string]KeyValue),
		expires: make(map[string]time.Time),
	}
}

func (mr *MiniRedis) Get(key string) (interface{}, bool) {
	mr.mutex.Lock()
	defer mr.mutex.Unlock()

	item, ok := mr.data[key]
	if !ok {
		return nil, false
	}

	// Check if the key has expired
	if item.ExpiryTime.Before(time.Now()) {
		mr.deleteKeyAfterExpiry(key, item.ExpiryTime)
		return nil, false
	}

	return item.Value, true
}

func (mr *MiniRedis) Set(key string, value interface{}, expiry time.Duration) {
	mr.mutex.Lock()
	defer mr.mutex.Unlock()

	expiryTime := time.Now().Add(expiry)
	item := KeyValue{
		Key:        key,
		Value:      value,
		ExpiryTime: expiryTime,
	}

	mr.data[key] = item
	mr.expires[key] = expiryTime

	// Start a goroutine to delete the key after expiry
	if expiry > 0 {
		go mr.deleteKeyAfterExpiry(key, expiryTime)
	}
}

func (mr *MiniRedis) Delete(key string) bool {
	mr.mutex.Lock()
	defer mr.mutex.Unlock()

	_, ok := mr.data[key]
	if !ok {
		return false
	}

	delete(mr.data, key)
	delete(mr.expires, key)
	return true
}

func (mr *MiniRedis) ZRank(key string, member string) (int, bool) {
	mr.mutex.Lock()
	defer mr.mutex.Unlock()

	item, ok := mr.data[key]
	if !ok {
		return 0, false
	}

	value, ok := item.Value.(map[string]int)
	if !ok {
		return 0, false
	}

	rank, ok := value[member]
	if !ok {
		return 0, false
	}

	return rank, true
}

//  this might create an overhead of creating so many goroutines, So will look into it.
func (mr *MiniRedis) deleteKeyAfterExpiry(key string, expiryTime time.Time) {
	time.AfterFunc(time.Until(expiryTime), func() {
		mr.mutex.Lock()
		defer mr.mutex.Unlock()

		// Double-check if the key has already been deleted
		if expiryTime.Equal(mr.expires[key]) {
			delete(mr.data, key)
			delete(mr.expires, key)
			fmt.Printf("Key %s expired and deleted\n", key)
		}
	})
}

//  take care of this in future
func (mr *MiniRedis) SaveDataToFile(filepath string) error {
	mr.mutex.Lock()
	defer mr.mutex.Unlock()

	// before closing the connection or the server is down, write to the disk for making it persistent.

	return nil
}

//  take care of this in future
func (mr *MiniRedis) LoadDataFromFile(filepath string) error {
	mr.mutex.Lock()
	defer mr.mutex.Unlock()

	// have to handle the code here to load the data from disk.
	//  SO, I can run goroutine back to delete the key as soon as it is expired.

	return nil
}


// Process All commands for MINI-REDIS Here
func processMiniRedisClientRequest(mr *MiniRedis, request string) string {
	parts := strings.Fields(request)
	if len(parts) < 2 {
		return "Invalid command"
	}

	command := strings.ToUpper(parts[0])
	key := parts[1]

	switch command {
	case "GET":
		value, ok := mr.Get(key)
		if !ok {
			return "Key not found"
		}
		return fmt.Sprintf("%s", value)

	case "SET":
		if len(parts) < 3 {
			//  will take care of multiple flags as well, for now just return it as an invalid command
			return "Invalid command"
		}
		value := strings.Join(parts[2:], " ")
		mr.Set(key, value, 0)
		return "OK"

	case "DELETE":
		if mr.Delete(key) {
			return "OK"
		}
		return "Key not found"

	case "ZRANK":
		if len(parts) < 3 {
			return "Invalid command"
		}
		member := parts[2]
		rank, ok := mr.ZRank(key, member)
		if !ok {
			return "Member not found"
		}
		return strconv.Itoa(rank)

	default:
		return "Unknown command"
	}
}

func miniRedisClientConnection(mr *MiniRedis, conn net.Conn) {
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
		response := processMiniRedisClientRequest(mr, request)

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
	
	// Create a new MiniRedis instance
	mr := NewMiniRedis()

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

		go miniRedisClientConnection(mr, conn)
	}
}
