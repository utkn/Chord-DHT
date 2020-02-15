package main

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
)

var mainMenu = `
1) Enter the filename to store
2) Enter the filename to retrieve
3) Exit
`

var hasher = fnv.New32a()
var ringCapacity uint32 = 127

// Returns the id of a node (given its full address) or key of a file (given its name).
func hsh(in string) int {
	hasher.Write([]byte(in))
	digest := hasher.Sum32()
	hasher.Reset()
	return int(digest % ringCapacity)
}

// Connects to the peer at the given address.
func connectToPeer(address string) (net.Conn, *bufio.Reader) {
	address = strings.TrimSpace(address)
	conn, err := net.Dial("tcp", address)
	if err != nil {
		log.Println("Could not connect to the peer.")
		log.Fatalln(err)
	}
	// Create a buffered reader.
	reader := bufio.NewReader(conn)
	return conn, reader
}

func extractServerResponse(resp string) (string, string) {
	resp = strings.TrimSpace(resp)
	var prefix string
	var msg string
	if strings.HasPrefix(resp, "OK") {
		prefix = "OK"
		if len(resp) > 2 {
			msg = resp[3:]
		}
	} else if strings.HasPrefix(resp, "ERR") {
		prefix = "ERR"
		if len(resp) > 3 {
			msg = resp[4:]
		}
	}
	return prefix, msg
}

// Constructs a store request with the file name to store, then sends the file.
// STORE <file name> <file size>, followed by the file itself.
func storeFile(fileName string, peerAddr string) {
	// Find the successor (owner) of the file.
	fileKey := hsh(fileName)
	succAddr := askForSuccesor(fileKey, peerAddr)
	// Begin trying to store the file on the successor.
	conn, reader := connectToPeer(succAddr)
	defer conn.Close()
	srcFile, err := os.Open(fileName)
	defer srcFile.Close()
	if err != nil {
		log.Println("Could not send store request")
		log.Fatalln(err)
	}
	fileInfo, _ := srcFile.Stat()
	fileSize := fileInfo.Size()
	// Send the store request.
	storeRequest := fmt.Sprintf("STORE %s %d\n", fileName, fileSize)
	conn.Write([]byte(storeRequest))
	// Read the response.
	serverResponse, _ := reader.ReadString('\n')
	respType, respMsg := extractServerResponse(serverResponse)
	// Response: ERR <error msg>
	if respType != "OK" {
		fmt.Println("> Server response:", respMsg)
		return
	}
	// Response: OK
	io.Copy(conn, srcFile)
	// Read the next response.
	serverResponse, _ = reader.ReadString('\n')
	respType, respMsg = extractServerResponse(serverResponse)
	// Response: ERR <error msg>
	if respType != "OK" {
		fmt.Println("> Server response:", respMsg)
		return
	}
	// Response: OK
	fmt.Println("File successfully stored.")
}

func retrieveFile(fileName string, peerAddr string) {
	// Find the successor (owner) of the file.
	fileKey := hsh(fileName)
	succAddr := askForSuccesor(fileKey, peerAddr)
	// Begin trying to retrieve the file.
	conn, reader := connectToPeer(succAddr)
	defer conn.Close()
	// Construct the request.
	retrieveRequest := fmt.Sprintf("RETRIEVE %s\n", fileName)
	// Send the retrieve request.
	conn.Write([]byte(retrieveRequest))
	// Retrieve the size of the file from the connection.
	serverResponse, _ := reader.ReadString('\n')
	respType, respMsg := extractServerResponse(serverResponse)
	// Response: ERR <error msg>
	if respType != "OK" {
		fmt.Println("> Server response:", respMsg)
		return
	}
	// Response: OK <file size>
	fileSize, _ := strconv.Atoi(strings.TrimSpace(respMsg))
	// Create the local file.
	dstFile, _ := os.Create(fileName)
	defer dstFile.Close()
	// Retrieve the file from the connection.
	io.CopyN(dstFile, reader, int64(fileSize))
	// Read the next response.
	serverResponse, _ = reader.ReadString('\n')
	respType, respMsg = extractServerResponse(serverResponse)
	// Response: ERR <error msg>
	if respType != "OK" {
		fmt.Println("> Server response:", respMsg)
		return
	}
	// Response: OK
	fmt.Println("File retrieved successfully.")
}

// Constructs a successor request with the given id and sends it to the given address.
// Returns the answer to the request (i.e. the address of the successor).
// SUCC <id> => <succ addr>
func askForSuccesor(id int, peerAddr string) string {
	// Initiate a connection with the given peer address.
	conn, reader := connectToPeer(peerAddr)
	defer conn.Close()
	// Send the successor request.
	succRequest := fmt.Sprintf("SUCC %d\n", id)
	conn.Write([]byte(succRequest))
	// Wait for an answer.
	answer, err := reader.ReadString('\n')
	if err != nil {
		log.Println("Could not get the successor.")
		log.Fatalln(err)
	}
	// The answer will only contain the address of the successor.
	return answer
}

func main() {
	storeIP := os.Args[1]
	storePort := os.Args[2]
	storeAddr := storeIP + ":" + storePort
	// Show the main menu.
	fmt.Println(mainMenu)
	for {
		// Ask the user for a selection.
		fmt.Print("> Please select an option: ")
		var input string
		fmt.Scanln(&input)
		selectedOption, err := strconv.Atoi(input)
		if err != nil {
			fmt.Println("Invalid choice.")
			continue
		}
		// Act accordingly.
		switch selectedOption {
		case 1:
			// Ask the filename to hash.
			fmt.Print("> Enter the file name to store: ")
			var fileName string
			fmt.Scanln(&fileName)
			storeFile(fileName, storeAddr)
		case 2:
			// Ask the filename to hash.
			fmt.Print("> Enter the file name to retrieve: ")
			var fileName string
			fmt.Scanln(&fileName)
			retrieveFile(fileName, storeAddr)
		case 3:
			fmt.Println("Goodbye!")
			return
		}
	}
}
