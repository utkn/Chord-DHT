package main

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

type node struct {
	Address string
	ID      int
}

func newNode() node {
	return node{
		Address: "",
		ID:      -1,
	}
}

var mainMenu = `
1) Enter the peer address to connect
2) Enter the key to find its successor
3) Enter the filename to take its hash
4) Display pred-id, my-id, and succ-id
5) Display the stored filenames and their keys
6) Display my address
7) Exit`

var hasher = fnv.New32a()
var ringCapacity uint32 = 127

// Information about self.
var self = newNode()

// CW neighbor.
var successor = newNode()

// CCW neighbor.
var predecessor = newNode()

// The map of stored files' names to their keys.
var storedFiles = make(map[string]int)
var storedFilesMutex sync.Mutex

// Returns the full file path of the given file on the peer.
func filePath(fileName string) string {
	folder := fmt.Sprintf("%d", self.ID)
	os.Mkdir(folder, 0777)
	return filepath.Join(folder, fileName)
}

// Checks whether low < n < high on the ring.
func between(low int, n int, high int) bool {
	if low == high {
		return true
	}
	perimeter := int(ringCapacity)
	if high < low {
		high += perimeter
		if n < low {
			n += perimeter
		}
	}
	return (n > low && n < high)
}

// Returns the id of a node (given its full address) or key of a file (given its name).
func hsh(in string) int {
	hasher.Write([]byte(in))
	digest := hasher.Sum32()
	hasher.Reset()
	return int(digest % ringCapacity)
}

// "<prefix> <msg>\n" => "<prefix>", "<msg>"
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

// Runs the server at the given port, assigns its own ID and address, and
// starts listening to connections.
func serverRunner(port string) {
	ls, err := net.Listen("tcp", "localhost:"+port)
	if err != nil {
		log.Println("Could not start the server.")
		log.Fatalln(err)
	}
	// Acquire self address and id.
	self.Address = ls.Addr().String()
	self.ID = hsh(self.Address)
	for {
		// Wait for a connection.
		conn, err := ls.Accept()
		if err != nil {
			log.Println("Could not accept the connection.")
			log.Println(err)
			continue
		}
		// Once received, handle the request in the background.
		go handleRequest(conn)
	}
}

// Multiplexer for the requests from the clients
func handleRequest(conn net.Conn) {
	reader := bufio.NewReader(conn)
	request, _ := reader.ReadString('\n')
	request = strings.TrimSpace(request)
	log.Println("received", request)
	if strings.HasPrefix(request, "JOIN") {
		handleJoinRequest(conn, reader, request)
	} else if strings.HasPrefix(request, "SUCC") {
		handleSuccessorRequest(conn, reader, request)
	} else if strings.HasPrefix(request, "UPDATE") {
		handleUpdateRequest(conn, reader, request)
	} else if strings.HasPrefix(request, "STORE") {
		handleStoreRequest(conn, reader, request)
	} else if strings.HasPrefix(request, "RETRIEVE") {
		handleRetrieveRequest(conn, reader, request)
	}
}

// Handles a `RETRIEVE` request (RETRIEVE <file name>)
// Sends back the size of the file, then directly uploads the file through the connection.
func handleRetrieveRequest(conn net.Conn, reader *bufio.Reader, request string) {
	tokens := strings.Split(request, " ")
	fileName := tokens[1]
	_, ok := storedFiles[fileName]
	// Could not find the file.
	if !ok {
		conn.Write([]byte("ERR File does not exist.\n"))
		return
	}
	// Open the file.
	srcFile, err := os.Open(filePath(fileName))
	if err != nil {
		log.Println(err)
		conn.Write([]byte("ERR File does not exist.\n"))
		return
	}
	fileInfo, _ := srcFile.Stat()
	// Send back the size of the file.
	conn.Write([]byte(fmt.Sprintf("OK %d\n", fileInfo.Size())))
	// Send back the file itself.
	_, err = io.Copy(conn, srcFile)
	if err != nil {
		log.Println(err)
		conn.Write([]byte("ERR Could not copy the file.\n"))
		return
	}
	conn.Write([]byte("OK\n"))
}

// Handles a `STORE` request (STORE <file name> <file size>)
// Downloads the file from the client and saves it into local storage.
func handleStoreRequest(conn net.Conn, reader *bufio.Reader, request string) {
	tokens := strings.Split(request, " ")
	// Acquire the file name & size.
	fileName := tokens[1]
	fileSize, _ := strconv.Atoi(tokens[2])
	// Create the file on the system.
	dstFile, err := os.Create(filePath(fileName))
	defer dstFile.Close()
	if err != nil {
		log.Println(err)
		conn.Write([]byte("ERR Could not store file.\n"))
		return
	}
	conn.Write([]byte("OK\n"))
	// Get the file from the connection.
	_, err = io.CopyN(dstFile, reader, int64(fileSize))
	if err != nil {
		log.Println(err)
		conn.Write([]byte("ERR Could not copy file.\n"))
		return
	}
	fileKey := hsh(fileName)
	storedFiles[fileName] = fileKey
	conn.Write([]byte("OK\n"))
}

// Handles an UPDATE request by updating its successor & predecessor according to
// the request. Does not reply back.
// UPDATE <new succ addr> <new pred addr>
func handleUpdateRequest(conn net.Conn, reader *bufio.Reader, request string) {
	tokens := strings.Split(request, " ")
	// Get the new successor and predecessor addresses of this node.
	newSuccAddr := tokens[1]
	newPredAddr := tokens[2]
	if newSuccAddr != "KEEP" {
		// If the node claims that my new successor is myself, I am the only node left
		// in the ring.
		if newSuccAddr == self.Address {
			successor = newNode()
			predecessor = newNode()
		} else {
			successor.Address = newSuccAddr
			successor.ID = hsh(successor.Address)
		}
	}
	if newPredAddr != "KEEP" {
		// If the node claims that my new predecessor is myself, I am the only node left
		// in the ring.
		if newPredAddr == self.Address {
			successor = newNode()
			predecessor = newNode()
		} else {
			predecessor.Address = newPredAddr
			predecessor.ID = hsh(predecessor.Address)
		}
	}
}

// Handles and replies back to a JOIN request. The node that receives this request acts as
// an initiator.
// JOIN <new node addr> => <succ addr> <predec addr>
func handleJoinRequest(conn net.Conn, reader *bufio.Reader, request string) {
	tokens := strings.Split(request, " ")
	// Get the address & id of the new node.
	newNodeAddr := tokens[1]
	newNodeID := hsh(newNodeAddr)
	// If a node is trying to initiate itself, there is a problem. For now,
	// close the connection and report the problem.
	if self.ID == newNodeID {
		log.Println("Self-initiation is not allowed.")
		conn.Close()
		return
	}
	// If this is the only node in the system, join through this node.
	if successor.ID == -1 && predecessor.ID == -1 {
		// Move the files.
		moveFilesToNewNode(newNodeAddr, newNodeID)
		// Send itself as the successor & predecessor of the new node
		conn.Write([]byte(self.Address + " " + self.Address + "\n"))
		successor.Address = newNodeAddr
		successor.ID = newNodeID
		predecessor.Address = newNodeAddr
		predecessor.ID = newNodeID
		return
	}
	// Find the successor for the new node.
	newNodeSuccessorAddr := findSuccessor(newNodeID)
	// If this is the successor of the new node, join through this node.
	if newNodeSuccessorAddr == self.Address {
		// The new node's successor is this node and the new node's predecessor
		// is this node's old predecessor.
		conn.Write([]byte(self.Address + " " + predecessor.Address + "\n"))
		// Tell this node's predecessor to update its successor.
		sendUpdateRequest(newNodeAddr, "KEEP", predecessor.Address)
		// Move the files.
		moveFilesToNewNode(newNodeAddr, newNodeID)
		// Update this node's predecessor.
		predecessor.Address = newNodeAddr
		predecessor.ID = newNodeID
		return
	}
	// If this is not the successor of the new node, route the join request to
	// the new node's successor.
	newNodeSucc, newNodePred := sendJoinRequest(newNodeAddr, newNodeSuccessorAddr)
	// Route the answer back to the new node.
	conn.Write([]byte(newNodeSucc + " " + newNodePred + "\n"))
}

// Handles and replies back to a SUCC request.
// SUCC <id> => <succ addr>
func handleSuccessorRequest(conn net.Conn, reader *bufio.Reader, request string) {
	tokens := strings.Split(request, " ")
	// Get the requested id.
	id, err := strconv.Atoi(tokens[1])
	if err != nil {
		log.Println("Could not handle successor request")
		log.Fatalln(err)
	}
	// Find the successor.
	answer := findSuccessor(id)
	// Send back the successor.
	conn.Write([]byte(answer + "\n"))
}

// Checks through the files that are owned by this node and for the files
// that should be moved to the new node, moves them.
func moveFilesToNewNode(newNodeAddr string, newNodeID int) {
	// Acquire the list of files that need to be transferred to the new node.
	toTransfer := []string{}
	for fileName, fileKey := range storedFiles {
		if between(newNodeID, fileKey, self.ID) {
			continue
		}
		toTransfer = append(toTransfer, fileName)
	}
	for _, fileName := range toTransfer {
		// Store the file in the new peer.
		storeFile(fileName, newNodeAddr)
		// Remove the file from this peer.
		os.Remove(filePath(fileName))
		delete(storedFiles, fileName)
	}
}

// Stores the given file to the given peer.
func storeFile(fileName string, peerAddr string) {
	conn, reader := connectToPeer(peerAddr)
	defer conn.Close()
	srcFile, err := os.Open(filePath(fileName))
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
		fmt.Println(respType, respMsg)
		return
	}
	// Response: OK
	io.Copy(conn, srcFile)
	// No error checking for now...
}

// Constructs an update request with the given new successor and new predecessor addresses
// for the target peer. Set to `KEEP` if no change should be made to either of them.
// UPDATE <new succ addr> <new pred addr>
func sendUpdateRequest(newSuccAddr string, newPredAddr string, peerAddr string) {
	// Initiate a connection with the given peer address.
	conn, _ := connectToPeer(peerAddr)
	defer conn.Close()
	// Send the successor request.
	succRequest := fmt.Sprintf("UPDATE %s %s\n", newSuccAddr, newPredAddr)
	conn.Write([]byte(succRequest))
}

// Constructs a successor request with the given id and sends it to the given address.
// Returns the answer to the request (i.e. the address of the successor).
// SUCC <id> => <succ addr>
func sendSuccessorRequest(id int, peerAddr string) string {
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

// Constructs a join request with the new peer's id and sends it to the given initiator address.
// Returns the answer to the request (i.e. the successor & predecessor address of the new peer).
// JOIN <newNodeAddress> => <succ addr> <predec addr>
func sendJoinRequest(newNodeAddress string, address string) (string, string) {
	// Initiate a connection with the given initiator.
	conn, reader := connectToPeer(address)
	defer conn.Close()
	// Send the join request.
	conn.Write([]byte("JOIN " + newNodeAddress + "\n"))
	// Wait for an answer.
	answer, err := reader.ReadString('\n')
	if err != nil {
		log.Println("Could not get the join answer.")
		log.Fatalln(err)
	}
	// Return the successor and predecessor.
	tokens := strings.Split(strings.TrimSpace(answer), " ")
	return tokens[0], tokens[1]
}

// Returns the address of the successor of the given id (node or file).
func findSuccessor(id int) string {
	// If I am the only node in the ring, I am the successor of every id.
	if predecessor.ID == -1 && successor.ID == -1 {
		return self.Address
	}
	// If the id is between predecessor's id and this node's id, this node is the successor.
	if between(predecessor.ID, id, self.ID) || id == self.ID {
		return self.Address
	}
	// If the id is between this node's id and successor's id, my successor is the successor.
	if between(self.ID, id, successor.ID) || id == successor.ID {
		return successor.Address
	}
	// Otherwise, ask to this node's successor.
	return sendSuccessorRequest(id, successor.Address)
}

// Joins a ring from the given initiator address.
func joinRing(initiatorAddress string) {
	// Send a join request to the initiator.
	successorAddr, predecessorAddr := sendJoinRequest(self.Address, initiatorAddress)
	// Set the successor & predecessor.
	successor.Address = successorAddr
	successor.ID = hsh(successorAddr)
	predecessor.Address = predecessorAddr
	predecessor.ID = hsh(predecessorAddr)
}

func leaveRing() {
	// You can't leave a ring if there's no ring!
	if successor.ID == -1 || predecessor.ID == -1 {
		return
	}
	// Update this node's successor's predecessor.
	sendUpdateRequest("KEEP", predecessor.Address, successor.Address)
	// Update this node's predecessor's successor.
	sendUpdateRequest(successor.Address, "KEEP", predecessor.Address)
	// Transfer the files to the successor.
	for fileName := range storedFiles {
		storeFile(fileName, successor.Address)
	}
	// Remove the peer directory.
	os.RemoveAll(fmt.Sprintf("%d", self.ID))
	successor = newNode()
	predecessor = newNode()
}

func main() {
	peerPort := os.Args[1]
	// Start the server on the background.
	go serverRunner(peerPort)
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
			// Ask the initiator address.
			fmt.Print("> Enter the initiator address: ")
			var initiatorAddr string
			fmt.Scanln(&initiatorAddr)
			leaveRing()
			joinRing(initiatorAddr)
			fmt.Println("Connected to the ring!")
		case 2:
			// Ask the key.
			fmt.Print("> Enter the key to find its successor: ")
			var keyString string
			fmt.Scanln(&keyString)
			key, err := strconv.Atoi(keyString)
			if err != nil {
				fmt.Println("Invalid key!")
				continue
			}
			address := findSuccessor(key)
			fmt.Println("Address of the successor: ", address)
		case 3:
			// Ask the filename to hash.
			fmt.Print("> Enter the file name: ")
			var fileName string
			fmt.Scanln(&fileName)
			// Output the result.
			fmt.Println(fileName, "=>", hsh(fileName))
		case 4:
			// Output the neighbor and self ids.
			fmt.Printf("(%d, %d, %d)\n", predecessor.ID, self.ID, successor.ID)
		case 5:
			if len(storedFiles) < 1 {
				fmt.Println("No files are stored!")
			}
			// Iterate through the storedFiles map and show each key, value pair.
			for fileName, key := range storedFiles {
				fmt.Println(fileName, "=>", key)
			}
		case 6:
			fmt.Println(self.Address)
		case 7:
			leaveRing()
			fmt.Println("Left the ring.")
			fmt.Println("Goodbye!")
			return
		}
	}
}
