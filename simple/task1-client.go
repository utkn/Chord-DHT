package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

var mainMenuMsg string = `
Welcome, %s.
1) Login
2) Store file
3) Retrieve file
4) Exit
`
var serverReader *bufio.Reader
var stdReader *bufio.Reader

// Extracts the argument from a server response.
func extractArg(serverResponse string) string {
	i := strings.IndexByte(serverResponse, ' ')
	return strings.TrimSpace(serverResponse[i+1:])
}

// Prints the main menu with the given user name.
// Shows the main menu with the given username from the server.
func handleMainMenu(conn net.Conn, userName string) {
	fmt.Printf(mainMenuMsg, userName)
}

// Handles a `PROMPT` response from the server.
// Shows a prompt to the user with the given message.
func handlePrompt(conn net.Conn, promptMsg string) {
	fmt.Printf("> " + promptMsg + ": ")
	clientAnswer, _ := stdReader.ReadString('\n')
	conn.Write([]byte(clientAnswer))
}

// Handles a `MSG` response from the server.
// Shows user the given message from the server.
func handleMessage(conn net.Conn, msg string) {
	fmt.Println("> Server response:", msg)
}

// Handles a `STORE` response from the server.
// Stores a file in the server.
func handleStore(conn net.Conn, fileName string) {
	srcFile, err := os.Open(fileName)
	defer srcFile.Close()
	if os.IsNotExist(err) {
		log.Fatalln(err)
	}
	// Send the size information to the server.
	srcFileInfo, _ := srcFile.Stat()
	fileSize := fmt.Sprintf("%d\n", srcFileInfo.Size())
	conn.Write([]byte(fileSize))
	// Send the file to the server.
	_, err = io.Copy(conn, srcFile)
	if err != nil {
		log.Fatalln(err)
	}
}

// Handles a `RETRIEVE` response from the server.
// Retrieves a file from the server.
func handleRetrieve(conn net.Conn, fileName string) {
	dstFile, _ := os.Create(fileName)
	defer dstFile.Close()
	// Retrieve the size information from the server.
	size, _ := serverReader.ReadString('\n')
	size = strings.TrimSpace(size)
	sizeBytes, _ := strconv.Atoi(size)
	// Retrieve the file from the client w.r.t. the size.
	io.CopyN(dstFile, serverReader, int64(sizeBytes))
}

func main() {
	// Acquire the server information.
	serverIP := os.Args[1]
	serverPort := os.Args[2]
	// Connect to the server.
	fmt.Print("Connecting... ")
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%s", serverIP, serverPort))
	if err != nil {
		log.Fatalf("Could not connect to the server: %s", err)
	}
	fmt.Println("Done.")
	// Create readers.
	serverReader = bufio.NewReader(conn)
	stdReader = bufio.NewReader(os.Stdin)
	// Main program loop.
	for {
		// Read a single response from the server.
		serverResponse, err := serverReader.ReadString('\n')
		if err != nil {
			log.Fatalf("Could not read the server response: %s", err)
		}
		// A server response has the following structure:
		// <COMMAND> <ARGUMENT>
		// According to its command, handle the response.
		if strings.HasPrefix(serverResponse, "MENU") {
			handleMainMenu(conn, extractArg(serverResponse))
		} else if strings.HasPrefix(serverResponse, "PROMPT") {
			handlePrompt(conn, extractArg(serverResponse))
		} else if strings.HasPrefix(serverResponse, "MSG") {
			handleMessage(conn, extractArg(serverResponse))
		} else if strings.HasPrefix(serverResponse, "STORE") {
			// Keep track of the time as we transfer a file.
			start := time.Now()
			handleStore(conn, extractArg(serverResponse))
			elapsed := time.Since(start)
			fmt.Println("Transfer took", elapsed.Microseconds(), "us")
		} else if strings.HasPrefix(serverResponse, "RETRIEVE") {
			// Keep track of the time as we transfer a file.
			start := time.Now()
			handleRetrieve(conn, extractArg(serverResponse))
			elapsed := time.Since(start)
			fmt.Println("Transfer took", elapsed.Microseconds(), "us")
		} else if strings.HasPrefix(serverResponse, "CLOSE") {
			fmt.Println("Goodbye!")
			conn.Close()
			return
		} else {
			fmt.Printf("Unrecognized server response: %s\n", serverResponse)
		}
	}
}
