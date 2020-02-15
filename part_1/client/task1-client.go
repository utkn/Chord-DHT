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

func extractArg(serverResponse string) string {
	i := strings.IndexByte(serverResponse, ' ')
	return strings.TrimSpace(serverResponse[i+1:])
}

func handleMainMenu(conn net.Conn, userName string) {
	fmt.Printf(mainMenuMsg, userName)
}

func handlePrompt(conn net.Conn, promptMsg string) {
	fmt.Printf("> " + promptMsg + ": ")
	clientAnswer, _ := stdReader.ReadString('\n')
	conn.Write([]byte(clientAnswer))
}

func handleMessage(conn net.Conn, msg string) {
	fmt.Println("> Server response:", msg)
}

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
	serverIP := os.Args[1]
	serverPort := os.Args[2]

	fmt.Print("Connecting... ")
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%s", serverIP, serverPort))
	if err != nil {
		log.Fatalf("Could not connect to the server: %s", err)
	}
	fmt.Println("Done.")

	serverReader = bufio.NewReader(conn)
	stdReader = bufio.NewReader(os.Stdin)

	for {
		serverResponse, err := serverReader.ReadString('\n')
		if err != nil {
			log.Fatalf("Could not read the server response: %s", err)
		}
		if strings.HasPrefix(serverResponse, "MENU") {
			handleMainMenu(conn, extractArg(serverResponse))
		} else if strings.HasPrefix(serverResponse, "PROMPT") {
			handlePrompt(conn, extractArg(serverResponse))
		} else if strings.HasPrefix(serverResponse, "MSG") {
			handleMessage(conn, extractArg(serverResponse))
		} else if strings.HasPrefix(serverResponse, "STORE") {
			handleStore(conn, extractArg(serverResponse))
		} else if strings.HasPrefix(serverResponse, "RETRIEVE") {
			handleRetrieve(conn, extractArg(serverResponse))
		} else if strings.HasPrefix(serverResponse, "CLOSE") {
			fmt.Println("Goodbye!")
			conn.Close()
			return
		} else {
			fmt.Printf("Unrecognized server response: %s\n", serverResponse)
		}
	}
}
