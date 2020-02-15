package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type Session struct {
	SessionID int
	UserName  string
}

func sendResponse(conn net.Conn, respType string, arg string) {
	conn.Write([]byte(respType + " " + arg + "\n"))
}

func createUserFile(conn net.Conn, clientReader *bufio.Reader, session Session, fileName string) (*os.File, error) {
	_, err := os.Stat(session.UserName)
	if os.IsNotExist(err) {
		os.MkdirAll(session.UserName, 0777)
		if err != nil {
			fmt.Println("* Created user directory for", session.UserName)
		}
	}
	fullFilePath := filepath.Join(session.UserName, fileName)
	_, err = os.Stat(fullFilePath)
	if !os.IsNotExist(err) {
		overwrite, _ := askInput(conn, clientReader,
			"File "+fileName+" already exists. Overwrite? (Y/N)")
		if strings.ToLower(overwrite) != "y" {
			return nil, errors.New("canceled by the user")
		}
	}
	f, err := os.Create(fullFilePath)
	if err != nil {
		return nil, err
	}
	fmt.Println("* Created user file ", fullFilePath)
	return f, nil
}

func getUserFile(conn net.Conn, session Session, fileName string) (*os.File, error) {
	fullFilePath := filepath.Join(session.UserName, fileName)
	f, err := os.Open(fullFilePath)
	return f, err
}

func askInput(conn net.Conn, clientReader *bufio.Reader, msg string) (string, error) {
	sendResponse(conn, "PROMPT", msg)
	input, err := clientReader.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(input), nil
}

func handleLogin(conn net.Conn, clientReader *bufio.Reader, session *Session) {
	input, err := askInput(conn, clientReader, "Enter username")
	if err != nil {
		log.Println(err)
		return
	}
	session.UserName = input
	fmt.Printf("* User changed for client %d to %s\n", session.SessionID, session.UserName)
	sendResponse(conn, "MSG", "Success.")
	sendResponse(conn, "MENU", session.UserName)
}

func handleStore(conn net.Conn, clientReader *bufio.Reader, session Session) {
	fileName, _ := askInput(conn, clientReader, "Enter the file name to store")
	dstFile, err := createUserFile(conn, clientReader, session, fileName)
	defer dstFile.Close()
	if err != nil {
		sendResponse(conn, "MSG", err.Error())
		return
	}
	sendResponse(conn, "STORE", fileName)
	// Retrieve the size information from the client.
	size, _ := clientReader.ReadString('\n')
	size = strings.TrimSpace(size)
	sizeBytes, _ := strconv.Atoi(size)
	// Retrieve the file from the client w.r.t. the size.
	_, err = io.CopyN(dstFile, clientReader, int64(sizeBytes))
	if err != nil {
		sendResponse(conn, "MSG", err.Error())
		return
	}
	fmt.Println("* Stored user file ", dstFile.Name())
	sendResponse(conn, "MSG", "File successfully stored.")
}

func handleRetrieve(conn net.Conn, clientReader *bufio.Reader, session Session) {
	fileName, _ := askInput(conn, clientReader, "Enter the file name to retrieve")
	srcFile, err := getUserFile(conn, session, fileName)
	defer srcFile.Close()
	if os.IsNotExist(err) {
		sendResponse(conn, "MSG", "File does not exist.")
		return
	}
	sendResponse(conn, "RETRIEVE", fileName)
	// Send the file size to the client.
	srcFileInfo, _ := srcFile.Stat()
	fileSize := fmt.Sprintf("%d\n", srcFileInfo.Size())
	conn.Write([]byte(fileSize))
	// Send the file to the client.
	_, err = io.Copy(conn, srcFile)
	if err != nil {
		sendResponse(conn, "MSG", err.Error())
		return
	}
	sendResponse(conn, "MSG", "File successfully retrieved.")
}

func handleSession(conn net.Conn, session Session) {
	clientReader := bufio.NewReader(conn)
	sendResponse(conn, "MENU", session.UserName)
	for {
		// Ask for choice.
		input, err := askInput(conn, clientReader, "Please choose an option")
		if err != nil {
			log.Println(err)
			return
		}
		var chosenOption int
		fmt.Sscanf(input, "%d", &chosenOption)

		switch chosenOption {
		case 1:
			handleLogin(conn, clientReader, &session)
		case 2:
			handleStore(conn, clientReader, session)
		case 3:
			handleRetrieve(conn, clientReader, session)
		case 4:
			sendResponse(conn, "CLOSE", "")
			break
		}
	}
}

func main() {
	// Launch the server.
	port := "8080"
	fmt.Printf("Launching the server at the port %s...\n", port)
	lst, err := net.Listen("tcp", "localhost:"+port)
	if err != nil {
		log.Fatalf("Could not create the server: %s", err)
	}

	lastSessionID := 0
	fmt.Println("Listening for connections.")
	for {
		// Accept a connection.
		conn, err := lst.Accept()
		if err != nil {
			log.Printf("* Could not accept the connection: %s\n", err)
			continue
		}
		// Construct the session.
		session := Session{SessionID: lastSessionID, UserName: "Guest"}
		lastSessionID++
		fmt.Printf("* Client connected with a session id of %d\n", session.SessionID)
		// Handle the session.
		go handleSession(conn, session)
	}
}
