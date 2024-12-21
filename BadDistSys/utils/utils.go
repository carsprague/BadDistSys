package utils

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"baddistsys/bhash"
)

type Member struct {
	Id        string
	Hostname  string
	Timestamp time.Time
	Status    string
	IsSelf    bool
}

type Message struct {
	Type       string
	SourceId   string
	Data       interface{}
	GossipInfo []Gossip
}

type Gossip struct {
	Type      string
	Id        string
	Timestamp time.Time
	OldStatus string
	NewStatus string
}

type GossipBuffer struct {
	mu  sync.Mutex
	arr []Gossip
}

type PingData struct {
	Mu sync.Mutex
	Id string
}

type DropPacketFlag struct {
	MU   sync.Mutex
	Drop bool
}

type InitData struct {
	Members   []Member
	SusStatus bool
}

// append log struct
// contains unique node id and the data which was appended
type AppendLog struct {
	Id   string
	Data string
}

var ringnodes []string = []string{
	"fa24-cs425-1501",
	"fa24-cs425-1502",
	"fa24-cs425-1503",
	"fa24-cs425-1504",
	"fa24-cs425-1505",
	"fa24-cs425-1506",
	"fa24-cs425-1507",
	"fa24-cs425-1508",
	"fa24-cs425-1509",
	"fa24-cs425-1510",
}

var sourcenodes []string = []string{
	"fa24-cs425-1502",
	"fa24-cs425-1505",
	"fa24-cs425-1508",
}

var op1nodes []string = []string{
	"fa24-cs425-1503",
	"fa24-cs425-1506",
	"fa24-cs425-1509",
}

var op2nodes []string = []string{
	"fa24-cs425-1504",
	"fa24-cs425-1507",
	"fa24-cs425-1510",
}

var SUSPECT_TIMEOUT time.Duration = 5 * time.Second
var PING_TIMEOUT time.Duration = 3000 * time.Millisecond
var DELAY time.Duration = 200 * time.Millisecond
var JOIN_TIMEOUT time.Duration = 5 * time.Second
var LEAVE_DISSEMINATION time.Duration = 5 * time.Second
var MESSAGE_DROP_RATE float64 = -1.0

var MembershipList sync.Map
var gossipBuffer GossipBuffer
var SelfAddr string
var SelfId string
var InitiatorAddr string = "fa24-cs425-1501:9000"
var LastStateChange time.Time = time.Now()
var LastGossip time.Time = time.Now()
var SusStatus bool = false
var JOIN_ACK_CHANNEL chan Message = make(chan Message, 1)
var PING_ACK_CHANNEL chan Message = make(chan Message, 1)
var DropFlag DropPacketFlag
var LastPing PingData
var LogFile *os.File
var hash *bhash.ConsistentHash // each member has a local hash based on the membership list
var ring map[string]int
var StoredFiles []string // locally stored files
var appendLogs map[string][]AppendLog

var execPath string

// var execCmd string
var execArg string

var selfRole string

var rainFile string

var destRainFile string

var wg sync.WaitGroup

var fileData string

var fileDataLock sync.Mutex

var stopWriting bool

var inactivityTimer *time.Timer

var storage map[string]string

var storageLock sync.Mutex

var countsMap map[string]int

var countsLock sync.Mutex

var globalOp1 string

var globalOp2 string

var TCPinUse bool = false

var connections []net.Conn

var appendfileData string

// ------------------------------------------- RAINSTORM CODE --------------------------------------------------
// ------------------------------------------- LEADER NODE --------------------------------------------------

// we can assume only the leader will call this, and can then tell the nodes what to do (role)
func ExecuteRainstorm(op1 string, op2 string, hydfs_src string, hydfs_dest string, num_tasks string, arg string) {
	// num_tasks_int, err := strconv.Atoi(num_tasks)

	// if err != nil {
	// 	fmt.Printf("[ERROR] Num tasks was not passed as a valid integer, error message: \n", err)
	// }

	// need to partition the nodes, can just use the topology we discussed earlier
	// send a message to each node with their role and process to run, at which the node will run the process in a thread
	fmt.Printf("Special arg: %s\n", arg)
	destRainFile = hydfs_dest
	stopWriting = false
	fileData = ""
	storage = make(map[string]string)
	globalOp1 = op1
	globalOp2 = op2

	if op2 == "count" {
		countsMap = make(map[string]int)
	}

	encodedArg := url.QueryEscape(arg)

	for _, node := range sourcenodes {
		URL := fmt.Sprintf("http://%s/role?rolename=%s&exec=%s&arg=%s&filename=%s", node+":8080", "source", "maketuple", encodedArg, hydfs_src)
		resp, err := http.Get(URL)
		if err != nil {
			fmt.Printf("[RAIN] Source Node failed to initiate: %s\n", node)
			continue
		} else {
			fmt.Printf("[RAIN] Source Node initiated: %s\n", node)
		}
		resp.Body.Close()
	}

	for _, node := range op1nodes {
		URL := fmt.Sprintf("http://%s/role?rolename=%s&exec=%s&arg=%s&filename=%s", node+":8080", "op1", op1, encodedArg, hydfs_src)
		resp, err := http.Get(URL)
		if err != nil {
			fmt.Printf("[RAIN] OP1 Node failed to initiate: %s\n", node)
			continue
		} else {
			fmt.Printf("[RAIN] OP1 Node initiated: %s\n", node)
		}
		resp.Body.Close()
	}

	for _, node := range op2nodes {
		URL := fmt.Sprintf("http://%s/role?rolename=%s&exec=%s&arg=%s&filename=%s", node+":8080", "op2", op2, encodedArg, hydfs_src)
		resp, err := http.Get(URL)
		if err != nil {
			fmt.Printf("[RAIN] OP2 Node failed to initiate: %s\n", node)
			continue
		} else {
			fmt.Printf("[RAIN] OP2 Node initiated: %s\n", node)
		}
		resp.Body.Close()
	}

	if !TCPinUse {
		go RainListener()
		TCPinUse = true
	} else {
		closeAllConnections()
	}

	// need to send each source node their chunk of the file to process
	PartitionData(hydfs_src, num_tasks)

}

// this should just set up the node to do the role
func InitRole(role string, exe string, arg string, filename string) {
	fmt.Printf("Inside Init Role assining role %s\n", role)
	fmt.Printf("[RAIN] role: %s, exec: %s, arg: %s\n", role, exe, arg)

	// setup executable path to call
	execPath = "./" + exe + "/" + exe
	execArg = arg
	selfRole = role
	rainFile = filename
	storage = make(map[string]string)

	if selfRole == "source" {
		// spout
		fmt.Println("Calling RainSpout")
		RainSpout()
		if !TCPinUse {
			go RainListener()
			TCPinUse = true
		} else {
			closeAllConnections()
		}

	} else {
		fmt.Println("Calling Listener")
		if !TCPinUse {
			go RainListener()
			TCPinUse = true
		} else {
			closeAllConnections()
		}

	}

	go generateLogFile()
}

func RainSpout() {
	self := GetHostname()
	fmt.Printf("Inside RainSpout function, self is %s\n", self)
	var filename string
	switch self {
	case "fa24-cs425-1502":
		filename = "chunk_1.txt"
	case "fa24-cs425-1505":
		filename = "chunk_2.txt"
	case "fa24-cs425-1508":
		filename = "chunk_3.txt"
	}

	// open file ./hydfs/filename
	// Open the file for reading
	filePath := "./hydfs/" + filename
	for {
		_, err := os.Stat(filePath)
		if err == nil {
			// File exists, break the loop
			break
		}
		// Sleep for a short duration before checking again
		time.Sleep(100 * time.Millisecond)
	}
	fmt.Println("File exists, opening now")
	file, err := os.Open(filePath)
	if err != nil {
		fmt.Printf("Error opening file %s: %v\n", filePath, err)
		return
	}
	defer file.Close()
	// go line by line in file

	fmt.Println("Scanning chunk file stored at this node")
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		numAndLine := strings.SplitN(line, ",", 2)
		data := numAndLine[1]
		escaped := strings.ReplaceAll(data, `"`, `\"`)
		finalData := fmt.Sprintf(`"%s"`, escaped)
		//fmt.Printf("Final data: %s\n", finalData)
		tuple := rainFile + ":" + numAndLine[0] + "," + finalData
		// INSERT INTO STORAGE
		RainSender(tuple)
		storeInMap(tuple)
	}

	generateLogFile()

	fmt.Printf("STORAGE SIZE: %d\n", len(storage))

	// send line by line

}

func ExecuteRole(conn net.Conn) {
	defer wg.Done()
	defer conn.Close()

	senderAddr := conn.RemoteAddr().String()
	// Extract the IP part of the sender address
	ip, _, err := net.SplitHostPort(senderAddr)
	if err != nil {
		//fmt.Println("Error splitting remote address:", err)
		return
	}

	// Set the new port to 8000
	senderAddr = ip + ":8000"

	// Read the incoming stream of data (line by line)
	buffer := make([]byte, 1024)
	for {
		// Read data from the connection
		n, err := conn.Read(buffer)
		if err == io.EOF {
			// Connection closed by the client
			//fmt.Println("Connection closed by client")
			break
		}
		if err != nil {
			fmt.Println("Error reading data:", err)
			conn.Close()
			break
		}

		// Process the received data line by line
		receivedData := string(buffer[:n])

		// got an ACK
		if strings.Contains(receivedData, "DATA_PROCESSED") {
			// DATA_PROCESSED,key
			key := strings.SplitN(receivedData, ",", 2)
			key[1] = strings.TrimRight(key[1], "\x00\n\r")
			//fmt.Printf("REMOVING KEY: %s", key[1])
			deleteFromMap(key[1])
			fmt.Printf("STORAGE SIZE: %d\n", len(storage))
			// close the connection?
			conn.Close()
			//fmt.Printf("ACK RECEIVED\n")
			break
		}

		currentTime := time.Now().Format("2006-01-02 15:04:05")
		appendfileData += "[RECEIVED: " + currentTime + "]:" + receivedData + "\n" // Add data with a newline between entries

		// new data
		isDuplicate := storeInMap(receivedData)
		if !isDuplicate {
			//fmt.Printf("STORAGE SIZE: %d\n", len(storage))
		}

		if GetHostname() != "fa24-cs425-1501" {
			// Call the executable to process the data
			if !isDuplicate {
				processedData, err := ExecuteCode(receivedData)
				if err != nil {
					//fmt.Println("Error processing data:", err)
					break
				}

				// Send processed data to the next node (for example, send it to another server)
				err = RainSender(processedData)
				if err != nil {
					//fmt.Println("Error sending data to next node:", err)
					break
				}
			}
			resetWriteTimer()
		} else {
			if !isDuplicate {
				if globalOp2 == "count" {
					//fmt.Println("COUNT PROCESSING AT LEADER NODE")
					updateCount(receivedData)
					printCounts()
					updateFileDataCount()

				} else {
					fmt.Println(receivedData)
					fileDataLock.Lock()
					fileData += receivedData + "\n"
					fileDataLock.Unlock()
				}
				resetWriteTimer()
			}
		}
		newConn, err := net.Dial("tcp", senderAddr)
		if err != nil {
			// fmt.Println("Error creating new connection to sender:", err)
			break
		}
		defer newConn.Close()
		key := strings.SplitN(receivedData, ",", 2)
		// respond with ack to previous node
		_, err = newConn.Write([]byte("DATA_PROCESSED," + key[0] + "\n"))
		if err != nil {
			fmt.Println("Error sending ACK:", err)
			break
		}
		//fmt.Printf("ACK SENT\n")
	}
}

func updateCount(data string) {
	// fmt.Printf("Updating count for category %s\n", category)
	parts := strings.Split(data, ",")
	category := parts[1]
	countsLock.Lock()
	defer countsLock.Unlock()
	countsMap[category]++
}

func printCounts() {
	fmt.Println("Printing counts to the console")
	countsLock.Lock()
	defer countsLock.Unlock()

	for word, count := range countsMap {
		fmt.Printf("%s,%d\n", word, count)
	}

}

func updateFileDataCount() {
	fmt.Println("Putting count data into batch file to send to HyDFS")
	fileDataLock.Lock()
	defer fileDataLock.Unlock()
	fileData = ""

	for word, count := range countsMap {
		fileData += fmt.Sprintf("%s,%d\n", word, count)
	}
}

func storeInMap(data string) bool {
	ret := false
	storageLock.Lock()
	key := strings.SplitN(data, ",", 2)
	if _, exists := storage[key[0]]; exists {
		// Key exists, handle accordingly
		fmt.Printf("DISCARDING DUPLICATE: %s\n", key[0])
		ret = true
	} else {
		// Key doesn't exist, store the data
		storage[key[0]] = data
		fmt.Printf("STORING KEY: %s\n", key[0])
		ret = false
	}
	storageLock.Unlock()
	return ret
}

func deleteFromMap(key string) {
	storageLock.Lock()
	delete(storage, key)
	storageLock.Unlock()
	fmt.Printf("DELETING KEY: %s\n", key)
}

func periodicWrite() {
	for {
		// If we need to stop, break out of the loop
		if stopWriting {
			return
		}

		// Wait for 5 seconds before writing the file
		time.Sleep(4 * time.Second)

		// Lock fileData before writing to file
		fileDataLock.Lock()
		//currentTime := time.Now().Format("2006-01-02 15:04:05")
		//fmt.Printf("FILEDATA LENGTH: %d\n", len(fileData))
		err := os.WriteFile("./local/output.txt", []byte(fileData+"\n"), 0777)
		if err != nil {
			fmt.Println("Error writing to file:", err)
		}
		AddRingFile("output.txt", destRainFile)
		fileDataLock.Unlock()
	}
}

func generateLogFile() {
	for {
		// If we need to stop, break out of the loop
		if stopWriting {
			return
		}

		// Wait for 5 seconds before writing the file
		time.Sleep(4 * time.Second)

		for _, data := range storage {
			currentTime := time.Now().Format("2006-01-02 15:04:05")
			appendfileData += "[" + currentTime + "]:" + data + "\n" // Add data with a newline between entries
		}

		err := os.WriteFile("./local/log"+GetHostname()+".txt", []byte(appendfileData), 0777)
		if err != nil {
			fmt.Println("Error writing to file:", err)
		}
		AddRingFile("log"+GetHostname()+".txt", "log"+GetHostname()+".txt")
	}
}

func resetWriteTimer() {
	// Cancel any existing timer
	if inactivityTimer != nil {
		inactivityTimer.Stop()
	}

	// Start a new 5-second timer
	inactivityTimer = time.AfterFunc(10*time.Second, func() {
		// Stop the periodic write thread if no data is received within 5 seconds
		stopWriting = true
		//fmt.Println("No data received for 5 seconds. Stopping periodic write.")
	})
}

func ExecuteCode(data string) (string, error) {
	// return data, nil
	cmd := exec.Command(execPath, data, execArg)
	output, err := cmd.CombinedOutput()
	if err != nil {
		fmt.Printf("[RAIN] ERROR EXECUTING CODE %v\n", err)
	}

	// fmt.Printf("Command output: %s\n", string(output))

	// Return the output of the executable as the processed data
	return string(output), err
}

func RainListener() {
	// Start TCP listener on port 8000
	listen, err := net.Listen("tcp", ":8000")
	if err != nil {
		fmt.Println("Error starting TCP server:", err)
		os.Exit(1)
	}
	defer listen.Close()
	//fmt.Println("TCP server listening on port 8080...")

	if GetHostname() == "fa24-cs425-1501" {
		go periodicWrite()
	}

	// Continuously accept incoming connections
	for {
		conn, err := listen.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			continue
		}

		connections = append(connections, conn)

		// Handle each connection in a separate goroutine
		wg.Add(1)
		go ExecuteRole(conn)
	}

	// Wait for all goroutines to finish
	wg.Wait()
}

func closeAllConnections() {
	for _, conn := range connections {
		conn.Close()
	}
	connections = nil
}

func ResendSavedData() {
	storageLock.Lock()
	defer storageLock.Unlock()

	// Iterate over the storage map and send each entry line by line
	for _, data := range storage {
		var sendData string
		if selfRole != "source" {
			sendData, _ = ExecuteCode(data)
		} else {
			sendData = data
		}
		err := RainSender(sendData)
		if err != nil {
			fmt.Println("Error sending data:", err)
		} else {
			fmt.Printf("Successfully resent data\n")
		}
	}
}

func RainSender(data string) error {
	// For demonstration purposes, we'll send data to another server (localhost:8081)
	if data == "" {
		return nil // no data to send, just return
	}
	self := GetHostname()
	nextNodeAddr, _ := getNextNodeAddress(self)

	conn, err := net.Dial("tcp", nextNodeAddr)
	if err != nil {
		return fmt.Errorf("error connecting to next node: %v", err)
	}
	defer conn.Close()

	// Send the processed data to the next node
	_, err = conn.Write([]byte(data))
	if err != nil {
		return fmt.Errorf("error sending data to next node: %v", err)
	}

	currentTime := time.Now().Format("2006-01-02 15:04:05")
	appendfileData += "[SENT: " + currentTime + "]:" + data + "\n" // Add data with a newline between entries

	fmt.Printf("Sent processed data to next node: %s\n", data)
	return nil
}

func getNextNodeAddress(hostname string) (string, error) {
	// Split the hostname to extract the identifier
	parts := strings.Split(hostname, "-")
	if len(parts) < 2 {
		return "", fmt.Errorf("invalid hostname format: %s", hostname)
	}

	// Extract the numeric identifier (last part of the hostname)
	id := parts[len(parts)-1]
	nextID, err := incrementNodeID(id)
	if err != nil {
		return "", err
	}

	// Construct the next node hostname
	nextNode := strings.Join(append(parts[:len(parts)-1], nextID), "-")
	return fmt.Sprintf("%s:8000", nextNode), nil
}

// incrementNodeID increments the node ID while maintaining leading zeros
func incrementNodeID(id string) (string, error) {
	num, err := strconv.Atoi(id)
	if err != nil {
		return "", fmt.Errorf("invalid numeric ID: %s", id)
	}
	// send to leader
	if selfRole == "op2" {
		return "1501", nil
	}

	check := fmt.Sprintf("%04d", num+1)
	if isAliveNode(check) {
		return check, nil
	} else {
		for i := 1; i < 3; i++ {
			check = fmt.Sprintf("%04d", num+1+3*i)
			if isAliveNode(check) {
				return check, nil
			}
			check = fmt.Sprintf("%04d", num+1-3*i)
			if isAliveNode(check) {
				return check, nil
			}
		}
	}

	return fmt.Sprintf("%04d", num+1), nil
}

func isAliveNode(num string) bool {
	if _, exists := ring["fa24-cs425-"+num]; exists {
		return true
	}
	return false
}

func PartitionData(hydfsfilename string, num_tasks string) {
	tasks, err := strconv.Atoi(num_tasks)
	if err != nil {
		fmt.Printf("Could not convert between string and integer: %s\n", err)
	}

	// fetch file from HyDFS
	message := GetFileLocally(hydfsfilename, "input.txt")

	if message != "Success" {
		fmt.Printf("File could not be downloaded from HyDFS: %s", message)
	}

	filepath := "./local/input.txt"

	file, err := os.Open(filepath)

	if err != nil {
		fmt.Printf("Error opening file: %v\n", err)
		return
	}

	defer file.Close()

	// creating files for each chunk
	chunkFiles := make([]*os.File, tasks)

	for i := 0; i < tasks; i++ {
		chunkFilename := fmt.Sprintf("chunk_%d.txt", i+1)
		chunkFile, err := os.Create("./local/" + chunkFilename)
		if err != nil {
			fmt.Printf("could not create chunk file %s: %v", chunkFilename, err)
			return
		}
		chunkFiles[i] = chunkFile
		defer chunkFile.Close()
	}

	// Read lines from the input file and distribute them
	scanner := bufio.NewScanner(file)
	lineNumber := 1

	for scanner.Scan() {
		line := scanner.Text()
		chunkIndex := (lineNumber - 1) % tasks // Determine the chunk file in a round-robin way
		outputLine := fmt.Sprintf("%d,%s\n", lineNumber, line)

		_, err := chunkFiles[chunkIndex].WriteString(outputLine)
		if err != nil {
			fmt.Printf("could not write to chunk file: %v", err)
		}

		lineNumber++
	}

	// send each of these files to source nodes
	idx := 1

	for _, node := range sourcenodes {
		fmt.Printf("Inside for loop sending to node: %s\n", node)
		file := fmt.Sprintf("chunk_%d.txt", idx)
		message := SendFile(node+":8080", file, file)
		fmt.Printf("Return from SendFile: %s\n", message)
		idx++
	}

}

// ------------------------------------------- SOURCE NODE --------------------------------------------------
// this bad boy should be taking the data sent from the leader and creating tuples

// ------------------------------------------- OP_1 NODE --------------------------------------------------

// ------------------------------------------- OP_2 NODE --------------------------------------------------

// this iterates through the files stored on this node
// in the hydfs, and if the base copy is stored here,
// we replicate it on the next 2 nodes in the ring
func ReplicateNode() {
	self := GetHostname()

	files, err := os.ReadDir("./hydfs/")
	if err != nil {
		fmt.Printf("[HyDFS] HyDFS directory doesn't exist...\n")
		return
	}
	ringsize := len(ring)
	for _, file := range files {
		node := hash.Get(file.Name())
		node = strings.Split(node, ":")[0]
		fmt.Printf("file stored here: %s, where it's based: %s\n", file.Name(), node)
		if node == self {
			// send to next 2 nodes in the ring,
			// use modular arithmetic to circulate correctly
			id := ring[node]
			for key, val := range ring {
				if (val == (id+1)%ringsize || val == (id+2)%ringsize) && (strings.Split(key, ":")[0] != self) {
					SendReplica(key+":8080", file.Name())
					fmt.Printf("[HyDFS] replicating [%s] at node [%s]\n", file.Name(), key)
				}
			}

		}
	}
}

func ReplicateFile(hydfsfilename string) {
	// send to next 2 nodes in the ring,
	// use modular arithmetic to circulate correctly
	node := hash.Get(hydfsfilename) // this isn't hashing to correct value
	ringsize := len(ring)
	if ringsize == 0 {
		return
	}
	self := GetHostname()
	id := ring[node]
	//fmt.Printf("Node id of local file: [%d]\n", id)
	for key, val := range ring {
		if (val == (id+1)%ringsize || val == (id+2)%ringsize) && (strings.Split(key, ":")[0] != self) {
			key = strings.Split(key, ":")[0]
			//fmt.Printf("Value in ring: [%d]\n", val)
			SendReplica(key+":8080", hydfsfilename)
			//fmt.Printf("RETURN: %s\n", ret)
			fmt.Printf("[HyDFS] replicating [%s] at node [%s]\n", hydfsfilename, key)
		} else {
			//fmt.Printf("Not replicating HyDFS file [%s] at node [%s]\n", hydfsfilename, key)
		}
	}
}

// just send file but using the HyDFS files instead of local files
func SendReplica(dest string, hydfsfilename string) string {
	// Open the file to upload
	path := "./hydfs/" + hydfsfilename
	file, err := os.Open(path)
	if err != nil {
		fmt.Printf("[ERROR] Could not open file: %s. Error: %s\n", path, err)
		return "File not found replica" // TODO: running into issues here
	}
	defer file.Close()

	// create query string
	url := fmt.Sprintf("http://%s/upload?hydfsfilename=%s", dest, hydfsfilename)

	// Create a new POST request with the file as the body
	req, err := http.NewRequest("POST", url, file)
	if err != nil {
		return "Could not create POST request"
	}

	// Execute the request
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return "Could not make POST request"
	}
	defer resp.Body.Close()

	// Return the response status
	return resp.Status
}

func SendFile(dest string, filename string, hydfsfilename string) string {
	fmt.Printf("Inside send file. Sending filename [%s] to node [%s] with hydfsfilename [%s]\n", filename, dest, hydfsfilename)
	// Open the file to upload
	path := "./local/" + filename
	file, err := os.Open(path)
	if err != nil {
		return "File not found send"
	}
	defer file.Close()
	// fmt.Println("Died at opening local file")

	// create query string
	url := fmt.Sprintf("http://%s/upload?hydfsfilename=%s", dest, hydfsfilename)

	// Create a new POST request with the file as the body
	req, err := http.NewRequest("POST", url, file)
	if err != nil {
		return "Could not create POST request"
	}

	// fmt.Println("Died at creating new request")

	// Execute the request
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return "Could not make POST request"
	}
	defer resp.Body.Close()

	// Return the response status
	return resp.Status
}

func GetFileLocally(hydfsfilename string, filename string) string {
	// figure out where the file lives
	node := hash.Get(hydfsfilename)
	if node == "" {
		return ""
	}
	node = strings.Split(node, ":")[0] + ":8080"

	// create query string
	url := fmt.Sprintf("http://%s/download?hydfsfilename=%s", node, hydfsfilename)

	// Send GET request to the server
	resp, err := http.Get(url)
	if err != nil {
		return "Get request failed"
	}
	defer resp.Body.Close()

	// Check if the request was successful
	if resp.StatusCode != http.StatusOK {
		return "Download failed"
	}

	// Create a local file with the same name to save the downloaded content
	outFile, err := os.Create("./local/" + filename)
	if err != nil {
		return "Couldn't create file"
	}
	defer outFile.Close()

	// Copy the response content to the local file
	_, err = io.Copy(outFile, resp.Body)
	if err != nil {
		return "Couldn't save file"
	}

	fmt.Printf("[HyDFS] downloaded [%s] locally as [%s]\n", hydfsfilename, filename)
	return "Success"
}

func GetReplicaFileLocally(replica string, hydfsfilename string, filename string) string {
	node := replica + ":8080"

	// create query string
	url := fmt.Sprintf("http://%s/download?hydfsfilename=%s", node, hydfsfilename)

	// Send GET request to the server
	resp, err := http.Get(url)
	if err != nil {
		return "Get request failed"
	}
	defer resp.Body.Close()

	// Check if the request was successful
	if resp.StatusCode != http.StatusOK {
		return "Download failed"
	}

	// Create a local file with the same name to save the downloaded content
	outFile, err := os.Create("./local/" + filename)
	if err != nil {
		return "Couldn't create file"
	}
	defer outFile.Close()

	// Copy the response content to the local file
	_, err = io.Copy(outFile, resp.Body)
	if err != nil {
		return "Couldn't save file"
	}

	fmt.Printf("[HyDFS] downloaded [%s] locally as [%s]\n", hydfsfilename, filename)
	return "Success"
}

// function to append to an already existing hydfs file
func AppendToHyDFSFile(localfilename string, hydfsfilename string) string {
	// Determine the node where the file is stored
	node := hash.Get(hydfsfilename)
	node = strings.Split(node, ":")[0]
	if node == "" {
		fmt.Printf("[HyDFS] filename [%s] does not exist\n", hydfsfilename)
		return fmt.Sprintf("Error: File %s not found in HyDFS", hydfsfilename)
	}

	// Construct the lock file name
	lockFilename := "./local/" + hydfsfilename + ".lock"

	// Wait for the lock to be released
	for {
		if _, err := os.Stat(lockFilename); os.IsNotExist(err) {
			// Lock file does not exist, so we can create it and proceed
			lockFile, err := os.Create(lockFilename)
			if err != nil {
				return fmt.Sprintf("Error: Could not create lock file for %s", hydfsfilename)
			}
			lockFile.Close()
			break
		}
		// Lock file exists, wait and then check again
		time.Sleep(100 * time.Millisecond)
	}

	// At this point, we have acquired the "lock"
	defer os.Remove(lockFilename) // Ensure the lock file is removed when done

	// Proceed with the append operation
	tempFilename := "temp_" + hydfsfilename
	status := GetFileLocally(hydfsfilename, tempFilename)
	if status != "Success" {
		return fmt.Sprintf("Error: Failed to retrieve file %s from HyDFS", hydfsfilename)
	}

	// Open both the temporary local file (HyDFS file) and the local file to append
	tempFile, err := os.OpenFile("./local/"+tempFilename, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Printf("Error: Could not open temp file %s for appending\n", tempFilename)
		return "Could not open temp file"
	}
	defer tempFile.Close()

	localFile, err := os.Open("./local/" + localfilename)
	if err != nil {
		fmt.Printf("Error: Could not open local file %s\n", localfilename)
		return "Could not open local file"
	}
	defer localFile.Close()

	// Append the contents of the local file to the HyDFS file
	_, err = io.Copy(tempFile, localFile)
	if err != nil {
		return fmt.Sprintf("Error: Failed to append contents from %s to %s\n", localfilename, tempFilename)
	}

	// Send the updated file back to HyDFS
	appendStatus := SendFile(node+":8080", tempFilename, hydfsfilename)
	if appendStatus != "200 OK" {
		return fmt.Sprintf("Error: Failed to upload updated file %s to HyDFS\n", hydfsfilename)
	}

	// -------------------- Append logging and replication -----------------------------

	appendLogs = make(map[string][]AppendLog)
	// Create a log entry for this append operation
	appendLog := AppendLog{
		Id:   GetHostname(),
		Data: GetFileContents("./local/" + localfilename),
	}

	// log the append on this machine
	LogAppendOperation(hydfsfilename, appendLog)

	// replicate the append on the replica machines
	// if node is the hostname, then just send replication request
	// otherwise, make http to send a replication from another node
	self := GetHostname()
	//node = strings.Split(node, ":")[0]
	if node == self {
		ReplicateFile(hydfsfilename)
	} else {
		replicateURL := fmt.Sprintf("http://%s/replicate?hydfsfilename=%s", node+":8080", hydfsfilename)
		replicateResp, err := http.Get(replicateURL)
		if err != nil || replicateResp.StatusCode != http.StatusOK {
			fmt.Printf("[ERROR] Failed to trigger replication for [%s] at node [%s]\n", hydfsfilename, node)
			fmt.Printf("[ERROR MESSAGE] %s \n", err)
			fmt.Printf("[STATUS CODE] %d \n", replicateResp.StatusCode)
			return "Failed due to replication error"
		}
	}

	os.Remove("./local/" + tempFilename) // Clean up the temporary file

	return "[HyDFS] append successfully completed"
}

// function to get file contents
func GetFileContents(filename string) string {
	data, err := os.ReadFile(filename)
	if err != nil {
		fmt.Printf("Error reading file content: %s\n", err)
		return ""
	}
	return string(data)
}

// function to log an append operation for a given file
func LogAppendOperation(hydfsfilename string, log AppendLog) {
	// Initialize the log slice if it doesn't already exist
	if _, exists := appendLogs[hydfsfilename]; !exists {
		appendLogs[hydfsfilename] = []AppendLog{}
	}
	// Add the new log entry to the slice for this file
	appendLogs[hydfsfilename] = append(appendLogs[hydfsfilename], log)
}

func AppendClient(localfilename string, hydfsfilename string) string {
	nodes := GetReplicaNodes(hydfsfilename)
	if len(nodes) == 0 {
		fmt.Printf("[HyDFS] filename [%s] does not exist\n", hydfsfilename)
		return fmt.Sprintf("Error: File %s not found in HyDFS", hydfsfilename)
	}

	// Generate the append file name
	appendFilename := "append_" + GetHostname() + "_" + hydfsfilename

	// Send the file to all the replicas
	for _, node := range nodes {
		node = strings.Split(node, ":")[0]
		sendStatus := SendFile(node+":8080", localfilename, appendFilename)
		if sendStatus != "200 OK" {
			return fmt.Sprintf("Error: Failed to upload updated file %s to node %s\n", hydfsfilename, node)
		}

		// Send append request to each node
		url := fmt.Sprintf("http://%s/append?hydfsfilename=%s&appendfilename=%s", node+":8080", hydfsfilename, appendFilename)

		// Perform the append operation on the node
		result, err := http.Get(url)
		if err != nil {
			return fmt.Sprintf("[HyDFS] Append error at %s with: %s\n", node, err)
		}

		if result.StatusCode != http.StatusOK {
			return fmt.Sprintf("[HyDFS] Append failed at %s with status: %s\n", node, result.Status)
		}
	}

	// Successfully appended to all replicas
	return "[HyDFS] Append operation completed successfully to all replicas"
}

func AppendServer(appendname string, hydfsfilename string) string {
	hydfspath := "./hydfs/" + hydfsfilename
	hydfsfile, err := os.OpenFile(hydfspath, os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return "File not found hydfs"
	}
	defer hydfsfile.Close()

	appendpath := "./hydfs/" + appendname
	appendfile, err := os.Open(appendpath)
	if err != nil {
		return "File not found append"
	}
	defer appendfile.Close()
	// append to the hydfs file, how?

	_, err = io.Copy(hydfsfile, appendfile)
	if err != nil {
		return fmt.Sprintf("Error appending content: %v", err)
	}

	//ReplicateFile(hydfsfilename)

	// Successfully appended
	os.Remove(appendpath)
	return "[HyDFS] Append operation completed successfully"
}

// multiappend function
func MultiAppend(hydfsfilename string, nodes []string, localFilenames []string) string {
	if len(nodes) != len(localFilenames) {
		fmt.Printf("Error: The number of nodes and local filenames must match")
		return "Error"
	}

	var wg sync.WaitGroup
	results := make([]string, len(nodes))

	// Launch appends concurrently for each VM and corresponding local file
	for i := 0; i < len(nodes); i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			vm := nodes[index]
			localFile := localFilenames[index]

			// Perform the append operation
			fmt.Printf("[HyDFS] Launching append from %s with file %s\n", vm, localFile)

			url := fmt.Sprintf("http://%s/multiappend?hydfsfilename=%s&localfilename=%s", vm+":8080", hydfsfilename, localFile)

			result, err := http.Get(url)
			if err != nil {
				results[index] = fmt.Sprintf("VM: %s, Result: %s\n", vm, err)
				result.Body.Close()
			}

			results[index] = fmt.Sprintf("VM: %s, Result: %s\n", vm, result.Status)
			result.Body.Close()
		}(i)
	}

	// Wait for all appends to finish
	wg.Wait()

	// Aggregate and return results
	return strings.Join(results, "\n")
}

// function to merge all copies of a file on hydfs
func MergeHyDFSFile(hydfsfilename string) string {
	// TODO: Update all replicas of the given file to be identical.
	self := GetHostname()
	fmt.Printf("Self: %s\n", self)
	// get primary node
	node := hash.Get(hydfsfilename)
	node = strings.Split(node, ":")[0]

	/*
		STEPS FOR MERGE
		1. Get all replicas of the node
		2. If replicas are identical, do nothing
		3. Use the append logs (TODO) to consolidate all appends
		4. Send the merged file back to all the replica nodes as the "latest" file

		Notes
			* Will need to implement append logs
			* Use the SendFile to send the updated file back to replica nodes

	*/

	fmt.Printf("Node where HyDFS file [%s] lives: [%s]\n", hydfsfilename, node)

	replicas := GetReplicaNodes(hydfsfilename)
	if len(replicas) == 0 {
		return fmt.Sprintf("Error: No replicas found for file %s", hydfsfilename)
	}

	// Retrieve contents from all replicas
	replicaContents := make([][]byte, 0)
	for _, node := range replicas {
		content, err := FetchFileFromNode(node, hydfsfilename)
		if err != nil {
			fmt.Printf("[ERROR] Failed to fetch file %s from node %s: %s\n", hydfsfilename, node, err)
			continue
		}
		replicaContents = append(replicaContents, content)
	}

	// check if identical
	if AreReplicasIdentical(replicaContents) {
		fmt.Printf("[HyDFS] No merge needed for [%s], replicas are identical.\n", hydfsfilename)
		return "Merge complete (no changes needed)"
	}

	// Merge the contents based on client ordering
	mergedContent := MergeReplicaContents(replicaContents)

	// send merged file to this node
	SendMergedFile(node, hydfsfilename, mergedContent)

	// send merged file to the replicas
	for _, node := range replicas {
		err := SendMergedFile(node, hydfsfilename, mergedContent)
		if err != nil {
			fmt.Printf("[ERROR] Failed to send merged file %s to node %s: %s\n", hydfsfilename, node, err)
		}
	}

	return "Success"
}

func Merge(hydfsfilename string) string {
	self := GetHostname()
	fmt.Printf("Self: %s\n", self)
	// get primary node
	node := hash.Get(hydfsfilename)
	node = strings.Split(node, ":")[0]

	//fmt.Printf("Node where HyDFS file [%s] lives: [%s]\n", hydfsfilename, node)

	replicas := GetReplicaNodes(hydfsfilename)
	if len(replicas) == 0 {
		return fmt.Sprintf("Error: No replicas found for file %s", hydfsfilename)
	}

	content, err := FetchFileFromNode(node, hydfsfilename)

	if err != nil {
		return "Count not fetch data for merge"
	}

	// send merged file to the replicas
	for _, replica := range replicas {
		if replica == node {
			continue
		}
		err := SendMergedFile(replica, hydfsfilename, content)
		if err != nil {
			fmt.Printf("[ERROR] Failed to send merged file %s to node %s: %s\n", hydfsfilename, node, err)
		}
	}

	return "Success"

}

// function to check whether the file and its replicas are identical
func AreReplicasIdentical(replicaContents [][]byte) bool {
	if len(replicaContents) < 2 {
		return true // Only one or no content, so considered identical
	}

	// Compare each replica's content to the first replica's content
	for _, content := range replicaContents[1:] {
		if !bytes.Equal(replicaContents[0], content) {
			return false // Found a difference, replicas are not identical
		}
	}
	return true // All replicas are identical
}

// send the merged file content to the current node and its replicas
func SendMergedFile(node, hydfsfilename string, mergedContent []byte) error {
	url := fmt.Sprintf("http://%s/upload?hydfsfilename=%s", node+":8080", hydfsfilename)
	req, err := http.NewRequest("POST", url, bytes.NewReader(mergedContent))
	if err != nil {
		return fmt.Errorf("could not create POST request: %s", err)
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("could not make POST request: %s", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	return nil
}

// merging helper
func MergeReplicaContents(replicaContents [][]byte) []byte {
	// Combine all replica contents in order.
	// Assuming contents are ordered as they were appended, concatenate them.
	mergedContent := bytes.Join(replicaContents, []byte("\n"))
	return mergedContent
}

// helper function to get node replicas
func GetReplicaNodes(hydfsfilename string) []string {
	ringsize := len(ring)
	if ringsize == 0 {
		return nil
	}

	// Find primary node and calculate its successors
	primaryNode := hash.Get(hydfsfilename)
	id := ring[primaryNode]
	replicaNodes := []string{primaryNode}

	// Get the next two successor nodes for replication
	for i := 1; i <= 2; i++ {
		successorVal := (id + i) % ringsize
		for node, val := range ring {
			if val == successorVal {
				replicaNodes = append(replicaNodes, node)
			}
		}
	}

	return replicaNodes
}

// helper function to fetch file contents from a replica node
func FetchFileFromNode(node, hydfsfilename string) ([]byte, error) {
	url := fmt.Sprintf("http://%s/download?hydfsfilename=%s", node+":8080", hydfsfilename)
	resp, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("HTTP GET failed: %s", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	content, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("could not read response body: %s", err)
	}

	return content, nil
}

func CreateHyDFSFileLocally(filename string, hydfsfilename string) string {
	inFile, err := os.Open("./local/" + filename)
	if err != nil {
		fmt.Println("Could not find local file")
		return "Could not find local file"
	}
	defer inFile.Close()

	outFile, err := os.Create("./hydfs/" + hydfsfilename)
	if err != nil {
		fmt.Println("Could not create new file")
		return "Could not create new file"
	}
	defer outFile.Close()

	_, err = io.Copy(outFile, inFile)
	if err != nil {
		fmt.Println("Could not write data")
		return "Could not write data"
	}

	fmt.Printf("Created new file [%s] on HyDFS", hydfsfilename)
	return "Success"
}

// this layer lives above the failure detector
func AddRingFile(filename string, hydfsfilename string) {
	node := hash.Get(hydfsfilename)
	node = strings.Split(node, ":")[0]
	//fmt.Printf("Node to store the HyDFS file [%s]: [%s]\n", hydfsfilename, node)
	self := GetHostname()
	if node == self {
		resp := CreateHyDFSFileLocally(filename, hydfsfilename)
		if resp != "Success" {
			fmt.Printf("[ERROR] FILE UPLOAD FAILED FOR [%s] LOCALLY with message %s\n", hydfsfilename, resp)
			return
		} else {
			StoredFiles = append(StoredFiles, filename)
			//fmt.Printf("[HyDFS] new file [%s] stored here\n", hydfsfilename)
		}
		ReplicateFile(hydfsfilename)
	} else {
		resp := SendFile(node+":8080", filename, hydfsfilename)
		if resp != "200 OK" {
			fmt.Printf("[ERROR] FILE UPLOAD FAILED FOR [%s] at [%s] with message %s\n", hydfsfilename, node, resp)
			return
		} else {
			//fmt.Printf("[HyDFS] new file [%s] stored at %s\n", hydfsfilename, node)

			// POTENTIAL SOLUTION: make the replication request through HTTP if file stored on another machine
			// TODO: write replication http handler, maybe this will solve replication issue
			// ReplicateFile(hydfsfilename)

			// Send a request to the node to replicate the file
			replicateURL := fmt.Sprintf("http://%s/replicate?hydfsfilename=%s", node+":8080", hydfsfilename)
			replicateResp, err := http.Get(replicateURL)
			if err != nil || replicateResp.StatusCode != http.StatusOK {
				fmt.Printf("[ERROR] Failed to trigger replication for [%s] at node [%s]\n", hydfsfilename, node)
				fmt.Printf("[ERROR MESSAGE] %s \n", err)
				fmt.Printf("[STATUS CODE] %d \n", replicateResp.StatusCode)
			}

			// close response body to avoid a resource leak
			replicateResp.Body.Close()

		}
	}
}

// this layer lives above the failure detector
func RemoveRingFile(filename string) {
	node := hash.Get(filename)
	self := GetHostname()
	if node == self {
		StoredFiles = remove(StoredFiles, filename)
		fmt.Printf("[HyDFS] removed file [%s] stored here\n", filename)
	} else {
		// TODO: send message to the actual node to remove it...
		// was thinking HTTP
		fmt.Printf("[HyDFS] removed file [%s] stored at %s\n", filename, node)
	}
}

// this layer lives within the failure detector
// func AddRingNode(nodename string) {
// 	nodename = strings.Split(nodename, ":")[0]
// 	hash.Add(nodename)
// 	nodes := hash.GetNodeNames()
// 	//sort.Strings(nodes)
// 	ring = make(map[string]int)
// 	for i, node := range nodes {
// 		node = strings.Split(node, ":")[0]
// 		ring[node] = i
// 	}
// 	ReplicateNode()
// 	// fmt.Printf("[HyDFS] added new node %s to ring\n", nodename)
// }

// this layer lives within the failure detector
func RemoveRingNode(nodename string) {
	nodename = strings.Split(nodename, ":")[0]
	hash.Remove(nodename)
	nodes := hash.GetNodeNames()
	nextNode, _ := getNextNodeAddress(GetHostname())
	//sort.Strings(nodes)
	ring = make(map[string]int)
	for i, node := range nodes {
		node = strings.Split(node, ":")[0]
		ring[node] = i
	}
	ReplicateNode()
	if GetHostname() != "fa24-cs425-1501" && nextNode == nodename+":8000" {
		ResendSavedData()
	}
	//fmt.Printf("[HyDFS] removed node %s from the ring\n", nodename)
}

// just instantiates an empty hash
func InitRing() {
	config := bhash.Config{
		ReplicationFactor: 1,
	}
	hash = bhash.NewWithNodes(ringnodes, config)
	nodes := hash.GetNodeNames()
	//sort.Strings(nodes)
	ring = make(map[string]int)
	for i, node := range nodes {
		node = strings.Split(node, ":")[0]
		ring[node] = i
	}
}

func remove(slice []string, elem string) []string {
	for i, v := range slice {
		if v == elem {
			return append(slice[:i], slice[i+1:]...)
		}
	}
	return slice
}

func SetLog(logFile *os.File) {
	LogFile = logFile
}
func GetHostname() string {
	name, err := os.Hostname()
	if err != nil {
		panic(err)
	}
	hostname := strings.Split(name, ".")
	return hostname[0]
}

func EncodeMessage(msg Message) []byte {
	gossipBuffer.mu.Lock()
	defer gossipBuffer.mu.Unlock()
	n := len(gossipBuffer.arr)

	msg.GossipInfo = gossipBuffer.arr[max(0, n-10):n]

	data, err := json.Marshal(msg)
	if err != nil {
		fmt.Printf("[ERROR] Error Marshaling Message\n")
	}
	return data
}

func DecodeMessage(data []byte) Message {
	var response Message
	err := json.Unmarshal(data, &response)
	if err != nil {
		fmt.Printf("[ERROR] Error Unmarshaling Response: %s\n", err)
	}
	return response
}

func GetShuffledMembers() []interface{} {
	keys := make([]interface{}, 0)
	MembershipList.Range(func(key, value interface{}) bool {
		keys = append(keys, key)
		return true // Continue iteration
	})

	// randomize list of keys
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	r.Shuffle(len(keys), func(i, j int) {
		keys[i], keys[j] = keys[j], keys[i]
	})

	return keys
}

func EnableSus(timestamp ...time.Time) {
	fmt.Printf("[INFO] ENABLING SUSPICION\n")
	var old_status string
	if SusStatus {
		old_status = "on"
	} else {
		old_status = "off"
	}

	if len(timestamp) == 0 {
		SusStatus = true
		cur_time := time.Now()
		GossipInfo(Gossip{Type: "STATE", Id: "", Timestamp: cur_time, OldStatus: old_status, NewStatus: "on"})
		LastStateChange = cur_time
	} else {
		SusStatus = true
		GossipInfo(Gossip{Type: "STATE", Id: "", Timestamp: timestamp[0], OldStatus: old_status, NewStatus: "on"})
		LastStateChange = timestamp[0]
	}
}

func DisableSus(timestamp ...time.Time) {
	fmt.Printf("[INFO] DISABLING SUSPICION\n")
	var old_status string
	if SusStatus {
		old_status = "on"
	} else {
		old_status = "off"
	}

	if len(timestamp) == 0 {
		SusStatus = false
		cur_time := time.Now()
		GossipInfo(Gossip{Type: "STATE", Id: "", Timestamp: cur_time, OldStatus: old_status, NewStatus: "off"})
		LastStateChange = cur_time
	} else {
		SusStatus = false
		GossipInfo(Gossip{Type: "STATE", Id: "", Timestamp: timestamp[0], OldStatus: old_status, NewStatus: "off"})
		LastStateChange = timestamp[0]
	}

}

func HandleError(err error, err_msg string) {
	if err != nil {
		if err_msg == "" {
			fmt.Printf("[ERROR] %s\n", err)
		} else {
			fmt.Printf("[ERROR] %s\n", err_msg)
		}
	}
}

func HandleResponse(data []byte) {
	response := DecodeMessage(data)
	if response.Type == "JOIN" {
		fmt.Printf("[INFO] recieved join request from: %s\n", response.SourceId)
		AddNewMember(response.SourceId, response.Data.(string))
		fmt.Printf("[INFO] sucessfully added new member: %s\n", response.SourceId)
		fmt.Fprintf(LogFile, "[INFO] sucessfully added new member: %s\n", response.SourceId)
		members := GetMemberList()

		init_data := InitData{Members: members, SusStatus: SusStatus}

		join_ack_msg := Message{Type: "JOIN_ACK", SourceId: "initiator", Data: init_data}
		data := EncodeMessage(join_ack_msg)
		conn, err := net.Dial("udp", response.Data.(string))
		HandleError(err, "error sending JOIN_ACK")
		defer conn.Close()

		conn.Write(data)
	} else if response.Type == "JOIN_ACK" {
		JOIN_ACK_CHANNEL <- response
	} else if response.Type == "PING" {
		HandleGossip(response.GossipInfo)
		if DropFlag.Drop {
			time.Sleep(DELAY)
		}
		conn, err := net.Dial("udp", response.Data.(string))
		HandleError(err, "error sending PING_ACK")
		defer conn.Close()

		ping_ack_msg := Message{Type: "PING_ACK", SourceId: SelfId, Data: SelfAddr}
		data := EncodeMessage(ping_ack_msg)
		conn.Write(data)
	} else if response.Type == "PING_ACK" {
		rand := rand.Float64()
		if rand >= MESSAGE_DROP_RATE || response.Data.(string) == InitiatorAddr {
			LastPing.Mu.Lock()
			last_ping := LastPing.Id
			LastPing.Mu.Unlock()
			if last_ping == response.SourceId {
				HandleGossip(response.GossipInfo)
				PING_ACK_CHANNEL <- response
			}
		}
	}
}

func HandleGossip(gossipInfo []Gossip) {
	for _, gossip := range gossipInfo {
		if gossip.Type == "NODE" {
			if value, ok := MembershipList.Load(gossip.Id); ok { //if gossip target is in membership list
				member := value.(Member)
				if member.Timestamp.Unix() < gossip.Timestamp.Unix() { // gossip is new information
					member.Timestamp = gossip.Timestamp
					LastGossip = gossip.Timestamp
					member.Status = gossip.NewStatus
					MembershipList.Store(member.Id, member)
					GossipInfo(gossip)
					if gossip.NewStatus == "SUSPECT" {
						// fmt.Printf("[GOSSIP] mark %s as SUSPECT\n", member.Id)
						fmt.Fprintf(LogFile, "[GOSSIP] mark %s as ALIVE\n", member.Id)
					} else if gossip.NewStatus == "ALIVE" {
						// fmt.Printf("[GOSSIP] mark %s as ALIVE\n", member.Id)
						fmt.Fprintf(LogFile, "[GOSSIP] mark %s as ALIVE\n", member.Id)
					} else if gossip.NewStatus == "FAILED" {
						if member.Id == SelfId {
							os.Exit(0)
						}
						MembershipList.Delete(member.Id)
						RemoveRingNode(member.Hostname)
						// fmt.Printf("[GOSSIP] mark %s as FAILED\n", member.Id)
					} else if gossip.NewStatus == "LEFT" {
						if member.Id == SelfId {
							os.Exit(0)
						}
						MembershipList.Delete(member.Id)
						RemoveRingNode(member.Hostname)
						fmt.Printf("[GOSSIP] mark %s as LEFT\n", member.Id)
					}
				}

			} else if gossip.OldStatus == "" && gossip.NewStatus == "ALIVE" { // if gossip is a new join
				if LastGossip.Unix() < gossip.Timestamp.Unix() {
					fmt.Printf("[GOSSIP] ADDING NEW MEMBER: %s\n", gossip.Id)
					fmt.Fprintf(LogFile, "[GOSSIP] ADDING NEW MEMBER: %s\n", gossip.Id)
					fields := strings.Split(gossip.Id, "@")
					AddNewMember(gossip.Id, fields[0], gossip.Timestamp)
				}
			}
		} else if gossip.Type == "STATE" {
			if gossip.Timestamp.Unix() > LastStateChange.Unix() {
				if gossip.NewStatus == "on" && !SusStatus {
					EnableSus(gossip.Timestamp)
				} else if gossip.NewStatus == "off" && SusStatus {
					DisableSus(gossip.Timestamp)
				}
			}
		}
	}
}

func GossipInfo(gossip Gossip) {
	gossipBuffer.mu.Lock()
	defer gossipBuffer.mu.Unlock()

	if len(gossipBuffer.arr) >= 20 {
		gossipBuffer.arr = gossipBuffer.arr[12:]
	}

	gossipBuffer.arr = append(gossipBuffer.arr, gossip)

}

func AddNewMember(id string, hostname string, update_time ...time.Time) {
	var cur_time time.Time
	if len(update_time) == 0 {
		cur_time = time.Now()
	} else {
		cur_time = update_time[0]
	}
	member := Member{Id: id, Hostname: hostname, Timestamp: cur_time, Status: "ALIVE", IsSelf: id == SelfId}
	MembershipList.Store(id, member)
	//AddRingNode(hostname)
	gossip := Gossip{Type: "NODE", Id: id, Timestamp: cur_time, OldStatus: "", NewStatus: "ALIVE"}
	GossipInfo(gossip)
}

func LeaveGroup() {
	GossipInfo(Gossip{Type: "NODE", Id: SelfId, Timestamp: time.Now(), OldStatus: "ALIVE", NewStatus: "LEFT"})
	fmt.Printf("[INFO] %s leaving group...\n", SelfId)
	fmt.Fprintf(LogFile, "[INFO] %s leaving group...\n", SelfId)
	time.Sleep(LEAVE_DISSEMINATION)
	fmt.Printf("[INFO] %s left group\n", SelfId)
	fmt.Fprintf(LogFile, "[INFO] %s left group\n", SelfId)
	os.Exit(0)
}

func MarkFailed(member Member) {
	member.Status = "FAILED"
	MembershipList.Delete(member.Id)
	RemoveRingNode(member.Hostname)
	GossipInfo(Gossip{Type: "NODE", Id: member.Id, Timestamp: time.Now(), OldStatus: member.Status, NewStatus: "FAILED"})

}

func MarkSuspect(member Member) {
	member.Status = "SUSPECT"
	MembershipList.Store(member.Id, member)
	GossipInfo(Gossip{Type: "NODE", Id: member.Id, Timestamp: time.Now(), OldStatus: member.Status, NewStatus: "SUSPECT"})
}

func MarkAlive(member Member) {
	member.Status = "ALIVE"
	MembershipList.Store(member.Id, member)
	GossipInfo(Gossip{Type: "NODE", Id: member.Id, Timestamp: time.Now(), OldStatus: member.Status, NewStatus: "ALIVE"})

}

func GetMember(id string) Member {
	value, ok := MembershipList.Load(id)
	if !ok {
		// fmt.Printf("[ERROR] Member missing from list %s\n", id)
		return Member{Id: "NULL"}
	}
	member := value.(Member)
	return member
}

func GetMemberList() []Member {
	var m []Member
	MembershipList.Range(func(id interface{}, value interface{}) bool {
		m = append(m, value.(Member))
		return true
	})
	return m
}

func InitializeNode(data interface{}) {
	init_data := data.(map[string]interface{})
	SusStatus = init_data["SusStatus"].(bool)

	for _, memberData := range init_data["Members"].([]interface{}) {
		memberMap := memberData.(map[string]interface{})

		parsedTime, err := time.Parse(time.RFC3339, memberMap["Timestamp"].(string))
		HandleError(err, "error parsing time")

		member := Member{
			Id:        memberMap["Id"].(string),
			Hostname:  memberMap["Hostname"].(string),
			Timestamp: parsedTime,
			Status:    memberMap["Status"].(string),
			IsSelf:    memberMap["Id"].(string) == SelfId,
		}

		MembershipList.Store(member.Id, member)
		//AddRingNode(member.Hostname)
	}

}

func PrintMembershipList() {
	fmt.Print("-------------------------------------[MEMBERSHIP LIST]-------------------------------------\n\n")
	// Step 1: Collect keys and values
	var keys []string
	items := make(map[string]Member)

	MembershipList.Range(func(key, value interface{}) bool {
		k := key.(string)
		v := value.(Member)
		keys = append(keys, k)
		items[k] = v
		return true
	})

	// Step 2: Sort the keys
	sort.Strings(keys)

	// Step 3: Print in sorted order of keys
	for _, k := range keys {
		var is_self string
		if items[k].IsSelf {
			is_self = "(self)"
		}
		fmt.Printf("%s\t%s\t%s %s\n", k, items[k].Status, items[k].Timestamp.String(), is_self)
	}
	fmt.Print("\n-------------------------------------------------------------------------------------------\n\n")
}

func PrintRingNodes() {
	fmt.Print("-------------------------------------[RING NODES]-------------------------------------\n\n")
	nodes := hash.GetNodeNames() // Get the sorted node names from the hash ring
	for i, node := range nodes {
		// Print the node and its corresponding RingID (which is the index in this case)
		fmt.Printf("HostName: %s, RingID: %d\n", node, i)
	}
	fmt.Print("\n-------------------------------------------------------------------------------------------\n\n")
}

func PrintRingFilesLocal() {
	fmt.Printf("----------------------------------[STORED FILES [RingID = %d] ]-------------------------------------\n\n", ring[GetHostname()])
	files, err := os.ReadDir("./hydfs/")
	if err != nil {
		fmt.Printf("[HyDFS] HyDFS directory doesn't exist...\n")
		return
	}
	for _, file := range files {
		node := hash.Get(file.Name())
		node = strings.Split(node, ":")[0]
		fmt.Printf("Filename: %s, RingID: %d\n", file.Name(), ring[node])
	}
	fmt.Print("\n-------------------------------------------------------------------------------------------\n\n")
}

// function to get the replicas and primary node of a given hydfs file
func PrintReplicas(hydfsfilename string) string {
	fmt.Printf("--------------------------------[REPLICA NODES FOR FILE [%s]: BASE NODE: [%s]]---------------------------\n\n", hydfsfilename, hash.Get(hydfsfilename))
	for key := range ring {
		url := fmt.Sprintf("http://%s/exists?hydfsfilename=%s", key+":8080", hydfsfilename)

		// Send GET request to the server
		resp, err := http.Get(url)
		if err != nil {
			continue
		}

		// Check if the request was successful
		if resp.StatusCode == http.StatusOK {
			fmt.Printf("Replica: %s, RingID: %d\n", key, ring[key])
		}

		resp.Body.Close()
	}
	fmt.Print("\n-------------------------------------------------------------------------------------------\n\n")

	return "Success"
}
