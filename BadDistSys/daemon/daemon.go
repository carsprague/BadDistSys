package main

import (
	"baddistsys/utils"
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"
)

func JoinMembership() int {
	conn, err := net.Dial("udp", utils.InitiatorAddr)
	utils.HandleError(err, "error connecting to initiator")
	defer conn.Close()

	join_msg := utils.Message{Type: "JOIN", SourceId: utils.SelfId, Data: utils.SelfAddr}
	data := utils.EncodeMessage(utils.Message(join_msg))
	conn.Write(data)

	select {
	case msg, ok := <-utils.JOIN_ACK_CHANNEL: // if JOIN_ACK recieved initialize and print membership list
		if !ok {
			fmt.Printf("[ERROR] JOIN_ACK channel is closed \n")
		} else {
			fmt.Printf("[INFO] sucessfully joined group \n")
		}

		utils.InitializeNode(msg.Data)
		return 0

	case <-time.After(utils.JOIN_TIMEOUT): // timeout joining if no response
		fmt.Printf("[ERROR] joining group timed out\n")
		return -1
	}
}

func PingCycle(wg *sync.WaitGroup, logFile *os.File) {
	defer wg.Done()
	for {
		// collect all keys
		time.After(utils.PING_TIMEOUT)
		keys := utils.GetShuffledMembers()
		for _, k := range keys {
			member := utils.GetMember(k.(string))
			if k.(string) == utils.SelfId && member.Id == "NULL" {
				os.Exit(0)
			}
			if member.Id != "NULL" {
				if utils.SusStatus && member.IsSelf && member.Status == "SUSPECT" {
					fmt.Printf("[INFO] marking SELF %s as ALIVE\n", member.Id)
					utils.MarkAlive(member)
				}
				if !member.IsSelf && member.Status != "FAILED" && member.Status != "LEFT" {
					// Connect to server for which to ping
					conn, err := net.Dial("udp", member.Hostname)
					err_msg := fmt.Sprintf("couldn't dial member for ping: %s", member.Id)
					utils.HandleError(err, err_msg)

					// send ping
					ping_msg := utils.Message{Type: "PING", SourceId: utils.SelfId, Data: utils.SelfAddr}
					data := utils.EncodeMessage(utils.Message(ping_msg))
					utils.LastPing.Mu.Lock()
					utils.LastPing.Id = member.Id
					utils.LastPing.Mu.Unlock()
					conn.Write(data)
					time.Sleep(utils.PING_TIMEOUT)

					// wait for response
					select {
					case _, ok := <-utils.PING_ACK_CHANNEL: // if PING_ACK recieved do nothing
						if !ok {
							fmt.Printf("[ERROR] PING_ACK channel is closed \n")
						} else if utils.SusStatus && member.Status == "SUSPECT" {
							fmt.Printf("[INFO] PING_ACK recieved from, %s: marking SUSPECT as ALIVE\n", member.Id)
							utils.MarkAlive(member)
						}
						// time.Sleep(utils.PING_CYCLE)

					// case <-time.After(utils.PING_TIMEOUT): // if no PING_ACK, gossip node as failed or suspicious
					default: // if no PING_ACK, gossip node as failed or suspicious
						if utils.SusStatus && member.Status != "SUSPECT" {
							fmt.Printf("[WARNING] PING_ACK not recieved from, %s: marking SUSPECT \n", member.Id)
							fmt.Fprintf(logFile, "[WARNING] PING_ACK not recieved from, %s: marking SUSPECT \n", member.Id)
							utils.MarkSuspect(member)
						} else if utils.SusStatus && member.Status == "SUSPECT" && time.Since(member.Timestamp) > utils.SUSPECT_TIMEOUT {
							fmt.Printf("[ERROR] marking SUSPECT %s as FAILED\n", member.Id)
							fmt.Fprintf(logFile, "[ERROR] marking SUSPECT %s as FAILED\n", member.Id)
							utils.MarkFailed(member)
						} else if !utils.SusStatus {
							fmt.Printf("[ERROR] PING_ACK not recieved from, %s: marking FAILED\n", member.Id)
							fmt.Fprintf(logFile, "[ERROR] PING_ACK not recieved from, %s: marking FAILED\n", member.Id)
							utils.MarkFailed(member)
						}

					}
				}
			}

		}
	}

}

func run_server(wg *sync.WaitGroup) {
	defer wg.Done()
	// set up UDP listener
	addr := net.UDPAddr{
		Port: 9000,
		IP:   net.ParseIP("0.0.0.0"), // Listen on all available interfaces
	}
	server, err := net.ListenUDP("udp", &addr)
	utils.HandleError(err, "could not set up daemon UDP listener")
	defer server.Close()

	// Main server loop
	buf := make([]byte, 4096)
	for {
		n, _, err := server.ReadFromUDP(buf)
		utils.HandleError(err, "could not read from UDP")
		utils.HandleResponse(buf[:n])
	}

}

func downloadHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "invalid request method", http.StatusMethodNotAllowed)
		return
	}

	// Retrieve filename from query parameters
	hydfsfilename := r.URL.Query().Get("hydfsfilename")
	if hydfsfilename == "" {
		http.Error(w, "missing filename", http.StatusBadRequest)
		return
	}

	// Construct file path and check if the file exists
	filePath := "./hydfs/" + hydfsfilename
	file, err := os.Open(filePath)
	if err != nil {
		http.Error(w, "file not found", http.StatusNotFound)
		return
	}
	defer file.Close()

	_, err = io.Copy(w, file) // write file to the http writer

	if err != nil {
		http.Error(w, "failed to download file", http.StatusInternalServerError)
		return
	}
}

func uploadHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "invalid request method", http.StatusMethodNotAllowed)
		return
	}

	// Retrieve the filename from the query parameters
	hydfsfilename := r.URL.Query().Get("hydfsfilename")
	if hydfsfilename == "" {
		http.Error(w, "missing filename", http.StatusBadRequest)
		return
	}

	// Create the file on the server to save the uploaded content
	//TODO: this may create some errors with replication, just watch out for that...
	outFile, err := os.Create("./hydfs/" + hydfsfilename)
	if err != nil {
		http.Error(w, "could not save file", http.StatusInternalServerError)
		return
	}
	defer outFile.Close()

	// Copy the request body directly to the server file
	_, err = io.Copy(outFile, r.Body)
	if err != nil {
		http.Error(w, "failed to save file", http.StatusInternalServerError)
		return
	}

	fmt.Printf("[HyDFS] new file [%s] stored here\n", hydfsfilename)
	// utils.ReplicateFile(hydfsfilename) - TODO: come back to this
}

// http handler for a replication request
func replicationHandler(w http.ResponseWriter, r *http.Request) {
	hydfsfilename := r.URL.Query().Get("hydfsfilename")
	if hydfsfilename == "" {
		http.Error(w, "Missing hydfsfilename", http.StatusBadRequest)
		return
	}

	// Call the ReplicateFile function
	utils.ReplicateFile(hydfsfilename)
	w.WriteHeader(http.StatusOK)
	fmt.Printf("Replication triggered for %s\n", hydfsfilename)
	fmt.Fprintf(w, "Replication triggered for %s\n", hydfsfilename)
}

func existsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "invalid request method", http.StatusMethodNotAllowed)
		return
	}

	// Retrieve filename from query parameters
	hydfsfilename := r.URL.Query().Get("hydfsfilename")
	if hydfsfilename == "" {
		http.Error(w, "missing filename", http.StatusBadRequest)
		return
	}

	// Construct file path and check if the file exists
	filePath := "./hydfs/" + hydfsfilename
	_, err := os.Stat(filePath)
	if err == nil {
		w.WriteHeader(http.StatusOK)
	} else {
		http.Error(w, "File not found", http.StatusNotFound)
	}
}

func multiappendHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "invalid request method", http.StatusMethodNotAllowed)
		return
	}

	// Retrieve filename from query parameters
	hydfsfilename := r.URL.Query().Get("hydfsfilename")
	if hydfsfilename == "" {
		http.Error(w, "missing hydfsfilename", http.StatusBadRequest)
		return
	}

	// Retrieve filename from query parameters
	localfilename := r.URL.Query().Get("localfilename")
	if localfilename == "" {
		http.Error(w, "missing filename", http.StatusBadRequest)
		return
	}

	localPath := "./local/" + localfilename
	_, err := os.Stat(localPath)
	if err != nil {
		http.Error(w, "local file not found", http.StatusNotFound)
		return
	}

	//ret := utils.AppendToHyDFSFile(localfilename, hydfsfilename)
	ret := utils.AppendClient(localfilename, hydfsfilename)
	fmt.Printf("[HyDFS] multiappend result: %s\n", ret)
	w.WriteHeader(http.StatusOK)
}

func appendHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "invalid request method", http.StatusMethodNotAllowed)
		return
	}

	// Retrieve filename from query parameters
	hydfsfilename := r.URL.Query().Get("hydfsfilename")
	if hydfsfilename == "" {
		http.Error(w, "missing hydfsfilename", http.StatusBadRequest)
		return
	}

	appendfilename := r.URL.Query().Get("appendfilename")
	if appendfilename == "" {
		http.Error(w, "missing appendfilename", http.StatusBadRequest)
		return
	}

	ret := utils.AppendServer(appendfilename, hydfsfilename)
	fmt.Printf("[HyDFS] append result: %s\n", ret)
	w.WriteHeader(http.StatusOK)
}

func roleHandler(w http.ResponseWriter, r *http.Request) {
	role := r.URL.Query().Get("rolename")
	exec := r.URL.Query().Get("exec")
	arg := r.URL.Query().Get("arg")
	file := r.URL.Query().Get("filename")

	// Decode the argument - this is to handle special chars in arg input
	decodedArg, err := url.QueryUnescape(arg)
	if err != nil {
		fmt.Printf("[ERROR] Failed to decode arg: %s\n", err)
		http.Error(w, "Failed to decode arg", http.StatusBadRequest)
		return
	}

	if strings.Contains(decodedArg, "\"") {
		decodedArg = strings.ReplaceAll(decodedArg, "\"", "")
	}

	// run a thread?
	fmt.Printf("Running Init role with the following args: %s\n %s\n %s\n %s\n", role, exec, decodedArg, file)
	go utils.InitRole(role, exec, decodedArg, file)
}

func killHandler(w http.ResponseWriter, r *http.Request) {
	os.Exit(0)
}

func killNode(dest string) {
	url := fmt.Sprintf("http://%s/kill", dest)
	http.Get(url)
}

func setupHttp(wg *sync.WaitGroup) {
	defer wg.Done()
	http.HandleFunc("/upload", uploadHandler)
	http.HandleFunc("/download", downloadHandler)
	http.HandleFunc("/replicate", replicationHandler)
	http.HandleFunc("/exists", existsHandler)
	http.HandleFunc("/multiappend", multiappendHandler)
	http.HandleFunc("/append", appendHandler)
	http.HandleFunc("/role", roleHandler)
	http.HandleFunc("/kill", killHandler)
	http.ListenAndServe(":8080", nil)
}

// parseCommand parses the command string, ensuring the last argument is enclosed in quotes and properly escapes special characters.
func parseCommand(command string) ([]string, string) {
	// Split the command into words while respecting quoted parts
	parts := splitCommandWithQuotes(command)

	// Process the last part to ensure it is properly quoted and escaped
	lastPart := parts[len(parts)-1]
	quotedLastPart := fmt.Sprintf(`"%s"`, escapeSpecialCharacters(strings.Trim(lastPart, `"`)))

	// Return the rest of the command parts and the quoted argument
	return parts[:len(parts)-1], quotedLastPart
}

// splitCommandWithQuotes splits the command string into components, respecting quotes.
func splitCommandWithQuotes(input string) []string {
	var result []string
	var current string
	inQuotes := false

	for _, char := range input {
		switch char {
		case '"':
			// Toggle inQuotes
			inQuotes = !inQuotes
			if !inQuotes && len(current) > 0 {
				// End of quoted section
				result = append(result, current)
				current = ""
			}
		case ' ':
			if inQuotes {
				current += string(char)
			} else if len(current) > 0 {
				// End of a word
				result = append(result, current)
				current = ""
			}
		default:
			current += string(char)
		}
	}

	// Add the final component
	if len(current) > 0 {
		result = append(result, current)
	}

	return result
}

// escapeSpecialCharacters escapes quotes and other special characters in the string.
func escapeSpecialCharacters(input string) string {
	var escaped strings.Builder
	for _, char := range input {
		switch char {
		case '"':
			escaped.WriteString(`\"`) // Escape quotes
		case '\\':
			escaped.WriteString(`\\`) // Escape backslashes
		default:
			escaped.WriteRune(char)
		}
	}
	return escaped.String()
}

func main() {
	logFile, err := os.Create("machine.log")
	if err != nil {
		log.Fatalf("Failed to create log file: %v", err)
	}
	defer logFile.Close()

	utils.SetLog(logFile)
	args := os.Args

	// Access the rest of the arguments
	if len(args) > 1 {
		MESSAGE_DROP_RATE, err := strconv.ParseFloat(args[1], 64)
		if err != nil {
			fmt.Printf("[ERROR] please input a float number: %s\n", args[1])
		}

		utils.MESSAGE_DROP_RATE = MESSAGE_DROP_RATE
	}

	var wg sync.WaitGroup
	wg.Add(3)
	utils.SelfAddr = utils.GetHostname() + ":9000"
	utils.SelfId = utils.SelfAddr + "@" + strconv.Itoa(int(time.Now().Unix()))
	go run_server(&wg)
	// TODO: run HTTP server to listen for file transfers
	go setupHttp(&wg)
	utils.InitRing()

	if utils.SelfAddr == utils.InitiatorAddr { // if I am initiator
		fmt.Printf("INITIATOR Listening on %s\n", utils.InitiatorAddr)
		member := utils.Member{Id: utils.SelfId, Hostname: utils.InitiatorAddr, Timestamp: time.Now(), Status: "ALIVE", IsSelf: true}
		utils.MembershipList.Store(utils.SelfId, member)
		//utils.AddRingNode(member.Hostname)
	} else {
		time.Sleep(1 * time.Second)
		err_code := JoinMembership()
		if err_code == -1 {
			return
		}
	}

	go PingCycle(&wg, logFile)

	scanner := bufio.NewReader(os.Stdin)
	for {
		text, err := scanner.ReadBytes('\n')
		utils.HandleError(err, "error reading stdin")
		command := string(text[:len(text)-1])
		// commandParts := parseCommandWithQuotes(command)
		// for _, part := range commandParts {
		// 	fmt.Printf("Command part: %s\n", part)
		// }
		switch commandParts := strings.Split(command, " "); commandParts[0] {
		case "create":
			if len(commandParts) < 3 {
				fmt.Printf("[ERROR] create requires 3 parameters")
				continue
			}
			filename := commandParts[1]
			hydfsfilename := commandParts[2]
			utils.AddRingFile(filename, hydfsfilename)
		case "get":
			if len(commandParts) < 3 {
				fmt.Printf("[ERROR] get requires 3 parameters")
				continue
			}
			hydfsfilename := commandParts[1]
			filename := commandParts[2]
			utils.GetFileLocally(hydfsfilename, filename)
		case "getfromreplica":
			if len(commandParts) < 4 {
				fmt.Printf("[ERROR] getfromreplica requires 4 parameters")
				continue
			}
			replica := commandParts[1]
			hydfsfilename := commandParts[2]
			filename := commandParts[3]
			utils.GetReplicaFileLocally(replica, hydfsfilename, filename)
		case "append":
			if len(commandParts) < 3 {
				fmt.Printf("[ERROR] append requires 3 parameters")
				continue
			}
			filename := commandParts[1]
			hydfsfilename := commandParts[2]
			ret := utils.AppendClient(filename, hydfsfilename)
			//ret := utils.AppendToHyDFSFile(filename, hydfsfilename)
			fmt.Printf("%s\n", ret)
		case "ls":
			// TODO
			if len(commandParts) < 2 {
				fmt.Printf("[ERROR] ls requires 2 parameters")
				continue
			}
			hydfsfilename := commandParts[1]
			utils.PrintReplicas(hydfsfilename)
		case "merge":
			if len(commandParts) < 2 {
				fmt.Printf("[ERROR] merge requires 2 parameters")
				continue
			}
			// TODO
			hydfsfilename := commandParts[1]
			utils.Merge(hydfsfilename)
		case "multiappend":
			if len(commandParts) < 3 {
				fmt.Printf("[ERROR] multiappend requires format of 'multiappend hydfsfilename fa24-cs425-15xx:filename fa24-cs425-15xx:filename\n")
				continue
			}
			hydfsfilename := commandParts[1]

			var nodes []string
			var localFilenames []string

			for _, pair := range commandParts[2:] {
				nodeFile := strings.Split(pair, ":")
				if len(nodeFile) != 2 {
					fmt.Printf("[ERROR] Invalid node:file pair format: %s\n", pair)
					continue
				}
				nodes = append(nodes, nodeFile[0])
				localFilenames = append(localFilenames, nodeFile[1])
			}

			result := utils.MultiAppend(hydfsfilename, nodes, localFilenames)
			fmt.Printf("%s\n", result)

		case "store":
			utils.PrintRingFilesLocal()
		case "list_mem_ids":
			utils.PrintRingNodes()
		case "list_mem":
			utils.PrintMembershipList()
		case "list_self":
			fmt.Println(utils.SelfId)
		case "leave":
			utils.LeaveGroup()
		case "enable_sus":
			utils.EnableSus()
		case "disable_sus":
			utils.DisableSus()
		case "status_sus":
			var status string
			if utils.SusStatus {
				status = "on"
			} else {
				status = "off"
			}
			fmt.Println(status)
		case "delay":
			utils.DropFlag.MU.Lock()
			utils.DropFlag.Drop = true
			utils.DropFlag.MU.Unlock()
		case "app1": // file, pattern
			// change this
			parts, commaSeparatedString := parseCommand(command)
			src_file := parts[1]
			out_file := parts[2]
			// pattern := commandParts[3]
			fmt.Printf("Src file: %s\n", src_file)
			fmt.Printf("Out file: %s\n", out_file)
			fmt.Printf("Extra arg: %s\n", commaSeparatedString)

			utils.ExecuteRainstorm("filter", "split", src_file, out_file, "3", commaSeparatedString)
		case "test1":
			src_file := commandParts[1]
			out_file := commandParts[2]
			pattern := commandParts[3]
			utils.ExecuteRainstorm("filter", "split", src_file, out_file, "3", pattern)
			time.Sleep(1500 * time.Millisecond)
			killNode("fa24-cs425-1506:8080")
			killNode("fa24-cs425-1510:8080")
		case "app2":
			// change this
			parts, commaSeparatedString := parseCommand(command)
			src_file := parts[1]
			out_file := parts[2]
			// pattern := commandParts[3]
			// fmt.Printf("Src file: %s\n", src_file)
			// fmt.Printf("Out file: %s\n", out_file)
			// fmt.Printf("Extra arg: %s\n", commaSeparatedString)
			utils.ExecuteRainstorm("filter", "count", src_file, out_file, "3", commaSeparatedString)
		case "test2":
			parts, commaSeparatedString := parseCommand(command)
			src_file := parts[1]
			out_file := parts[2]
			utils.ExecuteRainstorm("filter", "count", src_file, out_file, "3", commaSeparatedString)
			time.Sleep(1500 * time.Millisecond)
			killNode("fa24-cs425-1506:8080")
			killNode("fa24-cs425-1510:8080")
		case "rainstorm":
			// RainStorm <op1 _exe> <op2 _exe> <hydfs_src_file> <hydfs_dest_filename> <num_tasks>
			if len(commandParts) < 5 {
				fmt.Printf("[ERROR] Rainstorm must take 5 arguments\n")
				continue
			}

			op1_exe := commandParts[1]
			op2_exe := commandParts[2]
			hydfs_src_file := commandParts[3]
			hydfs_dest_filename := commandParts[4]
			num_tasks := commandParts[5]
			arg := commandParts[6]

			utils.ExecuteRainstorm(op1_exe, op2_exe, hydfs_src_file, hydfs_dest_filename, num_tasks, arg)
		case "grep":
			if len(commandParts) < 3 {
				fmt.Println("[ERROR] grep requires a search term")
				continue
			}
			searchTerm := commandParts[1]
			fileName := commandParts[2]

			if _, err := os.Stat(fileName); os.IsNotExist(err) {
				fmt.Fprintf(os.Stderr, "[ERROR] File %s does not exist\n", fileName)
				continue
			}

			cmd := exec.Command("grep", "-n", searchTerm, fileName)
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr

			err := cmd.Run()
			if err != nil {
				if gt, gs := err.(*exec.ExitError); gs {
					if gt.ExitCode() == 1 {
						fmt.Fprintln(os.Stdout, "No hits")
					}
				} else {
					fmt.Fprintf(os.Stderr, "Error running grep: %v\n", err)
				}
			}
		default:
			fmt.Println("[ERROR] unrecognized command")
		}
	}

}
