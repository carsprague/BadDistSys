// Client definition for the logger service. The client connects
// to all avaliable servers and makes requests in a multi-cast
// fashion. The client generates separate log files containing
// the response from each server.

package logger_client

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	pb "baddistsys/logger"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// used to store the client, connection, and machine number
type clientConnPair struct {
	client     pb.LoggerServiceClient
	clientConn *grpc.ClientConn
	machineIdx int
}

var (
	// DNS for the virtual machines
	ipList = []string{"dns:///fa24-cs425-1501.cs.illinois.edu:8000",
		"dns:///fa24-cs425-1502.cs.illinois.edu:8000",
		"dns:///fa24-cs425-1503.cs.illinois.edu:8000",
		"dns:///fa24-cs425-1504.cs.illinois.edu:8000",
		"dns:///fa24-cs425-1505.cs.illinois.edu:8000",
		"dns:///fa24-cs425-1506.cs.illinois.edu:8000",
		"dns:///fa24-cs425-1507.cs.illinois.edu:8000",
		"dns:///fa24-cs425-1508.cs.illinois.edu:8000",
		"dns:///fa24-cs425-1509.cs.illinois.edu:8000",
		"dns:///fa24-cs425-1510.cs.illinois.edu:8000"}

	// map of client connections
	portToClient = make(map[string]clientConnPair)

	// synchronization primitives for important variables
	mutex_portToClient    sync.Mutex
	mutex_totalCount      sync.Mutex
	mutex_failedConnCount sync.Mutex
)

// This iterates through the list of servers (given by their DNS)
// and attempts to connect to them. It also probes each server
// with an echo command to make sure we are connected. If the
// client can connect to the server, the connection is added
// to the portToClient list.
func SetupClientConnections() (*pb.Reply, error) {
	var failedConnCount int = 0
	var wg sync.WaitGroup
	// iterate through the servers
	for idx, ip := range ipList {
		wg.Add(1)

		go func(idx int, ip string) {
			defer wg.Done()

			// setup connection to the server
			conn, err := grpc.NewClient(ip, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				log.Printf("could not connect to machine %d: {{ %v }}", idx+1, err)
				mutex_failedConnCount.Lock()
				failedConnCount++
				mutex_failedConnCount.Unlock()
				return
			}

			// Contact the server and print out its response.
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

			// create the client
			newClient := pb.NewLoggerServiceClient((conn))

			// probe server with an echo command
			testCmdArgs := []string{"connected!"}
			strm, err := newClient.LoggerRequest(ctx, &pb.Request{CmdName: "echo", Args: testCmdArgs})
			if err != nil {
				log.Printf("could not connect to machine %d: {{ %v }}", idx+1, err)
				cancel()
				mutex_failedConnCount.Lock()
				failedConnCount++
				mutex_failedConnCount.Unlock()
				return
			}

			output := HandleReply(strm, idx)
			log.Printf("reply from machine %d: %s", idx+1, output)

			mutex_portToClient.Lock()
			// add the connection to the list
			portToClient[ip] = clientConnPair{
				client:     newClient,
				clientConn: conn,
				machineIdx: idx + 1,
			}
			mutex_portToClient.Unlock()

			cancel()

		}(idx, ip)
	}

	wg.Wait()

	if failedConnCount == len(ipList) {
		return &pb.Reply{Output: "Failed!"}, fmt.Errorf("could not connect to any machine")
	}

	return &pb.Reply{Output: "Successfully connected!"}, nil

}

// This is the main loop of the client. It is responsible for
// reading user input, sending requests to the server,
// and finally creating a log file for the response from each
// server. If a grep command is passed in, it will also
// sum up the total pattern occurances, assuming that the user
// passes a command like 'grep -c && grep -i', so that the count
// occurs on the first line of the log file
func ProcessInputLoop() {
	reader := bufio.NewReader(os.Stdin)
	totalCount := 0
	grepFlag := false

	// loop forever
	for {
		// collect user input
		commandName, commandArgs := HandleInput(reader)

		startTime := time.Now()

		// Contact the server and print out its response.
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)

		// multithreading
		var wg sync.WaitGroup

		// iterate through
		for p, c := range portToClient {
			// add to waitgroup
			wg.Add(1)

			go func(p string, c clientConnPair) {
				defer wg.Done()
				// make the request
				strm, err := c.client.LoggerRequest(ctx, &pb.Request{CmdName: commandName, Args: commandArgs})

				// if the machine fails delete it from the list (failstop)
				if err != nil {
					log.Printf("request failed for machine %d\n {{ %v }}\ndisconnecting...", c.machineIdx, err)
					c.clientConn.Close()
					mutex_portToClient.Lock()
					delete(portToClient, p)
					mutex_portToClient.Unlock()
					return
				}

				output := HandleReply(strm, c.machineIdx)

				fname, err := NewFile(output, c.machineIdx)
				if err != nil {
					return
				}

				if commandName == "grep" {
					grepFlag = true
					// we pass in the command 'grep -c && grep -i' so that we can
					// parse the first line of the file for the count
					count := GetGrepCount(fname)
					log.Printf("ocurrances on machine %d: %d", c.machineIdx, count)
					mutex_totalCount.Lock()
					totalCount += count
					mutex_totalCount.Unlock()
				} else {
					log.Printf("output from machine %d: %s", c.machineIdx, output)
				}
			}(p, c)
		}

		// wait for all machines
		wg.Wait()

		if grepFlag {
			log.Printf("total occurances across all machines: %d", totalCount)
		}

		elapsedTime := time.Since(startTime)
		log.Printf("elapsed time: %s", elapsedTime)
		grepFlag = false
		totalCount = 0
		cancel()
		time.Sleep(500 * time.Millisecond)
	}
}

// Sets up the client
func SetupClient() {
	_, err := SetupClientConnections()
	if err != nil {
		log.Printf("%v\nexiting...", err)
		os.Exit(0)
	}

	defer CloseClientConnections()

	ProcessInputLoop()
}

// ------------------- HELPER FUNCTIONS ----------------- //

// Iterates through the list of connections and closes them,
// this should be called just before the client exits
func CloseClientConnections() {
	for _, pair := range portToClient {
		log.Printf("closing connection to machine %d", pair.machineIdx)
		pair.clientConn.Close()
	}
}

// Prints the prompt to the console and reads input from the user.
// The string will be parsed into the command and arguments.
func HandleInput(reader *bufio.Reader) (string, []string) {
	fmt.Print("baddistsys> ")
	input, err := reader.ReadString('\n')
	if err != nil {
		log.Fatalf("error reading input {{ %v }}", err)
	}

	// parse through the command, split into base command and arguments
	input = strings.TrimSpace(input)
	trimmedCommand := strings.Split(input, " ")

	commandName := trimmedCommand[0]
	commandArgs := trimmedCommand[1:]

	// fmt.Printf("Command: %s Args: %s\n", commandName, commandArgs)

	return commandName, commandArgs
}

// Parses the stream reply by continuing to read until reaching
// an EOF
func HandleReply(strm grpc.ServerStreamingClient[pb.Reply], idx int) string {
	var reply pb.Reply
	var output string
	for {
		err := strm.RecvMsg(&reply)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("error receiving message from machine %d: {{ %v }}", idx, err)
			break
		}
		output += reply.GetOutput()
	}
	return output
}

// Creates a new log file with the machine number and the current time
func NewFile(data string, idx int) (string, error) {
	fname := fmt.Sprintf("logs/m%d.%s.log", idx, time.Now().Format("20060102_150405"))
	f, err := os.Create(fname)
	if err != nil {
		log.Printf("error generating new file")
		return fname, err
	}
	defer f.Close()
	_, err = f.WriteString(data)
	if err != nil {
		log.Printf("error writing to file")
	}
	return fname, err
}

// Parses the first line of a file (given by fname) for
// the grep occurance count. This assumes the user
// enters a command like 'grep -c && grep -i' so that
// the count is the first line
func GetGrepCount(fname string) int {
	f, err := os.Open(fname)
	if err != nil {
		log.Printf("could not open file")
		return 0
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)

	if !scanner.Scan() {
		return 0
	}

	count, err := strconv.Atoi(scanner.Text())
	if err != nil {
		log.Printf("error converting string to text")
		return 0
	}

	return count
}
