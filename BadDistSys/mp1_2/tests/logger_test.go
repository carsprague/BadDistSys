package tests

import (
	// "net"

	"fmt"
	"log"

	// "strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"

	// "os/exec"
	"context"
	// "io"
	"strconv"
	"sync"
	"time"

	"google.golang.org/grpc/credentials/insecure"

	client "baddistsys/logger_client"
	// server "baddistsys/logger_server"
	pb "baddistsys/logger"
)

type clientConnPair struct {
	client     pb.LoggerServiceClient
	clientConn *grpc.ClientConn
	machineIdx int
}

var (
	test_ipList = []string{"dns:///fa24-cs425-1501.cs.illinois.edu:8000",
		"dns:///fa24-cs425-1502.cs.illinois.edu:8000",
		"dns:///fa24-cs425-1503.cs.illinois.edu:8000",
		"dns:///fa24-cs425-1504.cs.illinois.edu:8000",
		"dns:///fa24-cs425-1505.cs.illinois.edu:8000",
		"dns:///fa24-cs425-1506.cs.illinois.edu:8000",
		"dns:///fa24-cs425-1507.cs.illinois.edu:8000",
		"dns:///fa24-cs425-1508.cs.illinois.edu:8000",
		"dns:///fa24-cs425-1509.cs.illinois.edu:8000",
		"dns:///fa24-cs425-1510.cs.illinois.edu:8000"}

	// ANSI escape codes for green and reset
	green  string = "\033[32m"
	yellow string = "\033[33m"
	red    string = "\033[31m"
	reset  string = "\033[0m"

	// port to client map
	portToClient = make(map[string]clientConnPair)

	mutex_portToClient    sync.Mutex
	mutex_totalCount      sync.Mutex
	mutex_failedConnCount sync.Mutex
)

func GreenPrint(str string) {
	fmt.Println(string(green), str, string(reset))
}
func YellowPrint(str string) {
	fmt.Println(string(yellow), str, string(reset))
}
func RedPrint(str string) {
	fmt.Println(string(red), str, string(reset))
}

// write test cases here
func TestSingleClientConnection(t *testing.T) {
	YellowPrint("RUNNING TEST: TestSingleClientConnection")
	// log.Println("Here")

	// var wg sync.WaitGroup
	// // start the server in a goroutine and capture the output
	// var s *grpc.Server
	// wg.Add(1)
	// go func() {
	// 	defer wg.Done()
	// 	var err error
	// 	s, _, err = server.SetupServer()
	// 	if err != nil {
	// 		assert.Error(t, err)
	// 	}
	// }()

	// // Wait for the server to be set up
	// wg.Wait()

	// // Ensure the server is properly set up before proceeding with client
	// if s == nil {
	// 	assert.Fail(t, "TEST FAILED: Server did not initialize correctly")
	// }

	r, err := client.SetupClientConnections()

	assert.Nil(t, err)
	if assert.Equal(t, "Successfully connected!", r.GetOutput()) {
		GreenPrint("TEST PASSED")
	}

	client.CloseClientConnections()

	// s.GracefulStop()

	YellowPrint("TEST DONE: TestSingleClientConnection")
}

// func TestAllClientFailure(t *testing.T) {
// 	YellowPrint("RUNNING TEST: TestAllClientFailure")
// 	assert.Equal(t, 2, 2)

// 	r, _ := client.SetupClientConnections()

// 	if assert.Equal(t, "Failed!", r.GetOutput()) {
// 		GreenPrint("TEST PASSED")
// 	}

// 	YellowPrint("TEST DONE: TestSingleClientFailure")
// }

// func TestLocalDistributed(t *testing.T) {
// 	YellowPrint("RUNNING TEST: TestLocalDistributed")
// 	// add wait group to ensure go routine to start server finishes before connecting client
// 	var wg sync.WaitGroup

// 	var s *grpc.Server

// 	wg.Add(1)
// 	go func() {
// 		defer wg.Done()
// 		var err error
// 		s, _, err = server.SetupServer()
// 		if err != nil {
// 			log.Println("Could not start the server, error occurred")
// 			assert.Error(t, err)
// 		}
// 	}()

// 	// Wait for the server to be set up
// 	wg.Wait()

// 	// Ensure the server is properly set up before proceeding with client
// 	if s == nil {
// 		assert.Fail(t, "TEST FAILED: Server did not initialize correctly")
// 	}

// 	// start the client
// 	r, err := client.SetupClientConnections()

// 	assert.Nil(t, err)
// 	assert.Equal(t, "Successfully connected!", r.GetOutput())

// 	// // grep the output for one of the files, compare with the program output
// 	// command := "grep -cH PUT ../logs/machine.1.log"
// 	// // command := "pwd"
// 	// out, err := exec.Command("sh", "-c", command).Output()

// 	// if err != nil {
// 	// 	log.Printf("Error %v", err)
// 	// 	assert.Error(t, err)
// 	// 	assert.Fail(t, "TEST FAILED: grep command returned an error")
// 	// }

// 	// if assert.Equal(t, "../logs/machine.1.log:56879\n", string(out)) {
// 	// 	GreenPrint("TEST PASSED")
// 	// }

// 	//fmt.Println(string(green), "TEST PASSED: Distributed Test", string(reset))

// 	// shut down the client connectons
// 	client.CloseClientConnections()

// 	// stop the server
// 	s.GracefulStop()
// 	YellowPrint("TEST DONE: TestLocalDistribued")
// }

func TestAllMachines(t *testing.T) {
	// assume server is running

	// setup client connections to the server

	// grep commands that we want to run against the log file
	first := "grep"
	firstArgs := []string{"-c", "\"Maverick\"", "logs/data.log", "&&", "grep", "-i", "\"Maverick\"", "logs/data.log"}
	firstCount := PopulateClients(first, firstArgs)

	second := "grep"
	secondArgs := []string{"-c", "\"123\"", "logs/data.log", "&&", "grep", "-i", "\"123\"", "logs/data.log"}

	secondCount := PopulateClients(second, secondArgs)

	third := "grep"
	thirdArgs := []string{"-c", "\"plane\"", "logs/data.log", "&&", "grep", "-i", "\"plane\"", "logs/data.log"}
	
	thirdCount := PopulateClients(third, thirdArgs)
	
	fourth := "grep"
	fourthArgs := []string{"-c", "\"ejukse\"", "logs/data.log", "&&", "grep", "-i", "\"ejukse\"", "logs/data.log"}
	fourthCount := PopulateClients(fourth, fourthArgs)
	
	fifth := "grep"
	fifthArgs := []string{"-c", "\"hello\"", "logs/data.log", "&&", "grep", "-i", "\"hello\"", "logs/data.log"}
	fifthCount := PopulateClients(fifth, fifthArgs)
	
	// execute the commands
	assert.Equal(t, firstCount, 341)
	assert.Equal(t, secondCount, 1)
	assert.Equal(t, thirdCount, 55)
	assert.Equal(t, fourthCount, 0)
	assert.Equal(t, fifthCount, 1)

	client.CloseClientConnections()

}

// helper function

// populate clients
func PopulateClients(commandName string, commandArgs []string) int {
	var failedConnCount int = 0

	for idx, ip := range test_ipList {

		// setup connection to the server
		conn, err := grpc.NewClient(ip, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("could not connect to machine %d: {{ %v }}", idx+1, err)
			failedConnCount++
			continue
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
			failedConnCount++
			continue
		}

		output := client.HandleReply(strm, idx)
		log.Printf("reply from machine %d: %s", idx+1, output)

		// add the connection to the list
		portToClient[ip] = clientConnPair{
			client:     newClient,
			clientConn: conn,
			machineIdx: idx + 1,
		}
		cancel()

	}

	totalCount := 0

	ctx2, cancel2 := context.WithTimeout(context.Background(), 20*time.Second)

	for p, c := range portToClient {
		strm, err := c.client.LoggerRequest(ctx2, &pb.Request{CmdName: commandName, Args: commandArgs})

		if err != nil {
			log.Printf("request failed for machine %d\n {{ %v }}\ndisconnecting...", c.machineIdx, err)
			c.clientConn.Close()
			delete(portToClient, p)
			cancel2()
			continue
		}

		// var reply pb.Reply
		// var output string
		// for {
		// 	err := strm.RecvMsg(&reply)
		// 	log.Printf("%s", reply.String())
		// 	if err == io.EOF {
		// 		break
		// 	}
		// 	if err != nil {
		// 		log.Printf("error receiving message from machine %d: {{ %v }}", c.machineIdx, err)
		// 		break
		// 	}
		// 	output += reply.GetOutput()
		// }

		output := client.HandleReply(strm, c.machineIdx)

		currCount := 0
		for i := 0; i < len(output); i++ {
			if output[i] == '\n' {
				totalCount, _ = strconv.Atoi(output[0:currCount])
				log.Printf("Total count: %d\n", totalCount)
				cancel2()
				return totalCount
				// break
			}
			currCount++
		}
		cancel2()
		time.Sleep(500 * time.Millisecond)
	}

	cancel2()
	return totalCount
}
