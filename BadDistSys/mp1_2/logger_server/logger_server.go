// Server definition for the logger service.
// Fairly standard, the server executes a command
// locally passed in by the client before returning
// the output to said client.

package logger_server

import (
	"fmt"
	"log"
	"net"
	"os/exec"
	"strings"

	pb "baddistsys/logger"

	"baddistsys/swim"

	"google.golang.org/grpc"
)

type server struct {
	pb.UnimplementedLoggerServiceServer
}

// function to execute the command sent from client
func Execute(commandName string, Args []string) (string, error) {
	// concatenate to a single string and just use shell formatting
	command := commandName + " " + strings.Join(Args, " ")
	fmt.Println(command)

	// the 'sh' and 'c' flags make the command execute
	// exactly as it would on a regular terminal, makes
	// it easier with formatting...
	commandOut, err := exec.Command("sh", "-c", command).Output()
	output := string(commandOut)

	// check error
	if err != nil {
		return "", err
	}

	// return the output
	return output, nil
}

// function to send the request from client to the execute function.
// Since the outputs can be large, we use streams to break up the data
// so we don't accidentally kill the connection
func (s *server) LoggerRequest(in *pb.Request, stream pb.LoggerService_LoggerRequestServer) error {
	// log.Printf("Request from client: %v", in.GetRequest())
	log.Println("received TCP request:")
	var commandName = in.CmdName
	var args = in.Args
	output, err := Execute(commandName, args)

	if err != nil {
		return err
	}

	// we need to break up the data so we don't get a gRPC overload
	// big shock that it doesn't like sending a 50MB packet at once lol
	chunkSize := 1024 * 1024

	// Loop over the output and send it in smaller chunks
	for start := 0; start < len(output); start += chunkSize {
		end := start + chunkSize
		if end > len(output) {
			end = len(output)
		}

		reply := &pb.Reply{
			Output: output[start:end], // Send this chunk of the output
		}
		if err := stream.Send(reply); err != nil {
			return err // Stop if there's an error in sending
		}
	}
	return nil
}

// Sets up the server on ports 8000 (TCP) and 9001->9010 (UDP)
func SetupServer() (*grpc.Server, net.Listener, error) {
	tcpPort := 8000
	udpPorts := []int{9001, 9002, 9003, 9004, 9005, 9006, 9007, 9008, 9009, 9010}

	// Find local DNS
	FindDNS()
	swim.PopulateMap()

	// init UDP ports and pinger
	for _, port := range udpPorts {
		go SetupUDP(port)
	}

	localDNS := swim.GetLocalDNS()
	introducer := swim.GetIntroducer()

	// join the system
	if localDNS != introducer {
		swim.SwimJoin()
	} else {
		swim.InitIntroducer()
	}

	// init pinger
	go swim.SwimPinger()

	// init TCP server
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", tcpPort))
	if err != nil {
		log.Printf("failed to listen on TCP port: %v", err)
		return nil, nil, err
	}
	log.Printf("TCP server listening at %v", lis.Addr())

	s := grpc.NewServer()
	pb.RegisterLoggerServiceServer(s, &server{})

	// handle TCP requests in a separate goroutine
	go HandleTCP(lis, s)

	// handle swim inputs
	go swim.SwimUserInput()

	// returning listener and grpc object
	return s, lis, nil
}

// TCP handler, runs in a separate goroutine
func HandleTCP(lis net.Listener, s *grpc.Server) {
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

func SetupUDP(port int) {
	udpAddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Printf("error resolving UDP address: %v", err)
		return
	}
	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		log.Printf("failed to listen on UDP port: %v", err)
		return
	}
	log.Printf("UDP server listening at %v", conn.LocalAddr())

	go swim.SwimUDPHandler(conn)
}

func FindDNS() {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		log.Printf("failed to get local IP addresses: %v", err)
		return
	}
	var ipAddress string
	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() && ipnet.IP.To4() != nil {
			ipAddress = ipnet.IP.String()
			break
		}
	}
	if ipAddress == "" {
		log.Println("No valid IP address found")
	} else {
		log.Printf("Server is running on IP: %s", ipAddress)
	}
	dnsList, err := net.LookupAddr(ipAddress)
	if err != nil {
		log.Printf("could not find local dns")
	}
	dns := strings.TrimSuffix(dnsList[0], ".")
	swim.SetLocalDNS(dns)
}
