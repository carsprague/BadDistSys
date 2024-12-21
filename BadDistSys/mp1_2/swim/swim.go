// Function declarations for the SWIM fault detection service.
// This should include functions that will, after a time period
// T, send out a UDP packet to a (uniformly) randomly selected node.
// There should also be a handler, that when sent a packet/ping,
// will send an ack back to the sender.

/*

1. Define message struct which contains sender, membership list, and ping message to send - DONE
2. Send Ack message to sender (right now it is sending back to target) - DONE
3. Implement Introducer node - DONE
4. Handle node joins - DONE
5. Handle node leaves - DONE
6. Handle node failures - DONE
7. Implement Suspicion
8. Command line commands (for demo) -
9. Report
*/

package swim

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	CLEAR   = 0 // No suspicion
	SUSPECT = 1 // Suspicion enabled

	DEAD  = 0
	ALIVE = 1
	SUSSY = 2
)

var (
	// DNS for the virtual machines (UDP ports)
	udpList = []string{"fa24-cs425-1501.cs.illinois.edu",
		"fa24-cs425-1502.cs.illinois.edu",
		"fa24-cs425-1503.cs.illinois.edu",
		"fa24-cs425-1504.cs.illinois.edu",
		"fa24-cs425-1505.cs.illinois.edu",
		"fa24-cs425-1506.cs.illinois.edu",
		"fa24-cs425-1507.cs.illinois.edu",
		"fa24-cs425-1508.cs.illinois.edu",
		"fa24-cs425-1509.cs.illinois.edu",
		"fa24-cs425-1510.cs.illinois.edu"}

	localDNS string

	/*
		Maps an address to its status in the cluster
		0 = Not running/failed
		1 = active
		2 = suspected

	*/
	memList = make(map[string]MemberStatus)

	ackChannels = make(map[string]chan bool)

	introducer = "fa24-cs425-1501.cs.illinois.edu"

	mutexMemList     sync.Mutex
	ackChannelsMutex sync.Mutex
	susMutex         sync.Mutex

	// suspicion mode
	sus int = CLEAR

	// used for deterministic randomness
	randIdx  = []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	idxCount = 0
)

type UDPMessage struct {
	Addr      *net.UDPAddr
	Cmd       string
	List      map[string]MemberStatus
	Suspicion int
}

type SwimPacket struct {
	Cmd       string
	List      map[string]MemberStatus
	Suspicion int
}

type MemberStatus struct {
	Suspicion int
	Timestamp time.Time
}

// ===============================================SWIM THREADS===============================================

// This is the main SWIM reader/handler thread. This runs on a
// per-port basis (10 copies of this thread) to handle each port.
// It processes the packet differently depending on the message header.
func SwimUDPHandler(conn *net.UDPConn) {
	udpChannel := make(chan UDPMessage)
	go HandleUDP(conn, udpChannel)

	for msg := range udpChannel {

		// isolate the command
		message := strings.TrimSpace(msg.Cmd)
		dnsList, _ := net.LookupAddr(msg.Addr.IP.String())
		dns := strings.TrimSuffix(dnsList[0], ".")

		if message == "ACK" {
			log.Printf("got ack from %v: %s", dns, message)
			ackChannelsMutex.Lock()
			if ackChannel, exists := ackChannels[dns]; exists {
				ackChannel <- true
			}
			ackChannelsMutex.Unlock()
			go UpdateMap(msg.List)

		} else if message == "PING" {
			log.Printf("got ping from %v: %s", dns, message)
			SwimAck(dns)
			go UpdateMap(msg.List)

		} else if message == "JOIN" {
			//log.Printf("got join from %v: %s", dns, message)
			mutexMemList.Lock()
			UpdateSuspicion(dns, ALIVE)
			UpdateTime(dns, time.Now())
			mutexMemList.Unlock()
			Multicast("UPDATE")

		} else if message == "LEAVE" {
			//log.Printf("got leave from %v: %s", dns, message)
			mutexMemList.Lock()
			UpdateSuspicion(dns, DEAD)
			mutexMemList.Unlock()

		} else if message == "SUSPICION" {
			log.Printf("received suspicion command from %v", dns)
			susMutex.Lock()
			sus = SUSPECT
			susMutex.Unlock()

		} else if message == "CLEAR" {
			log.Printf("clearing suspicion state from %v", dns)
			susMutex.Lock()
			sus = CLEAR
			susMutex.Unlock()

		} else if message == "UPDATE" {
			UpdateMap(msg.List)
		}
	}
}

// This is our pinger. It sends a ping out
// after every K seconds.
func SwimPinger() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		SwimPing()
	}
}

// ===============================================HELPER FUNCTIONS===============================================

// API to update timestamp in membership list
func UpdateTime(dns string, t time.Time) {
	updated := memList[dns]
	updated.Timestamp = t
	memList[dns] = updated
}

// API to update suspicion in membership list
func UpdateSuspicion(dns string, sus int) {
	updated := memList[dns]
	updated.Suspicion = sus
	memList[dns] = updated
}

// Serializes our data to be sent over UDP
func SerializeGOB(cmd string) []byte {
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)

	data := SwimPacket{
		Cmd:       cmd,
		List:      memList,
		Suspicion: sus,
	}

	err := encoder.Encode(data)
	if err != nil {
		return nil
	}
	return buf.Bytes()
}

// Deserializes a message sent over UDP
func DeSerializeGOB(data []byte) SwimPacket {
	var packet SwimPacket
	buf := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(buf)

	err := decoder.Decode(&packet)
	if err != nil {
		return SwimPacket{}
	}
	return packet
}

// Initializes membership list
func PopulateMap() {
	for _, dns := range udpList {
		UpdateSuspicion(dns, DEAD)
		UpdateTime(dns, time.Time{})
	}
	UpdateSuspicion(introducer, ALIVE)
}

// Updates the membership list on specific conditions described
// in the SWIM paper. A newer timestamp has highest priority.
// If the timestamp is the same, then DEAD > SUS > ALIVE.
// If a significant change happens, it logs the membership list.
func UpdateMap(newList map[string]MemberStatus) {
	sendAlive := false
	listChanged := false
	mutexMemList.Lock()
	for k := range newList {
		currTime := memList[k].Timestamp
		currStatus := memList[k].Suspicion
		newTime := newList[k].Timestamp
		newStatus := newList[k].Suspicion

		if k == localDNS {
			// we have been marked as SUS NOOOOO
			if newStatus == SUSSY {
				// update our incarnation number
				UpdateSuspicion(k, ALIVE)
				UpdateTime(k, time.Now())
				sendAlive = true
				continue
			}
		}

		if newTime.After(currTime) {
			memList[k] = newList[k]
			switch newStatus {
			case DEAD:
				log.Printf("NODE FAILED: removing %s:%s from membership list", k, memList[k].Timestamp)
				UpdateSuspicion(k, DEAD)
				UpdateTime(k, newTime)
				listChanged = true
			case ALIVE:
				log.Printf("NEW ACTIVE NODE: adding %s:%s to membership list", k, memList[k].Timestamp)
				UpdateSuspicion(k, ALIVE)
				UpdateTime(k, newTime)
				listChanged = true
			case SUSSY:
				log.Printf("SUSPICIOUS NODE: marking %s:%s as suspicious", k, memList[k].Timestamp)
				UpdateSuspicion(k, SUSSY)
				UpdateTime(k, newTime)
				listChanged = true
			}
		} else if newTime.Equal(currTime) {
			if currStatus != newStatus {
				if currStatus == DEAD {
					// do nothing
				} else if newStatus == DEAD {
					UpdateSuspicion(k, DEAD)
					log.Printf("NODE FAILED: removing %s:%s from membership list", k, memList[k].Timestamp)
					listChanged = true
				} else if newStatus == SUSSY && currStatus == ALIVE {
					log.Printf("SUSPICIOUS NODE: marking %s:%s as suspicious", k, memList[k].Timestamp)
					UpdateSuspicion(k, SUSSY)
					listChanged = true
				}
			}
		}
	}
	mutexMemList.Unlock()

	if listChanged {
		NewMembershipFile()
	}

	if sendAlive {
		Multicast("UPDATE")
	}
}

// Initializes the introducer node
func InitIntroducer() {
	mutexMemList.Lock()
	intr := MemberStatus{
		Suspicion: 1,
		Timestamp: time.Now(),
	}
	memList[introducer] = intr
	mutexMemList.Unlock()
}

// APIs to access the membership
func GetMembership() map[string]MemberStatus {
	return memList
}

func SetLocalDNS(dns string) {
	localDNS = dns
}

func GetLocalDNS() string {
	return localDNS
}

func GetIntroducer() string {
	return introducer
}

// Function to handle/read from UDP port
func HandleUDP(conn *net.UDPConn, udpChannel chan UDPMessage) {
	buf := make([]byte, 1024)
	for {
		n, addr, err := conn.ReadFromUDP(buf)
		packet := DeSerializeGOB(buf)
		if err == nil && n > 0 {
			udpChannel <- UDPMessage{
				Addr:      addr,
				Cmd:       packet.Cmd,
				List:      packet.List,
				Suspicion: packet.Suspicion,
			}
		}
	}
}

// this is a generic helper function to send a custom message to a given dns target
func SendToTarget(target string, message string) {
	// Resolve the string address to a UDP address
	targetIp, _ := net.LookupIP(target)
	udpAddr, err := net.ResolveUDPAddr("udp", targetIp[0].String()+":90"+localDNS[13:15])
	if err != nil {
		log.Printf("error connecting to swim target: %v", err)
	}

	// Dial to the address with UDP
	conn, err := net.DialUDP("udp", nil, udpAddr)
	if err != nil {
		log.Printf("error dialing swim target: %v", err)
	}

	// Send a message to the server
	_, err = conn.Write(SerializeGOB(message))

	//log.Printf("Number of bytes sent: %v", b)

	if err != nil {
		log.Printf("error sending ping: %v", err)
	}
}

// Send a multicast message
func Multicast(cmd string) {
	for target, activeStatus := range memList {
		if activeStatus.Suspicion == 1 && (target != localDNS) {
			// send ping to this node
			SendToTarget(target, cmd)
		}
	}
}

// Handles failures
func HandleFailure(target string) {
	log.Printf("Node %s has failed.", target)

	mutexMemList.Lock()
	UpdateSuspicion(target, 0) // Update node as failed
	mutexMemList.Unlock()
}

// Handles suspicions
func HandleSuspicion(target string) {
	log.Printf("Node %s is suspected to have failed.", target)

	// update the memList accordingly to set value as suspected
	mutexMemList.Lock()
	UpdateSuspicion(target, 2)
	mutexMemList.Unlock()
}

// Helper function to handle suspicion status change
func ToggleSuspicion() {
	mutexMemList.Lock()
	if sus == SUSPECT {
		sus = CLEAR
		log.Println("Suspicion cleared.")
	} else {
		sus = SUSPECT
		log.Println("Suspicion enabled.")
	}
	mutexMemList.Unlock()

}

// Creates a new log file with the machine number and the current time
func NewMembershipFile() {
	fname := fmt.Sprintf("logs/membership.%s.log", time.Now().Format("20060102_150405"))
	f, err := os.Create(fname)
	if err != nil {
		log.Printf("error generating new file")
		return
	}
	defer f.Close()
	var result string
	for k, v := range memList {
		if v.Suspicion == 1 {
			result += fmt.Sprintf("%s:%s: ACTIVE\n", k, memList[k].Timestamp)
		} else if v.Suspicion == 2 {
			result += fmt.Sprintf("%s:%s: SUSPICIOUS\n", k, memList[k].Timestamp)
		}
	}
	result += "\n"
	_, err = f.WriteString(result)
	if err != nil {
		log.Printf("error writing to file")
	}
}

//==============================================MAIN FUNCTIONS===================================================

// Sends ping to a randomly selected node
func SwimPing() {
	isUs := false

	if idxCount == 10 {
		idxCount = 0
		rand.Shuffle(len(randIdx), func(i, j int) {
			randIdx[i], randIdx[j] = randIdx[j], randIdx[i]
		})
	}

	i := randIdx[idxCount]
	idxCount += 1
	target := udpList[i]

	if target == localDNS {
		i = (i + 1) % len(udpList)
		target = udpList[i]
	}

	isActive := memList[target].Suspicion
	if isActive == 0 {
		for {
			i = (i + 1) % len(udpList)
			target = udpList[i]
			active := memList[target].Suspicion

			if target == localDNS {
				isUs = true
			}

			if active != 0 {
				break
			}
		}
	}

	if isUs {
		return
	}

	log.Printf("sending ping to %s", target)

	// Ensure safe access to the ackChannels map
	ackChannelsMutex.Lock()

	// Check if an ackChannel for this target already exists to avoid overwriting
	if _, exists := ackChannels[target]; !exists {
		ackChannels[target] = make(chan bool)
	}
	ackChannel := ackChannels[target]
	ackChannelsMutex.Unlock()

	// Send the PING
	go func() {
		SendToTarget(target, "PING")
	}()

	// block until we receive the ACK or reach a timeout
	select {
	case <-ackChannel:
		log.Printf("Received ACK from %s", target)
		ackChannelsMutex.Lock()
		delete(ackChannels, target)
		ackChannelsMutex.Unlock()
	case <-time.After(4 * time.Second):
		log.Printf("Timeout waiting for ACK from %s", target)
		if sus == SUSPECT {
			if memList[target].Suspicion == 2 {
				HandleFailure(target)
			} else if memList[target].Suspicion == 1 {
				HandleSuspicion(target)
			}
		} else {
			HandleFailure(target)
		}
	}
}

// Sends ACK to the sender of ping
func SwimAck(dns string) {
	log.Printf("sending ack to %s", dns)
	SendToTarget(dns, "ACK")
}

// Handler for when a node joins the system
func SwimJoin() {
	//log.Printf("sending join to Introducer %s", introducer)

	SendToTarget(introducer, "JOIN")
}

// Handler for when a node voluntarily leaves the system
func SwimLeave(dns string) {
	Multicast("LEAVE")
}

// Handler for input commands to the server
func SwimUserInput() {
	reader := bufio.NewReader(os.Stdin)
	for {
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)

		switch input {
		case "list_mem":
			var result string
			for k, v := range memList {
				if v.Suspicion == 1 {
					result += fmt.Sprintf("%s:%s: ACTIVE\n", k, memList[k].Timestamp)
				} else if v.Suspicion == 2 {
					result += fmt.Sprintf("%s:%s: SUSPICIOUS\n", k, memList[k].Timestamp)
				}
			}
			result += "\n"
			log.Print(result)
		case "leave":
			log.Printf("leaving the system...")
			SwimLeave(localDNS)
			os.Exit(0)
		case "list_self":
			log.Printf("%s:%s - STATUS: %d (0 dead, 1 alive, 2 suspicious)", localDNS, memList[localDNS].Timestamp, memList[localDNS].Suspicion)
		case "enable_sus":
			log.Printf("enabling suspicion mode\n")
			// change the suspicion mode to on and update status for every node
			ToggleSuspicion()
			Multicast("SUSPICION")
		case "disable_sus":
			log.Printf("disabling suspicion mode\n")
			// change the suspicion mode to off and update status for every node
			ToggleSuspicion()
			Multicast("CLEAR")
		case "status_sus":
			// print the suspicion status to stdout
			fmt.Printf("suspicion mode: %d\n", sus)
		default:
			fmt.Printf("unknown command: %s\n", input)
		}
	}
}
