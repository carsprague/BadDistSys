package main

import (
	"baddistsys/logger_server"
	"log"
	"os"
	"os/signal"
)

func main() {
	// Start the server
	_, lis, err := logger_server.SetupServer()
	if err != nil {
		log.Fatalf("failed to set up server: %v", err)
	}

	// Block the main goroutine to keep the server running
	// until an interrupt signal is received
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)

	// Wait for an interrupt signal
	<-stop

	// Handle server shutdown logic here, if necessary
	log.Println("Shutting down server...")
	lis.Close()
}
