package main

import (
	"bufio"
	"fmt"
	"log"
	"math/rand"
	"os"
)

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
const numLines = 200
const lineLength = 50 // You can change this to your preferred line length

func main() {
	// Create a new file to write random strings
	f, err := os.Create("logs/data.log")
	if err != nil {
		log.Fatalf("failed to create file: %v", err)
	}
	// Ensure file is closed after all operations
	defer f.Close()

	// Write random strings to the file
	for i := 0; i < numLines; i++ {
		_, err = f.WriteString(randString(lineLength) + "\n")
		if err != nil {
			log.Printf("error writing to file: %s", err)
		}
	}

	// Open another file for reading
	file, ferr := os.Open("maverick.txt")
	if ferr != nil {
		fmt.Println("Error opening existing file:", ferr)
		return
	}
	defer file.Close() // Close this file too when done

	scanner := bufio.NewScanner(file)

	// Open the original file again for appending content from "maverick.txt"
	f, err = os.OpenFile("logs/data.log", os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("failed to reopen file for appending: %v", err)
	}
	defer f.Close()

	// Read the file line by line and append to the data file
	for scanner.Scan() {
		line := scanner.Text()
		_, err = f.WriteString(line + "\n")
		if err != nil {
			log.Printf("error writing to file from maverick: %s", err)
		}
	}
	if err := scanner.Err(); err != nil {
		log.Printf("error reading maverick.txt: %v", err)
	}
}

// Helper function to generate a random string of 'n' characters
func randString(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}
