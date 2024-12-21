package main

import (
	"fmt"
	"os"
	"strings"
)

// split takes a tuple and returns specific parts as a string
func split(tuple string, _ string) string {
	parts := strings.SplitN(tuple, ",", 2)
	line := strings.Split(parts[1], ",")
	// <file:lineNum>,val
	return parts[0] + "," + line[2] + ", " + line[3]
}

func main() {
	// Check if the correct number of arguments are provided
	if len(os.Args) < 2 {
		fmt.Println("Usage: program <tuple>")
		return
	}

	// Get the tuple from the command line arguments
	tuple := os.Args[1]

	// Call the split function
	result := split(tuple, "")

	// Output the result
	fmt.Print(result)
}
