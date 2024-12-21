package main

import (
	"fmt"
	"os"
	"strings"
)

// filters data input by a certain keyword
func filter(tuple string, pattern string) string {
	parts := strings.SplitN(tuple, ",", 2)
	if strings.Contains(parts[1], pattern) {
		// <file:lineNum>,line
		return tuple
	}

	return ""
}

func main() {
	// Check if the right number of arguments are provided
	if len(os.Args) < 3 {
		fmt.Println("Usage: program <tuple> <pattern>")
		return
	}

	// Get the tuple and pattern from the command line arguments
	tuple := os.Args[1]
	pattern := os.Args[2]

	// Call the filter function
	result := filter(tuple, pattern)

	fmt.Print(result)
}
