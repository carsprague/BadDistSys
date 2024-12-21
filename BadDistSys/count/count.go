package main

import (
	"fmt"
	"os"
	"strings"
)

// var (
// 	countsMutex sync.Mutex
// )

// need to check for previously counted tuples, and maintain running count
// helper which does the counting
// func count(tuple string, _ string) {
// 	// Split the tuple into two parts: header and line
// 	parts := strings.SplitN(tuple, ",", 2)
// 	if len(parts) != 2 {
// 		fmt.Printf("Invalid tuple format: %s\n", tuple)
// 		return
// 	}

// 	// Extract the filename and line number from the header
// 	header := parts[0]
// 	line := parts[1]

// 	lineNumber := strings.Split(header, ":")[1]
// 	fmt.Printf("Line number: %s\n", lineNumber)

// 	lineParts := strings.Split(line, ",")

// 	for _, part := range lineParts {
// 		fmt.Printf("Part: %s\n", part)
// 	}

// 	// checking for that abnormal case
// 	category := lineParts[8]
// 	if len(lineParts) > 20 {
// 		category = lineParts[9]

// 	}
// 	fmt.Printf("Category: %s\n", category)

// 	// Check if this tuple has already been processed
// 	if IsTupleProcessed(lineNumber) {
// 		fmt.Printf("Tuple %s has already been processed, skipping...", lineNumber)
// 		return
// 	}

// 	// // if not processed add to running count
// 	// err := UpdateCount(category)
// 	// if err != nil {
// 	// 	fmt.Printf("Error with updating the counts, check the UpdateCount function: %v\n", err)
// 	// }

// 	// // add to file tracking which tuples have been processed
// 	// err = UpdateProcessedTuples(lineNumber)

// 	// if err != nil {
// 	// 	fmt.Printf("Error with updating processed tuples, check UpdateProcessedTuples function: %v\n", err)
// 	// }
// }

// func IsTupleProcessed(lineNumber string) bool {
// 	countsMutex.Lock()
// 	defer countsMutex.Unlock()

// 	// check persistent storage
// 	processed, err := ReadProcessedTuples()
// 	if err != nil {
// 		fmt.Printf("Error reading processed tuples: %v", err)
// 		return false
// 	}

// 	// if not in persistent storage, then tuple needs to be processed
// 	return processed[lineNumber]
// }

// func CreateLocalFile(filename string) {
// 	_, err := os.Create("./local/" + filename)
// 	if err != nil {
// 		fmt.Printf("Could not create file, got error: %v\n", err)
// 	}
// }

// func ReadProcessedTuples() (map[string]bool, error) {
// 	filename := "processed.log"
// 	message := utils.GetFileLocally(filename, filename)
// 	if message == "" {
// 		fmt.Printf("[RAIN] Getting processed file failed with message: %s\n", message)
// 		// create the file if there was some error getting it locally
// 		CreateLocalFile(filename)
// 		utils.AddRingFile(filename, filename)
// 	}

// 	processed := make(map[string]bool)
// 	file, err := os.Open("./local/" + filename)

// 	if err != nil {
// 		return nil, fmt.Errorf("error opening processed file: %v", err)
// 	}

// 	defer file.Close()

// 	scanner := bufio.NewScanner(file)
// 	for scanner.Scan() {
// 		processed[scanner.Text()] = true
// 	}

// 	if err := scanner.Err(); err != nil {
// 		return nil, fmt.Errorf("error reading processed file: %v", err)
// 	}

// 	return processed, nil
// }

// func ReadCurrentCounts() (map[string]int, error) {
// 	// get the current counts from persistent storage
// 	filename := "counts.txt"
// 	message := utils.GetFileLocally(filename, filename)
// 	if message == "" {
// 		fmt.Printf("[RAIN] Getting counts file failed with message: %s\n", message)
// 		CreateLocalFile(filename)
// 		utils.AddRingFile(filename, filename)
// 	}

// 	// add to a map in memory
// 	counts := make(map[string]int)

// 	file, err := os.Open("./local/" + filename)

// 	if err != nil {
// 		fmt.Printf("error opening counts file: %v", err)
// 		return nil, fmt.Errorf("error opening counts file: %v", err)
// 	}

// 	defer file.Close()

// 	scanner := bufio.NewScanner(file)
// 	for scanner.Scan() {
// 		line := scanner.Text()

// 		// split line into category and current count
// 		parts := strings.SplitN(line, ",", 2)
// 		if len(parts) == 2 {
// 			category := strings.TrimSpace(parts[0])
// 			var count int
// 			_, err := fmt.Sscanf(parts[1], "%d", &count)
// 			if err != nil {
// 				fmt.Printf("Error parsing count for category %s: %v", category, err)
// 			} else {
// 				counts[category] = count
// 			}
// 		}

// 	}

// 	// check for any scanner errors
// 	if err := scanner.Err(); err != nil {
// 		fmt.Printf("error reading counts file: %v", err)
// 		return nil, fmt.Errorf("error reading counts file: %v", err)
// 	}

// 	// return map
// 	return counts, nil
// }

// func UpdateCount(category string) error {
// 	countsMutex.Lock()
// 	defer countsMutex.Unlock()

// 	currCounts, err := ReadCurrentCounts()
// 	if err != nil {
// 		fmt.Printf("ReadCurrentCounts() exited with error: %v\n", err)
// 		return err
// 	}

// 	// Update or add the category count
// 	currCounts[category]++

// 	// update the file
// 	file, err := os.Open("./local/counts.txt")

// 	if err != nil {
// 		fmt.Printf("error opening counts file for update: %v", err)
// 		return err
// 	}

// 	// write to the file in the same format as before
// 	defer file.Close()

// 	// Rewrite the counts to the file
// 	file.Seek(0, io.SeekStart)
// 	file.Truncate(0)

// 	for cat, count := range currCounts {
// 		_, err := file.WriteString(fmt.Sprintf("%s,%d\n", cat, count))
// 		if err != nil {
// 			fmt.Printf("error writing to counts file: %v", err)
// 			return err
// 		}
// 	}

// 	// send the counts file back to HyDFS
// 	utils.AddRingFile("counts.txt", "counts.txt")

// 	return nil
// }

// func UpdateProcessedTuples(lineNumber string) error {
// 	file, err := os.Open("./local/processed.log")
// 	if err != nil {
// 		fmt.Printf("Error opening processed file for updating: %v\n", err)
// 		return err
// 	}

// 	defer file.Close()

// 	_, err = file.WriteString(lineNumber + "\n")
// 	if err != nil {
// 		fmt.Printf("error writing to processed file: %v", err)
// 		return err
// 	}

// 	// send the processed nodes fle back to HyDFS
// 	utils.AddRingFile("processed.log", "processed.log")

// 	return nil
// }

// // Read the category counts from the counts file on HDFS
// func printCategoryCounts() {
// 	message := utils.GetFileLocally("counts.txt", "counts.txt")
// 	if message != "Success" {
// 		fmt.Printf("Error getting counts file for printing: %s\n", message)
// 	}

// 	file, err := os.Open("./local/counts.txt")
// 	if err != nil {
// 		fmt.Printf("Error getting counts file for printing: %s\n", message)
// 	}
// 	defer file.Close()

// 	fmt.Println("Current Category Counts:")
// 	scanner := bufio.NewScanner(file)
// 	for scanner.Scan() {
// 		line := scanner.Text()
// 		fmt.Println(line) // Print each line (category: count) from the file
// 	}
// 	if err := scanner.Err(); err != nil {
// 		fmt.Printf("Error reading counts file: %v", err)
// 	}
// }

// // Periodically print the current counts every 5 seconds
// func startPeriodicPrint() {
// 	ticker := time.NewTicker(5 * time.Second)
// 	defer ticker.Stop()

// 	for {
// 		<-ticker.C
// 		printCategoryCounts()
// 	}
// }

// count function, returns the field which it's counting
func count(tuple string, _ string) string {
	ret := ""
	// Split the tuple into two parts: header and line
	parts := strings.SplitN(tuple, ",", 2)
	if len(parts) != 2 {
		fmt.Printf("Invalid tuple format: %s\n", tuple)
		return ""
	}

	// Extract the filename and line number from the header
	header := parts[0]
	line := parts[1]

	// lineNumber := strings.Split(header, ":")[1]
	// fmt.Printf("Line number: %s\n", lineNumber)

	lineParts := strings.Split(line, ",")

	// for _, part := range lineParts {
	// 	fmt.Printf("Part: %s\n", part)
	// }

	// checking for that abnormal case
	category := lineParts[8]
	if len(lineParts) > 20 {
		category = lineParts[9]

	}
	// fmt.Printf("Category: %s\n", category)
	ret += header + "," + category
	return ret
}

// main function which gets executed
func main() {
	tuple := os.Args[1]
	// pattern := os.Args[2]
	// fmt.Printf("Tuple: %s\n", tuple)
	// start a periodic printing of the counts
	// go startPeriodicPrint()

	// call the counting function
	category := count(tuple, "")

	fmt.Print(category)

}
