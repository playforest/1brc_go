package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"sync"
	"time"
)

// Function to read a file synchronously and return file size and number of lines
func readFileSync(filename string) (int64, int) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("Failed to open file: %v", err)
	}
	defer file.Close()

	// Get file size
	fileInfo, err := file.Stat()
	if err != nil {
		log.Fatalf("Failed to get file info: %v", err)
	}
	fileSize := fileInfo.Size()

	// Count lines
	scanner := bufio.NewScanner(file)
	lineCount := 0
	for scanner.Scan() {
		lineCount++
	}
	if err := scanner.Err(); err != nil {
		log.Fatalf("Error reading file: %v", err)
	}

	return fileSize, lineCount
}

// Function to read a file using a specified number of goroutines
func readFileConcurrently(filename string, numGoroutines int) (int64, int) {
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	var fileSize int64
	var lineCount int

	// We only need to calculate the size and lines once, hence it's done in one of the goroutines
	var once sync.Once

	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			once.Do(func() {
				fileSize, lineCount = readFileSync(filename)
			})
		}()
	}

	wg.Wait()
	return fileSize, lineCount
}

func main() {
	filename := "../measurements.txt"

	// Synchronous read
	start := time.Now()
	fileSize, lineCount := readFileSync(filename)
	fmt.Printf("Synchronous Read:\n")
	fmt.Printf("Time taken: %v\n", time.Since(start))
	fmt.Printf("File Size: %d bytes, Number of Lines: %d\n", fileSize, lineCount)

	// Concurrent reads with 2 goroutines
	start = time.Now()
	fileSize, lineCount = readFileConcurrently(filename, 2)
	fmt.Printf("\nConcurrent Read with 2 Goroutines:\n")
	fmt.Printf("Time taken: %v\n", time.Since(start))
	fmt.Printf("File Size: %d bytes, Number of Lines: %d\n", fileSize, lineCount)

	// Concurrent reads with 4 goroutines
	start = time.Now()
	fileSize, lineCount = readFileConcurrently(filename, 4)
	fmt.Printf("\nConcurrent Read with 4 Goroutines:\n")
	fmt.Printf("Time taken: %v\n", time.Since(start))
	fmt.Printf("File Size: %d bytes, Number of Lines: %d\n", fileSize, lineCount)

	// Concurrent reads with 10 goroutines
	start = time.Now()
	fileSize, lineCount = readFileConcurrently(filename, 10)
	fmt.Printf("\nConcurrent Read with 10 Goroutines:\n")
	fmt.Printf("Time taken: %v\n", time.Since(start))
	fmt.Printf("File Size: %d bytes, Number of Lines: %d\n", fileSize, lineCount)
}
