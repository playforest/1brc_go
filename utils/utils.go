package utils

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"time"
)

func SaveCache(filename string, data interface{}) {
	file, err := os.Create(filename)
	if err != nil {
		fmt.Println("error creating cache file: ", err)
		return
	}
	defer file.Close()

	encoder := gob.NewEncoder(file)
	err = encoder.Encode(data)

	if err != nil {
		fmt.Println("error encoding cache file: ", err)
	}
}

func LoadCache(filename string, data interface{}) {
	file, err := os.Open(filename)
	if err != nil {
		fmt.Println("error opening cache file: ", err)
		return
	}
	defer file.Close()

	decoder := gob.NewDecoder(file)
	err = decoder.Decode(data)
	if err != nil {
		fmt.Println("error decoding cache file: ", err)
	}
}

func ClearCache(filename string) {
	os.Remove(filename)
	fmt.Println("cache file removed")
}

var cpuFile *os.File

func StartCPUProfiling(filename string) error {
	var err error
	cpuFile, err = os.Create(filename)
	if err != nil {
		return fmt.Errorf("could not create CPU profile: %v", err)
	}
	if err := pprof.StartCPUProfile(cpuFile); err != nil {
		cpuFile.Close()
		return fmt.Errorf("could not start CPU profile: %v", err)
	}
	return nil
}

func StopCPUProfiling() {
	pprof.StopCPUProfile()
	if cpuFile != nil {
		cpuFile.Close()
		cpuFile = nil
	}
}

func WriteMemoryProfile(filename string) {
	f, err := os.Create(filename)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	runtime.GC() // get up-to-date statistics
	if err := pprof.WriteHeapProfile(f); err != nil {
		log.Fatal(err)
	}
}

func ExportTopFunctionsByTime(cpuProfilePath, outputDir string, topCount int) {
	timestamp := time.Now().Format("2006-01-02_15-04-05")
	outputFileName := fmt.Sprintf("top_%d_functions_by_time_%s.txt", topCount, timestamp)
	outputPath := filepath.Join(outputDir, outputFileName)

	// Ensure the output directory exists
	if err := os.MkdirAll(outputDir, os.ModePerm); err != nil {
		log.Printf("Error creating directory: %v", err)
		return
	}

	cmd := exec.Command("go", "tool", "pprof", "-top", "-cum", fmt.Sprintf("-nodecount=%d", topCount), cpuProfilePath)

	// Capture both stdout and stderr
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	if err != nil {
		log.Printf("Error running pprof: %v", err)
		log.Printf("Stderr: %s", stderr.String())
		return
	}

	output := stdout.Bytes()
	err = os.WriteFile(outputPath, output, 0644)
	if err != nil {
		log.Printf("Error writing to file: %v", err)
		return
	}

	fmt.Printf("Top %d functions by time exported to %s\n", topCount, outputPath)

	// Print the output to console as well
	fmt.Println(string(output))
}
