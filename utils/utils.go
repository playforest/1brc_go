package utils

import (
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

func StartCPUProfiling(filename string) {
	f, err := os.Create(filename)
	if err != nil {
		log.Fatal(err)
	}
	if err := pprof.StartCPUProfile(f); err != nil {
		log.Fatal(err)
	}
}

func StopCPUProfiling() {
	pprof.StopCPUProfile()
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
	output, err := cmd.Output()
	if err != nil {
		log.Printf("Error running pprof: %v", err)
		return
	}

	err = os.WriteFile(outputPath, output, 0644)
	if err != nil {
		log.Printf("Error writing to file: %v", err)
		return
	}

	fmt.Printf("Top %d functions by time exported to %s\n", topCount, outputPath)
}
