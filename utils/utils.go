package utils

import (
	"encoding/gob"
	"fmt"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
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
