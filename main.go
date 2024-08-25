package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"sort"
	"strings"
	"testing"
	"time"
)

type CityTemperatures struct {
	Name string
	Stats
}

var cityData map[string]*CityTemperatures

type Stats struct {
	Min   float64
	Max   float64
	Sum   float64
	Count int
}

const (
	POOLS                 = 10
	ROWS                  = 1000000000
	MAX_ROWS              = 1000000000
	CACHE_THRESHOLD       = 250000000
	INITIAL_TEMP_CAPACITY = 100000
)

func main() {
	// Start CPU profiling here
	f, err := os.Create("cpu.prof")
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	if err := pprof.StartCPUProfile(f); err != nil {
		log.Fatal(err)
	}
	defer pprof.StopCPUProfile()

	cityData = make(map[string]*CityTemperatures)

	cacheFile := "measurements_data_cache.gob"
	clearCache(cacheFile)

	startTime := time.Now()

	_, err = os.Stat(cacheFile)
	if os.IsNotExist(err) {
		fmt.Println("cache file not found, processing file...")
		readFile("measurements.txt")
		if MAX_ROWS <= CACHE_THRESHOLD {
			fmt.Println("saving cache...")
			saveCache(cacheFile)
		} else {
			fmt.Println("cache threshold not reached, skipping cache save")
		}
	} else {
		fmt.Println("cache file found, loading cache...")
		loadCache(cacheFile)
	}

	// cityStats = make(map[string]Stats)
	cities := make([]string, 0, len(cityData))
	for cityName := range cityData {
		cities = append(cities, cityName)
	}
	sort.Strings(cities) // length: 413 cities

	// var wg sync.WaitGroup

	// citiesPerPool := len(cities) / POOLS

	// for pool := 0; pool < POOLS; pool++ {
	// 	wg.Add(1)
	// 	go func(poolIndex int) {
	// 		defer wg.Done()
	// 		start := poolIndex * citiesPerPool
	// 		end := start + citiesPerPool
	// 		if poolIndex == POOLS-1 {
	// 			end = len(cities)
	// 		}
	// 		for i := start; i < end; i++ {
	// 			cityName := cities[i]
	// 			data := cityData[cityName]
	// 			updateStats(data)
	// 		}
	// 	}(pool)
	// }

	// wg.Wait()

	result := "{"
	for i, cityName := range cities {

		result += fmt.Sprintf("%s=%.1f/%.1f/%.1f", cityName, cityData[cityName].Min, cityData[cityName].Sum/float64(cityData[cityName].Count), cityData[cityName].Max)
		if i < len(cities)-1 {
			result += ", "
		}
	}
	result += "}"

	fmt.Println(result)

	endTime := time.Now()
	duration := endTime.Sub(startTime)

	fmt.Printf("Pools: %d, Execution time: %v\n", POOLS, duration)

	// Memory profiling
	f, err = os.Create("mem.prof")
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	runtime.GC() // get up-to-date statistics
	if err := pprof.WriteHeapProfile(f); err != nil {
		log.Fatal(err)
	}
}

func readFile(filename string) {
	file, err := os.Open(filename)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer file.Close()

	buffer := make([]byte, 1024*1024*64)
	leftover := []byte{}

	for {
		n, err := file.Read(buffer)

		if err != nil && err != io.EOF {
			fmt.Println(err)
			return
		}
		chunk := append(leftover, buffer[:n]...)
		lines := bytes.Split(chunk, []byte("\n"))

		for i := 0; i < len(lines)-1; i++ {
			processLine(string(lines[i]))
		}

		leftover = lines[len(lines)-1]

		if err == io.EOF {
			if len(leftover) > 0 {
				processLine(string(leftover))
			}
			break
		}
	}

}

func saveCache(filename string) {
	file, err := os.Create(filename)
	if err != nil {
		fmt.Println("error creating cache file: ", err)
		return
	}
	defer file.Close()

	encoder := gob.NewEncoder(file)
	encoder.Encode(cityData)

	if err != nil {
		fmt.Println("error encoding cache file: ", err)
	}
}

func loadCache(filename string) {
	file, err := os.Open(filename)
	if err != nil {
		fmt.Println("error opening cache file: ", err)
		return
	}
	defer file.Close()

	decoder := gob.NewDecoder(file)
	decoder.Decode(&cityData)
}

func clearCache(filename string) {
	os.Remove(filename)
	fmt.Println("cache file removed")
}

func fastParseFloat(s string) (float64, error) {
	if len(s) == 0 {
		return 0, fmt.Errorf("empty string")
	}

	var result float64
	var negative bool
	var decimal bool
	var decimalPlaces float64 = 1

	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '-':
			if i != 0 {
				return 0, fmt.Errorf("misplaced minus sign")
			}
			negative = true
		case '.':
			if decimal {
				return 0, fmt.Errorf("multiple decimal points")
			}
			decimal = true
		case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
			// convert ascii digit to float64 by subtracting the ascii value of '0'.
			// this works because ascii digits are represented by consecutive byte values:
			// '0' = 48, '1' = 49, '2' = 50, etc.
			// example: for s[i] = '7' (ascii 55), '7' - '0' = 55 - 48 = 7
			digit := float64(s[i] - '0')
			if decimal {
				decimalPlaces *= 10
				result += digit / decimalPlaces
			} else {
				result = result*10 + digit
			}
		default:
			return 0, fmt.Errorf("invalid character: %c", s[i])
		}
	}

	if negative {
		result = -result
	}

	return result, nil
}

func processLine(line string) {
	colonIndex := strings.IndexByte(line, ';')
	if colonIndex == -1 {
		return
	}
	cityName := line[:colonIndex]
	temperature, err := fastParseFloat(line[colonIndex+1:])
	if err != nil {
		return
	}

	city, exists := cityData[cityName]

	if !exists {
		cityData[cityName] = &CityTemperatures{
			Name: cityName,
			Stats: Stats{
				Min:   temperature,
				Max:   temperature,
				Sum:   temperature,
				Count: 1,
			},
		}

	} else {
		if temperature < city.Min {
			city.Min = temperature
		}
		if temperature > city.Max {
			city.Max = temperature
		}
		city.Sum += temperature
		city.Count++
	}
}

func BenchmarkProcessFile(b *testing.B) {
	for i := 0; i < b.N; i++ {
		readFile("measurements.txt")
	}
}
