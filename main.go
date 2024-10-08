package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/playforest/1brc_go/utils"
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
	cpuProfilePath := "profiler/cpu.prof"
	memProfilePath := "profiler/mem.prof"
	// Start CPU profiling
	if err := utils.StartCPUProfiling(cpuProfilePath); err != nil {
		log.Printf("Failed to start CPU profiling: %v", err)
		return
	}
	defer utils.StopCPUProfiling()

	startTime := time.Now()

	cityData = make(map[string]*CityTemperatures)
	readFile("measurements.txt")

	// cityStats = make(map[string]Stats)
	cities := make([]string, 0, len(cityData))
	for cityName := range cityData {
		cities = append(cities, cityName)
	}
	sort.Strings(cities) // length: 413 cities

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

	utils.WriteMemoryProfile(memProfilePath)
}

func readFile(filename string) {
	file, err := os.Open(filename)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer file.Close()

	buffer := make([]byte, 2048*2048)
	leftover := []byte{}

	for {
		n, err := file.Read(buffer)
		if err == io.EOF {
			if len(leftover) > 0 {
				processLine(string(leftover))
			}
			break
		}
		if err != nil {
			panic(err)
		}

		chunk := append(leftover, buffer[:n]...)
		lines := bytes.Split(chunk, []byte("\n"))

		for i := 0; i < len(lines)-1; i++ {
			processLine(string(lines[i]))
		}

		leftover = lines[len(lines)-1]

	}

}

func processLine(line string) {
	colonIndex := fastIndexOfByte(line, ";")
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

func fastIndexOfByte(s string, c string) int {
	if len(c) != 1 {
		fmt.Errorf("c must be a single letter character")
		return -1
	}

	target := c[0]

	for i := 0; i < len(s); i++ {
		if s[i] == target {
			return i
		}
	}
	return -1
}

func BenchmarkProcessFile(b *testing.B) {
	for i := 0; i < b.N; i++ {
		readFile("measurements.txt")
	}
}
