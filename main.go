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
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

type CityTemperatures struct {
	Name         string
	Temperatures []float64
}

var cityData map[string]*CityTemperatures

type Stats struct {
	Min  float64
	Mean float64
	Max  float64
}

var cityStats sync.Map

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

	var wg sync.WaitGroup

	citiesPerPool := len(cities) / POOLS

	for pool := 0; pool < POOLS; pool++ {
		wg.Add(1)
		go func(poolIndex int) {
			defer wg.Done()
			start := poolIndex * citiesPerPool
			end := start + citiesPerPool
			if poolIndex == POOLS-1 {
				end = len(cities)
			}
			for i := start; i < end; i++ {
				cityName := cities[i]
				data := cityData[cityName]
				updateStats(data)
			}
		}(pool)
	}

	wg.Wait()

	result := "{"
	for i, cityName := range cities {
		stats, _ := cityStats.Load(cityName)
		typedStats := stats.(Stats)
		result += fmt.Sprintf("%s=%.1f/%.1f/%.1f", cityName, typedStats.Min, typedStats.Mean, typedStats.Max)
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

func updateStats(cityTemp *CityTemperatures) {
	temps := cityTemp.Temperatures
	if len(temps) == 0 {
		cityStats.Store(cityTemp.Name, Stats{Min: 0, Mean: 0, Max: 0})
		return
	}

	min, max, sum := temps[0], temps[0], temps[0]
	for _, temp := range temps[1:] {
		if temp < min {
			min = temp
		}
		if temp > max {
			max = temp
		}
		sum += temp
	}
	mean := sum / float64(len(temps))

	cityStats.Store(cityTemp.Name, Stats{
		Min:  min,
		Mean: mean,
		Max:  max,
	})

}

func processLine(line string) {
	// parts := strings.Split(line, ";")

	// if len(parts) != 2 {
	// 	return
	// }

	// cityName := parts[0]
	// temperature, err := strconv.ParseFloat(parts[1], 64)

	colonIndex := strings.IndexByte(line, ';')
	if colonIndex == -1 {
		return
	}
	cityName := line[:colonIndex]
	temperature, err := strconv.ParseFloat(line[colonIndex+1:], 64)
	if err != nil {
		return
	}

	city, exists := cityData[cityName]

	if !exists {
		cityData[cityName] = &CityTemperatures{
			Name:         cityName,
			Temperatures: make([]float64, 0, INITIAL_TEMP_CAPACITY),
		}
		city = cityData[cityName]
	} else {
		temps := city.Temperatures
		if len(temps) == cap(temps) {
			newTemps := make([]float64, len(temps), cap(temps)*2)
			copy(newTemps, temps)
			city.Temperatures = newTemps
			temps = newTemps
		}

		// temps = temps[:len(temps)+1]
		// temps[len(temps)-1] = temperature
		// city.Temperatures = temps
	}
	city.Temperatures = append(city.Temperatures, temperature)
}

func BenchmarkProcessFile(b *testing.B) {
	for i := 0; i < b.N; i++ {
		readFile("measurements.txt")
	}
}
