package main

import (
	"bufio"
	"encoding/gob"
	"fmt"
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
	POOLS           = 30
	ROWS            = 1000000000
	MAX_ROWS        = 1000000000
	CACHE_THRESHOLD = 250000000
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
		processFile("measurements.txt")
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

func processFile(filename string) {
	file, err := os.Open(filename)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	rowCount := 0
	for scanner.Scan() && rowCount < MAX_ROWS {
		line := scanner.Text()
		processLine(line)
		rowCount++
	}

	if err := scanner.Err(); err != nil {
		fmt.Println("error reading file: ", err)
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
	parts := strings.Split(line, ";")

	if len(parts) != 2 {
		return
	}

	cityName := parts[0]
	temperature, err := strconv.ParseFloat(parts[1], 64)

	if err != nil {
		return
	}

	_, exists := cityData[cityName]

	if !exists {
		cityData[cityName] = &CityTemperatures{
			Name:         cityName,
			Temperatures: []float64{},
		}
	} else {
		cityData[cityName].Temperatures = append(cityData[cityName].Temperatures, temperature)
	}
}

func BenchmarkProcessFile(b *testing.B) {
	for i := 0; i < b.N; i++ {
		processFile("measurements.txt")
	}
}
