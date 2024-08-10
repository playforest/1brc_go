package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type CityTemperatures struct {
	Name         string
	Temperatures []float64
}

type CityStats struct {
	Name string
	Min  float64
	Mean float64
	Max  float64
}

func main() {
	startTime := time.Now()

	file, err := os.Open("measurements.txt")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer file.Close()

	cityData := make(map[string]*CityTemperatures)

	scanner := bufio.NewScanner(file)
	// lineCount := 0
	for scanner.Scan() {
		line := scanner.Text()
		// fmt.Println(line)
		processLine(line, cityData)
		// lineCount++
	}

	if err := scanner.Err(); err != nil {
		fmt.Println("error reading file: ", err)
	}

	fmt.Print("{")
	for cityName, data := range cityData {
		min := min(data.Temperatures)
		mean := mean(data.Temperatures)
		max := max(data.Temperatures)
		fmt.Printf("%s=%.1f/%.1f/%.1f, ", cityName, min, mean, max)
	}
	fmt.Print("}")

	endTime := time.Now()
	duration := endTime.Sub(startTime)

	fmt.Printf("Execution time: %v\n", duration)
}

// func sortCityData(cityData map[string]*CityTemperatures) []CityData) {
// 	var cities []CityData
// 	for name, data := range cityData {
// 		cities = append(cities, CityData{Name: name, Data: data })
// 	}

// 	sort.Slice(cities, func(i, j int) bool {
// 		return cities[i].Name < cities[j].Name
// 	})

// 	return cities
// }

func updateStats(cityTemp *CityTemperatures, cityStats *CityStats) {
	cityStats.Name = cityTemp.Name
	cityStats.Min = min(cityTemp.Temperatures)
	cityStats.Mean = mean(cityTemp.Temperatures)
	cityStats.Max = max(cityTemp.Temperatures)
}

func min(temps []float64) float64 {
	if len(temps) == 0 {
		return 0
	}
	minTemp := temps[0]
	for _, temp := range temps[1:] {
		if temp < minTemp {
			minTemp = temp
		}
	}
	return minTemp
}

func sum(temps []float64) float64 {
	total := 0.0
	for _, v := range temps {
		total += v
	}

	return total
}

func mean(temps []float64) float64 {
	var mean float64
	if len(temps) > 0 {
		mean = sum(temps) / float64(len(temps))
	} else {
		mean = 0.
	}
	return mean
}

func max(temps []float64) float64 {
	if len(temps) == 0 {
		return 0
	}
	maxTemp := temps[0]
	for _, temp := range temps[1:] {
		if temp > maxTemp {
			maxTemp = temp
		}
	}
	return maxTemp
}

func processLine(line string, cityData map[string]*CityTemperatures) {
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
