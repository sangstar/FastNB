package loader

// TODO: Use a CLI flag for this
// TODO: Refactor the errors

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"log"
	"os"
	"strconv"
	"io"
	"fmt"
	"strings"
)

var ChunkSize int = 5

type Dataset struct {
	Features [][][]int32
	Classes  []uint16
}

type FreqCount struct {
	CountMap map[string]int
}

type FreqCounts []FreqCount

// ReadChunk returns a slice of a Dataset of chunk size `size`.
// Assumes dataset is in CSV format, and that features and classes
// are defined in a specific way
func (dataset *Dataset) ReadChunk(reader *csv.Reader, ChunkSize int) (Dataset, error) {
	var features [][][]int32
	var classes []uint16

	for i := 0; i < ChunkSize; i++ {
		var feature [][]int32
		record, err := reader.Read()
		log.Println("Read record ", record)
		if err != nil {
			if err == io.EOF {
				log.Println("Features at EOF", features)
				return Dataset{features, classes}, err
			}
			log.Println(err)
			return Dataset{}, err
		}
		strFeature, strClass := record[0], record[1]

		err = json.Unmarshal([]byte(strFeature), &feature)
		if err != nil {
			log.Println(err)
			return Dataset{}, err
		}
		class, err := strconv.Atoi(strClass)
		if err != nil {
			log.Println(err)
			return Dataset{}, err
		}
		log.Println("Appending to features:", feature)
		features = append(features, feature)
		classes = append(classes, uint16(class))
		log.Println("Feature array is now ", features)
	}

	return Dataset{features, classes}, nil
}

// Load loads the dataset from a CSV file and reads a chunk of it.
func Load() (FreqCount, error) {
	var dataChunk Dataset = Dataset{}

	file, err := os.Open("../data.csv")
	if err != nil {
		log.Fatal(err)
	}
	reader := csv.NewReader(file)

	var freqCounts FreqCounts
	for {
		dataChunk, readErr := dataChunk.ReadChunk(reader, ChunkSize)
		if (readErr != nil && readErr != io.EOF) {
			log.Fatal(err)
		}
		log.Println("Instantiated dataChunk", dataChunk)
		Counts, err := dataChunk.ToCounts()
		log.Println("Counts are ", Counts)
		if err != nil {
			log.Fatal(err)
		}
		freqCounts = append(freqCounts, Counts)
		if readErr == io.EOF {
			break
		}
	}
	log.Println("freqCounts: ", freqCounts)

	Combined, err := freqCounts.Combine()
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Combined is ", Combined)
	return Combined, nil
}


const separator = "|" // Signifying "such that", such as P(w|c)

// ToCounts converts a Dataset into a FreqCount, which gives the
// counts of features per class
func (dataset *Dataset) ToCounts() (FreqCount, error) {
	mapper := make(map[string]int)

	for i, class := range dataset.Classes {
		sClass := fmt.Sprintf("%d", class)
		for _, featureList := range dataset.Features[i] {
			for _, feature := range featureList {
				sFeature := fmt.Sprintf("%d", feature)
				mapper[strings.Join([]string{sClass, sFeature}, separator)]++
				mapper[sClass]++
			}
		}
	}

	return FreqCount{CountMap: mapper}, nil
}

// Combine concatenates a FreqCounts struct into a single
// FreqCount with all of the counts summed up per feature
func (freqCounts FreqCounts) Combine() (FreqCount, error) {
	if len(freqCounts) == 0 {
		return FreqCount{}, errors.New("No frequency counts to combine")
	}

	result := FreqCount{CountMap: make(map[string]int)}

	for _, freqCount := range freqCounts {
		for word, count := range freqCount.CountMap {
			result.CountMap[word] += count
		}
	}

	return result, nil
}
