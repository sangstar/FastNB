package loader

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"log"
	"os"
	"strconv"
)

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
func (dataset *Dataset) ReadChunk() (Dataset, error) {
	var features [][][]int32
	var classes []uint16
	var chunkSize int = 5 // TODO: Put this somewhere where it can be set by the user

	file, err := os.Open("../data.csv")
	if err != nil {
		log.Fatal(err)
	}
	reader := csv.NewReader(file)
	for i := 0; i < chunkSize; i++ {
		var feature [][]int32
		record, err := reader.Read()
		log.Println("Read record ", record)
		if err != nil {
			log.Fatal(err)
			return Dataset{}, err
		}
		strFeature, strClass := record[0], record[1]

		err = json.Unmarshal([]byte(strFeature), &feature)
		if err != nil {
			log.Fatal(err)
			return Dataset{}, err
		}
		class, err := strconv.Atoi(strClass)
		if err != nil {
			log.Fatal(err)
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

	// TODO: Use a CLI flag for this

	// TODO: Refactor the errors

	// For each ChunkSize..
	// 1. Read ChunkSize lines. If there's less lines than a chunk,
	// read that many lines instead
	// 2. Instantiate a Dataset object from those lines
	// 3. Get the freq counts of feature per class
	// 4. Send to channel
	// 5. Add together any freq counts any time the channel
	// encounters more than 1 at a time. It needs to be some
	// data type that can be added count-wise.

	// Each time a tokenId is encountered it checks if HashMap
	// has its tokenId as a key. If it does, it appends to it.
	// If not, it creates a new one and stores the freq count

	var freqCounts FreqCounts
	for i := 0; i < 5; i++ { // TODO: What should the last value for i be?
		dataChunk, err := dataChunk.ReadChunk()
		log.Println("Instantiated dataChunk", dataChunk)
		if err != nil {
			return FreqCount{}, err
		}
		Counts, err := dataChunk.ToCounts()
		log.Println("Counts are ", Counts)
		if err != nil {
			log.Fatal(err)
		}
		freqCounts = append(freqCounts, Counts)
	}
	log.Println("freqCounts: ", freqCounts)

	Combined, err := freqCounts.Combine()
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Combined is ", Combined)
	return Combined, nil
}

func (dataset *Dataset) ToCounts() (FreqCount, error) {
	mapper := make(map[string]int)
	log.Println(dataset.Classes)
	for i := 0; i < len(dataset.Classes); i++ {
		log.Println(dataset.Classes[i])
		for _, featureList := range dataset.Features[i] {
			for _, feature := range featureList {
				var label string = strconv.Itoa(int(dataset.Classes[i])) + "-" + strconv.Itoa(int(feature))
				mapper[label] += 1
			}

		}
	}
	return FreqCount{CountMap: mapper}, nil
}

func (freqCounts FreqCounts) Combine() (FreqCount, error) {
	if len(freqCounts) == 0 {
		return FreqCount{}, errors.New("no frequency counts to combine")
	}

	result := FreqCount{CountMap: make(map[string]int)}

	for _, freqCount := range freqCounts {
		for word, _ := range freqCount.CountMap {
			result.CountMap[word] += freqCount.CountMap[word]
		}
	}

	return result, nil
}
