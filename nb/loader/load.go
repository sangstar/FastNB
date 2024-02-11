package loader

// TODO: Use a CLI flag for this
// TODO: Refactor the errors

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
)

type Dataset struct {
	Features [][][]int32
	Classes  []uint16
}

type FreqCount struct {
	CountMap map[string]int
}

type FileLoader struct {
	file *os.File
}

type FreqCounts []FreqCount

const filePath string = "../data.csv"
const ChunkSize int = 3

func NewFileLoader(filePath string) (*FileLoader, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	return &FileLoader{file: file}, nil
}

func (fl *FileLoader) Close() error {
	return fl.file.Close()
}

func (fl *FileLoader) SeekStart() error {
	_, err := fl.file.Seek(0, 0) // Seek to the start of the file
	return err
}

func (fl *FileLoader) NewReader() *csv.Reader {
	return csv.NewReader(fl.file)
}

func (fl *FileLoader) getNumLines() (NumLines int, err error) {
	defer func() {
		err := fl.SeekStart()
		if err != nil {
			log.Fatal(err)
		}
	}()

	var i = 0

	reader := fl.NewReader()

	for {
		_, err := reader.Read()
		if err == io.EOF {
			return i, nil
		} else if err != nil {
			return 0, err
		} else {
			i += 1
		}
	}
}

// ReadChunk returns a slice of a Dataset of chunk size `size`.
// Assumes dataset is in CSV format, and that features and classes
// are defined in a specific way
func (dataset *Dataset) ReadChunk(dataFile *FileLoader, ChunkSize int, Pos int) (Dataset, error) {
	var features [][][]int32
	var classes []uint16

	defer func() {
		err := dataFile.SeekStart()
		if err != nil {
			log.Fatal(err)
		}
	}()

	reader := dataFile.NewReader()

	for i := 0; i < Pos; i++ {
		_, err := reader.Read()
		if err != nil {
			return Dataset{}, err
		}
	}

	for i := 0; i < ChunkSize; i++ {
		var feature [][]int32
		record, err := reader.Read()
		log.Println("Read record", record)
		if err != nil {
			if err == io.EOF {
				log.Println("Features at EOF", features)
				return Dataset{Features: features, Classes: classes}, err
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
		features = append(features, feature)
		classes = append(classes, uint16(class))
	}

	return Dataset{features, classes}, nil
}

// Load loads the dataset from a CSV file and reads a chunk of it.
func Load() (FreqCount, error) {
	var freqCounts FreqCounts
	var wg sync.WaitGroup

	dataFile, err := NewFileLoader(filePath)
	if err != nil {
		log.Fatal(err)
	}

	numLines, err := dataFile.getNumLines()
	if err != nil {
		log.Fatal(err)
	}
	log.Println(numLines)

	remainder := numLines % ChunkSize

	var numWorkers int
	if remainder == 0 {
		numWorkers = numLines / ChunkSize
	} else {
		numWorkers = numLines/ChunkSize + remainder
	}

	Data := Dataset{}
	countsChan := make(chan FreqCount, numWorkers)

	wg.Add(numWorkers)

	for i := 0; i < numWorkers; i++ {
		chunkSize := ChunkSize
		if i == numWorkers-1 && remainder != 0 {
			chunkSize = remainder
		}
		loadedChunk, err := Data.ReadChunk(dataFile, chunkSize, i*ChunkSize)
		if err != nil && err != io.EOF {
			log.Fatal(err)
		}
		go func(worker int, dataChunk Dataset) {
			defer wg.Done()
			counts, err := dataChunk.ToCounts()
			if err != nil {
				log.Fatal(err)
			}
			countsChan <- counts
		}(i, loadedChunk)
	}

	go func() {
		wg.Wait()
		close(countsChan)
	}()

	freqCounts = FreqCounts{}
	for count := range countsChan {
		freqCounts = append(freqCounts, count)
	}

	err = dataFile.Close()
	if err != nil {
		log.Fatal(err)
	}

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

	log.Println(dataset)
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
// FreqCount with all the counts summed up per feature
func (freqCounts FreqCounts) Combine() (FreqCount, error) {
	if len(freqCounts) == 0 {
		return FreqCount{}, errors.New("no frequency counts to combine")
	}

	result := FreqCount{CountMap: make(map[string]int)}

	for _, freqCount := range freqCounts {
		for word, count := range freqCount.CountMap {
			result.CountMap[word] += count
		}
	}

	return result, nil
}
