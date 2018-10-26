package main

import (
	"bufio"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
)

var files_pattern = "./sandbox/*.tsv.gz"

type AppInstalled struct {
	devType string
	devId   string
	lat     float64
	lon     float64
	apps    []int
}

// TEST TOOL
func ToSlice(c chan interface{}) []interface{} {
	s := make([]interface{}, 0)
	for i := range c {
		s = append(s, i)
	}
	return s
}

func readFile(path string) (<-chan string, <-chan error, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	defer file.Close()

	// TODO: how to pass differnt unpackers as an argument?
	unpacked, err := gzip.NewReader(file)
	if err != nil {
		return nil, nil, err
	}
	defer unpacked.Close()

	linesRead := make(chan string)
	readingErrs := make(chan error)
	go func() {
		reader := bufio.NewReader(unpacked)
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				if err == io.EOF {
					break
				} else {
					readingErrs <- err
				}
			}
			linesRead <- line
		}
		close(readingErrs)
		close(linesRead)
	}()

	return linesRead, readingErrs, nil
}

func parseTsvString(line string) (*AppInstalled, error) {
	appInstalled := new(AppInstalled)
	lineParts := strings.Split(line, "\t")

	appInstalledValue := reflect.ValueOf(appInstalled).Elem()

	fieldsNumber := appInstalledValue.NumField()
	if len(lineParts) != fieldsNumber {
		return appInstalled, errors.New("File string has wrong format: can not parse it")
	}

	// TODO: parse it with assigning automatically
	appInstalled.devType = lineParts[0]
	appInstalled.devId = lineParts[1]
	lat, err := strconv.ParseFloat(lineParts[2], 32)
	if err != nil {
		return appInstalled, err
	}
	appInstalled.lat = lat
	lon, err := strconv.ParseFloat(lineParts[3], 32)
	if err != nil {
		return appInstalled, err
	}
	appInstalled.lon = lon

	return appInstalled, nil
}

func handleFile(filesNames []string) {
	for _, fileName := range filesNames {
		fmt.Println("REDING", fileName)
		lines, readingErrs, err := readFile(fileName)
		if err != nil {
			log.Fatal(err)
			continue
		}
		for {
			select {
			case line, ok := <-lines:
				if ok {
					fmt.Println(line)
					//appInstalled, err := parseTsvString(line)
					//if err != nil {
					//	log.Fatal(err)
					//}
					//fmt.Printf("%+v\n", appInstalled)
				} else {
					lines = nil
				}
			case err, ok := <-readingErrs:
				if ok {
					log.Printf("Error reading file %s: %s", fileName, err)
				} else {
					readingErrs = nil
				}
			}

			if lines == nil && readingErrs == nil {
				break
			}
		}
	}
}

func main() {
	filesMatches, err := filepath.Glob(files_pattern)
	if err != nil {
		log.Fatal(err)
	}

	handleFile(filesMatches)
}

// Todo: Load it to Redis
// Todo: Pass options to the script
// Todo: Count errors while files processing
// Todo: Implement reconnect / timeouts for Redis
// Todo: Apply concurrency
