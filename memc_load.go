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

func readFile(path string) (<-chan string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// TODO: how to pass differnt unpackers as an argument?
	unpacked, err := gzip.NewReader(file)
	if err != nil {
		return nil, err
	}
	defer unpacked.Close()

	output := make(chan string)
	errs := make(chan error)
	go func() {
		reader := bufio.NewReader(unpacked)
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				if err == io.EOF {
					break
				} else {
					errs <- err
				}
			}
			output <- line
		}
		close(errs)
		close(output)
	}()

	if err, open := <-errs; open {
		return nil, err
	}

	return output, nil
}

func parseTsvString(line string) (*AppInstalled, error) {
	appInstalled := new(AppInstalled)
	lineParts := strings.Split(line, "\t")

	appInstalledValue := reflect.ValueOf(appInstalled).Elem()

	fieldsNumber := appInstalledValue.NumField()
	if len(lineParts) != fieldsNumber {
		return appInstalled, errors.New("file string has wrong format: can not parse it")
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
		lines, err := readFile(fileName)
		if err != nil {
			log.Fatal(err)
			continue
		}

		for line := range lines {
			appInstalled, err := parseTsvString(line)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("%+v\n", appInstalled)
		}
		fmt.Println()
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
