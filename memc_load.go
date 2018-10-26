package main

import (
	"bufio"
	"compress/gzip"
	"errors"
	"fmt"
	"github.com/mediocregopher/radix.v2/pool"
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

func readFile(path string) (<-chan string, <-chan error, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}

	// TODO: how to pass differnt unpackers as an argument?
	unpacked, err := gzip.NewReader(file)
	if err != nil {
		return nil, nil, err
	}

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
		defer file.Close()
		defer unpacked.Close()
	}()

	return linesRead, readingErrs, nil
}

func parseRecord(line string) (*AppInstalled, error) {
	appInstalled := new(AppInstalled)
	lineParts := strings.Split(line, "\t")

	appInstalledValue := reflect.ValueOf(appInstalled).Elem()

	fieldsNumber := appInstalledValue.NumField()
	if len(lineParts) != fieldsNumber {
		msg := fmt.Sprintf("Can not parse string: %s", line)
		return appInstalled, errors.New(msg)
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

	appsAsStrings := strings.Replace(lineParts[4], "\n", "", -1)
	appsListAsStrings := strings.Split(appsAsStrings, ",")
	var appsList []int
	for _, appAsString := range appsListAsStrings {
		appId, err := strconv.Atoi(appAsString)
		appsList = append(appsList, appId)
		if err != nil {
			msg := fmt.Sprintf("Can not parse string with wrong App IDs: %s", line)
			return appInstalled, errors.New(msg)
		}
	}
	appInstalled.apps = appsList

	return appInstalled, nil
}

func insertRecord(db_pool *pool.Pool, appInstalled *AppInstalled, dryRun bool) error {
	conn, err := db_pool.Get()
	if err != nil {
		return err
	}
	defer db_pool.Put(conn)

	if dryRun {
		log.Printf("%+v\n", appInstalled)

		// For testing DB interuction purposes
		key := fmt.Sprintf("%s:%s", appInstalled.devType, appInstalled.devId)
		resp := conn.Cmd("SET", key, appInstalled)
		if resp.Err != nil {
			log.Fatal(resp.Err)
		}
		respT1 := conn.Cmd("GET", key)
		fmt.Println("Got from DB", respT1)
		conn.Cmd("DEL", key)
	} else {
		key := fmt.Sprintf("%s:%s", appInstalled.devType, appInstalled.devId)
		resp := conn.Cmd("SET", key, appInstalled)
		if resp.Err != nil {
			return resp.Err
		}
	}

	return nil
}

func handleFile(dbPool *pool.Pool, filesNames []string, dryRun bool) {
	for _, fileName := range filesNames {
		lines, readingErrs, err := readFile(fileName)
		if err != nil {
			log.Fatal(err)
			continue
		}
		for {
			select {
			case line, ok := <-lines:
				if ok {
					appInstalled, err := parseRecord(line)
					if err != nil {
						log.Printf("Can not convert line into an inner entity %s: %s", fileName, err)
					}

					err = insertRecord(dbPool, appInstalled, dryRun)
					if err != nil {
						log.Printf("Can not save inner inner entity into DB: %s", err)
					}
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

	dbPool, err := pool.New("tcp", "localhost:6379", 10)
	if err != nil {
		log.Panic(err)
	}

	dryRun := true
	handleFile(dbPool, filesMatches, dryRun)
}

// Todo: Pass options to the script
// Todo: Count errors while files processing
// Todo: Implement reconnect / timeouts for Redis
// Todo: Apply concurrency
