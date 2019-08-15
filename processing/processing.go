package processing

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
	"sync"

	"../appinstalled"
	"../store"
	"github.com/golang/protobuf/proto"
)

const normalErrorRate = 0.01

// Config represents options that the processing can be started with
type Config struct {
	FilesPattern       string
	LogFilePath        string
	IsRunDry           bool
	StoreHostByDevType map[string]*string
}

// AppInstalled is an inner representation of AppInstalled data fetched from outer sources
type AppInstalled struct {
	DevType string
	DevId   string
	Lat     float64
	Lon     float64
	Apps    []uint32
}

func readFile(path string) (<-chan string, <-chan error, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}

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

	appInstalled.DevType = lineParts[0]
	appInstalled.DevId = lineParts[1]
	if appInstalled.DevType == "" || appInstalled.DevId == "" {
		msg := fmt.Sprintf("Either devId or devType is empty: %s", line)
		return appInstalled, errors.New(msg)
	}

	lat, err := strconv.ParseFloat(lineParts[2], 32)
	if err != nil {
		return appInstalled, err
	}
	appInstalled.Lat = lat
	lon, err := strconv.ParseFloat(lineParts[3], 32)
	if err != nil {
		return appInstalled, err
	}
	appInstalled.Lon = lon

	appsAsStrings := strings.Replace(lineParts[4], "\n", "", -1)
	appsListAsStrings := strings.Split(appsAsStrings, ",")
	var appsList []uint32
	for _, appAsString := range appsListAsStrings {
		appID, err := strconv.Atoi(appAsString)
		appsList = append(appsList, uint32(appID))
		if err != nil {
			msg := fmt.Sprintf("Can not parse string with wrong App IDs: %s", line)
			return appInstalled, errors.New(msg)
		}
	}
	appInstalled.Apps = appsList

	return appInstalled, nil
}

func renameWithDot(filePath string) {
	dir, fileName := filepath.Split(filePath)
	newfileName := fmt.Sprintf(".%s", fileName)

	err := os.Rename(filePath, filepath.Join(dir, newfileName))
	if err != nil {
		log.Fatalf("Can not rename file %s: %s", filePath, err)
	}
}

func composeRecord(appInstalled *AppInstalled) (*store.Record, error) {
	userApp := appinstalled.UserApps{
		Apps: appInstalled.Apps,
		Lat:  appInstalled.Lat,
		Lon:  appInstalled.Lon,
	}

	messagePacked, err := proto.Marshal(&userApp)
	if err != nil {
		return nil, err
	}

	key := fmt.Sprintf("%s:%s", appInstalled.DevType, appInstalled.DevId)

	record := store.Record{Key: key, Body: messagePacked}

	return &record, nil
}

func processLine(line string, connPoolManager *store.ConnPool) error {
	appInstalled, err := parseRecord(line)
	if err != nil {
		errMessage := fmt.Sprintf("Can not convert line into an inner entity %s: %s", line, err)

		return errors.New(errMessage)
	}

	record, err := composeRecord(appInstalled)
	if err != nil {
		errMessage := fmt.Sprintf("Can not compress line %s: %s", line, err)

		return errors.New(errMessage)
	}

	err = store.SaveRecord(connPoolManager, appInstalled.DevType, record)
	if err != nil {
		return err
	}

	return nil
}

func processLines(lines <-chan string, readingErrs <-chan error,
	connPoolManager *store.ConnPool) (uint, uint) {
	var processed, processingErrors uint
	for {
		select {
		case line, ok := <-lines:
			if ok {
				err := processLine(line, connPoolManager)
				if err != nil {
					processingErrors++

					log.Printf("Error sending line: error=%s; line=%s\n", err, line)
				}
			} else {
				lines = nil
			}

		case err, ok := <-readingErrs:
			if ok {
				processingErrors++

				log.Printf("File reading error: %s\n", err)
			} else {
				readingErrs = nil
			}
		}

		if lines == nil && readingErrs == nil {
			break
		}
	}

	return processed, processingErrors
}

func processFile(waitGroup *sync.WaitGroup, connPoolManager *store.ConnPool, filePath string) {
	defer waitGroup.Done()

	lines, readingErrs, err := readFile(filePath)
	if err != nil {
		log.Printf("Can not open file %s: %s\n", filePath, err)

		return
	}

	processed, processingErrors := processLines(lines, readingErrs, connPoolManager)

	if processed != 0 {
		errRate := float32(processingErrors) / float32(processed)
		if errRate < normalErrorRate {
			log.Printf("Acceptable error rate (%.2f). Successfull load\n", errRate)
		} else {
			log.Printf("High error rate (%.2f > %.2f). Failed load\n", errRate, normalErrorRate)
		}
	}

	renameWithDot(filePath)
}

// ProcessFilesMatched starts processing for files with regex pattern name matched
func ProcessFilesMatched(filesPattern string, connPoolManager *store.ConnPool) {
	filesMatches, err := filepath.Glob(filesPattern)
	if err != nil {
		log.Fatal(err)
	}

	var waitGroup sync.WaitGroup
	for _, path := range filesMatches {
		if strings.HasPrefix(filepath.Base(path), ".") {
			continue
		}

		waitGroup.Add(1)
		go processFile(&waitGroup, connPoolManager, path)
	}

	waitGroup.Wait()
}
