package main

import (
	"./appinstalled"
	"bufio"
	"compress/gzip"
	"errors"
	"flag"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/mediocregopher/radix.v2/pool"
	"github.com/mediocregopher/radix.v2/redis"
	"io"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
)

const normalErrorRate = 0.01

const dbConnectionPoolSize = 15
const dbConnectionTimeout = 2500 // milliseconds, 1s = 1000ms
const dbConnectionMaxRetry = 5
const dbConnectionRetryStartTimeout = 200 // milliseconds, 1s = 1000ms

type AppInstalled struct {
	devType string
	devId   string
	lat     float64
	lon     float64
	apps    []uint32
}

type Record struct {
	key  string
	body []byte
}

type Sender struct {
	toSend     chan<- *Record
	errsOutput <-chan error
}

func readFile(path string) (<-chan string, <-chan error, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}

	// TODO: how to pass different unpackers as an argument?
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

	appInstalled.devType = lineParts[0]
	appInstalled.devId = lineParts[1]
	if appInstalled.devType == "" || appInstalled.devId == "" {
		msg := fmt.Sprintf("Either devId or devType is empty: %s", line)
		return appInstalled, errors.New(msg)
	}

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
	var appsList []uint32
	for _, appAsString := range appsListAsStrings {
		appId, err := strconv.Atoi(appAsString)
		appsList = append(appsList, uint32(appId))
		if err != nil {
			msg := fmt.Sprintf("Can not parse string with wrong App IDs: %s", line)
			return appInstalled, errors.New(msg)
		}
	}
	appInstalled.apps = appsList

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

// TODO: how to create a generic decorator for Pool/connect creation, not only Cmd func?
func applyCmdReconnect(f func(cmd string, args ...interface{}) *redis.Resp) func(cmd string, args ...interface{}) *redis.Resp {
	return func(cmd string, args ...interface{}) *redis.Resp {
		var retryCount uint
		var resp *redis.Resp
		timeout := dbConnectionRetryStartTimeout

		for retryCount < dbConnectionMaxRetry {
			resp = f(cmd, args)
			if resp.Err == nil {
				break
			}

			time.Sleep(time.Duration(timeout) * time.Millisecond)
			timeout *= 2
			retryCount++
		}

		return resp
	}
}

func getRecordSender(pool *pool.Pool) *Sender {
	toSend := make(chan *Record)
	errsOutput := make(chan error)
	sender := Sender{toSend: toSend, errsOutput: errsOutput}
	go func() {
		conn, err := pool.Get()
		if err != nil {
			errsOutput <- err
		}
		defer pool.Put(conn)

		record := <-toSend
		resp := applyCmdReconnect(conn.Cmd)("SET", record.key, record.body)
		if resp.Err != nil {
			errsOutput <- resp.Err
		}
	}()

	return &sender
}

func composeRecord(appInstalled *AppInstalled) (*Record, error) {
	userApp := appinstalled.UserApps{
		Apps: appInstalled.apps,
		Lat:  appInstalled.lat,
		Lon:  appInstalled.lon,
	}

	messagePacked, err := proto.Marshal(&userApp)
	if err != nil {
		return nil, err
	}

	key := fmt.Sprintf("%s:%s", appInstalled.devType, appInstalled.devId)

	record := Record{key: key, body: messagePacked}

	return &record, nil
}

func insertRecord(sender *Sender, record *Record, isRunDry bool) error {
	select {
	case err := <-sender.errsOutput:
		return err
	case sender.toSend <- record:
	}

	return nil
}

func handleLine(line string, dbPools map[string]*pool.Pool, isRunDry bool) (processed, processingErrors uint) {
	appInstalled, err := parseRecord(line)
	if err != nil {
		log.Printf("Can not convert line into an inner entity %s: %s", line, err)
		processingErrors++
		return processed, processingErrors
	}

	record, err := composeRecord(appInstalled)
	if err != nil {
		log.Printf("Can not compress line %s: %s", line, err)
		processingErrors++
		return processed, processingErrors
	}

	if isRunDry {
		log.Printf("%s -> %s", record.key, strings.Replace(string(record.body), "\n", " ", -1))
	} else {
		dbPool, ok := dbPools[appInstalled.devType]
		if !ok {
			log.Printf("Unknown device type: %s", appInstalled.devType)
			processingErrors++
			return processed, processingErrors
		}
		sender := getRecordSender(dbPool)

		err = insertRecord(sender, record, isRunDry)
		if err != nil {
			log.Printf("Error sending record: %s", err)
			processingErrors++
			return processed, processingErrors
		}
	}

	processed++
	return processed, processingErrors
}

func handleFile(waitGroup *sync.WaitGroup, dbPools map[string]*pool.Pool, filePath string, isRunDry bool) {
	defer waitGroup.Done()

	lines, readingErrs, err := readFile(filePath)
	if err != nil {
		log.Printf("Can not open file %s: %s\n", filePath, err)
		return
	}

	var processed, processingErrors uint
	for {
		select {
		case line, ok := <-lines:
			if ok {
				processed, processingErrors = handleLine(line, dbPools, isRunDry)
			} else {
				lines = nil
			}
		case err, ok := <-readingErrs:
			if ok {
				log.Printf("Error reading file %s: %s\n", filePath, err)
				processingErrors++
			} else {
				readingErrs = nil
			}
		}

		if lines == nil && readingErrs == nil {
			break
		}
	}

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

func startLoading(filesPattern string, deviceIdVsDBHost map[string]*string, isRunDry bool) {
	deviceIdVsDBPool := make(map[string]*pool.Pool)

	for deviceId, host := range deviceIdVsDBHost {
		var dbPool *pool.Pool

		if !isRunDry {
			dialFunc := func(network, addr string) (*redis.Client, error) {
				client, err := redis.DialTimeout(network, addr, time.Duration(dbConnectionTimeout)*time.Millisecond)
				if err != nil {
					return nil, err
				}

				return client, nil
			}

			newDbPool, err := pool.NewCustom("tcp", *host, dbConnectionPoolSize, dialFunc)
			if err != nil {
				log.Panic(err)
			}
			dbPool = newDbPool
		}

		deviceIdVsDBPool[deviceId] = dbPool
	}

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
		go handleFile(&waitGroup, deviceIdVsDBPool, path, isRunDry)
	}

	waitGroup.Wait()
}

func main() {
	filesPattern := flag.String("pattern", "./input_files/*.tsv.gz", "File pattern, a string")
	isRunDry := flag.Bool("dry", false, "Run without saving to DB, a bool")
	logFilePath := flag.String("log", "", "Path to a log file, a string")
	if *logFilePath != "" {
		f, err := os.OpenFile(*logFilePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			log.Fatalf("error opening file: %v\n", err)
		}
		defer f.Close()

		log.SetOutput(f)
	}

	deviceIdVsDBHost := map[string]*string{
		"idfa": flag.String("idfa", "127.0.0.1:33013", "IDFA device DB host, a string"),
		"gaid": flag.String("gaid", "127.0.0.1:33014", "GAID device DB host, a string"),
		"adid": flag.String("adid", "127.0.0.1:33015", "ADID device DB host, a string"),
		"dvid": flag.String("dvid", "127.0.0.1:33016", "DVID device DB host, a string"),
	}

	flag.Parse()

	log.Println("Started...")
	startLoading(*filesPattern, deviceIdVsDBHost, *isRunDry)
	log.Println("Finished!")
}
