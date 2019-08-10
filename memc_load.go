package main

import (
	"bufio"
	"compress/gzip"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"./appinstalled"
	"github.com/golang/protobuf/proto"
	"github.com/mediocregopher/radix.v2/pool"
	"github.com/mediocregopher/radix.v2/redis"
)

const second = 1000 // milliseconds

const normalErrorRate = 0.01

const dbConnectionPoolSize = 15
const dbConnectionTimeout = 2.5 * second
const dbConnectionMaxRetry = 5
const dbConnectionRetryStartTimeout = 0.2 * second

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

type StoreConnPool struct {
	connPoolByDeviceType map[string]*pool.Pool
}

func (sp *StoreConnPool) Init(storeHostByDevType map[string]*string, poolSize int, dialFunc pool.DialFunc) error {
	sp.connPoolByDeviceType = make(map[string]*pool.Pool)

	for devType, host := range storeHostByDevType {
		newConnPool, err := pool.NewCustom("tcp", *host, poolSize, dialFunc)
		if err != nil {
			return err
		}

		sp.connPoolByDeviceType[devType] = newConnPool
	}

	return nil
}

func (sp *StoreConnPool) GetByDeviceType(devType string) (*pool.Pool, error) {
	pool, ok := sp.connPoolByDeviceType[devType]
	if !ok {
		err_message := fmt.Sprintf("Can't get pool for deviceId=%d", devType)
		return nil, errors.New(err_message)
	}

	return pool, nil
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

func storeExecuteCmd(pool *pool.Pool, record *Record, command string) error {
	conn, err := pool.Get()
	if err != nil {
		return err
	}
	defer pool.Put(conn)

	resp := conn.Cmd(command, record.key, record.body)
	if resp.Err != nil {
		return resp.Err
	}

	return nil
}

func insertRecord(pool *pool.Pool, record *Record) error {
	return storeExecuteCmd(pool, record, "SET")
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

func handleLine(line string, connPoolManager *StoreConnPool) (processed, processingErrors uint) {
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

	if connPoolManager == nil {
		log.Printf("DryRun: %s -> %s", record.key, strings.Replace(string(record.body), "\n", " ", -1))
	} else {
		connPool, err := connPoolManager.GetByDeviceType(appInstalled.devType)
		if err != nil {
			log.Printf("Unknown device type: %s", appInstalled.devType)
			processingErrors++

			return processed, processingErrors
		}

		err = insertRecord(connPool, record)
		if err != nil {
			log.Printf("Error sending record: %s", err)
			processingErrors++

			return processed, processingErrors
		}
	}

	processed++

	return processed, processingErrors
}

func handleFile(waitGroup *sync.WaitGroup, connPoolManager *StoreConnPool, filePath string) {
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
				processed, processingErrors = handleLine(line, connPoolManager)
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

func startForMatched(filesPattern string, connPoolManager *StoreConnPool) {
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
		go handleFile(&waitGroup, connPoolManager, path)
	}

	waitGroup.Wait()
}

func applyReconnect(f pool.DialFunc) pool.DialFunc {
	return func(network, addr string) (*redis.Client, error) {
		var retryCount uint
		var client *redis.Client
		var err error
		timeout := dbConnectionRetryStartTimeout

		for retryCount < dbConnectionMaxRetry {
			client, err = f(network, addr)
			if err == nil {
				break
			}

			log.Printf("Trying to reconnect, attempt=%d, err=%s", retryCount, err)

			time.Sleep(time.Duration(timeout) * time.Millisecond)
			timeout *= 2
			retryCount++
		}

		return client, nil
	}
}

func dial(network, addr string) (*redis.Client, error) {
	timeout := time.Duration(dbConnectionTimeout) * time.Millisecond
	client, err := redis.DialTimeout(network, addr, timeout)
	if err != nil {
		return nil, err
	}

	return client, nil
}

func main() {
	filesPattern := flag.String("pattern", "./input_files/*.tsv.gz", "File pattern, a string")
	isRunDry := flag.Bool("dry", false, "Run without saving to store, a bool")
	logFilePath := flag.String("log", "", "Path to a log file, a string")
	if *logFilePath != "" {
		f, err := os.OpenFile(*logFilePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			log.Fatalf("error opening file: %v\n", err)
		}
		defer f.Close()

		log.SetOutput(f)
	}

	storeHostByDevType := map[string]*string{
		"idfa": flag.String("idfa", "127.0.0.1:33013", "IDFA device store host, a string"),
		"gaid": flag.String("gaid", "127.0.0.1:33014", "GAID device store host, a string"),
		"adid": flag.String("adid", "127.0.0.1:33015", "ADID device store host, a string"),
		"dvid": flag.String("dvid", "127.0.0.1:33016", "DVID device store host, a string"),
	}

	flag.Parse()

	var connPoolManager *StoreConnPool
	if !*isRunDry {
		connPoolManager = new(StoreConnPool)
		connPoolManager.Init(storeHostByDevType, dbConnectionPoolSize, applyReconnect(dial))
	}

	log.Println("Started...")
	startForMatched(*filesPattern, connPoolManager)

	log.Println("Finished!")
}

// TODO: Parse config params in separate func
// TODO: 262-283 to handleLines func
// TODO: 231-248 to sendRecord func?
