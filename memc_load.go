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

type Config struct {
	FilesPattern       string
	LogFilePath        string
	IsRunDry           bool
	StoreHostByDevType map[string]*string
}

type AppInstalled struct {
	DevType string
	DevId   string
	Lat     float64
	Lon     float64
	Apps    []uint32
}

type Record struct {
	Key  string
	Body []byte
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
		errMessage := fmt.Sprintf("Can't get pool for deviceId=%d", devType)
		return nil, errors.New(errMessage)
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

func storeExecuteCmd(pool *pool.Pool, record *Record, command string) error {
	conn, err := pool.Get()
	if err != nil {
		return err
	}
	defer pool.Put(conn)

	resp := conn.Cmd(command, record.Key, record.Body)
	if resp.Err != nil {
		return resp.Err
	}

	return nil
}

func storeExecuteSet(pool *pool.Pool, record *Record) error {
	return storeExecuteCmd(pool, record, "SET")
}

func saveRecord(connPoolManager *StoreConnPool, devType string, record *Record) error {
	if connPoolManager == nil {
		log.Printf("DryRun: %s -> %s", record.Key, strings.Replace(string(record.Body), "\n", " ", -1))

	} else {
		connPool, err := connPoolManager.GetByDeviceType(devType)
		if err != nil {
			errMessage := fmt.Sprintf("Unknown device type: %s", devType)

			return errors.New(errMessage)
		}

		err = storeExecuteSet(connPool, record)
		if err != nil {
			errMessage := fmt.Sprintf("Error sending record: %s", err)

			return errors.New(errMessage)
		}
	}

	return nil
}

func composeRecord(appInstalled *AppInstalled) (*Record, error) {
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

	record := Record{Key: key, Body: messagePacked}

	return &record, nil
}

func processLine(line string, connPoolManager *StoreConnPool) error {
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

	err = saveRecord(connPoolManager, appInstalled.DevType, record)
	if err != nil {
		return err
	}

	return nil
}

func processLines(lines <-chan string, readingErrs <-chan error, connPoolManager *StoreConnPool) (uint, uint) {
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

func processFile(waitGroup *sync.WaitGroup, connPoolManager *StoreConnPool, filePath string) {
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

func processFilesMatched(filesPattern string, connPoolManager *StoreConnPool) {
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

func processFlags(args []string) *Config {
	flagSet := flag.NewFlagSet(args[0], flag.ExitOnError)

	var cfg Config

	flagSet.StringVar(&cfg.FilesPattern, "pattern", "./input_files/*.tsv.gz", "File pattern, a string")
	flagSet.BoolVar(&cfg.IsRunDry, "dry", false, "Run without saving to store, a bool")
	flagSet.StringVar(&cfg.LogFilePath, "log", "", "Path to a log file, a string")

	cfg.StoreHostByDevType = map[string]*string{
		"idfa": flagSet.String("idfa", "127.0.0.1:33013", "IDFA device store host, a string"),
		"gaid": flagSet.String("gaid", "127.0.0.1:33014", "GAID device store host, a string"),
		"adid": flagSet.String("adid", "127.0.0.1:33015", "ADID device store host, a string"),
		"dvid": flagSet.String("dvid", "127.0.0.1:33016", "DVID device store host, a string"),
	}

	flagSet.Parse(args[1:])

	return &cfg
}

func runLoader(cfg *Config) {
	if cfg.LogFilePath != "" {
		f, err := os.OpenFile(cfg.LogFilePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			log.Fatalf("Error opening file: %v\n", err)
		}
		defer f.Close()

		log.SetOutput(f)
	}

	var connPoolManager *StoreConnPool
	if !cfg.IsRunDry {
		connPoolManager = new(StoreConnPool)
		connPoolManager.Init(cfg.StoreHostByDevType, dbConnectionPoolSize, applyReconnect(dial))
	}

	log.Println("Started...")

	processFilesMatched(cfg.FilesPattern, connPoolManager)

	log.Println("Finished!")
}

func main() {
	cfg := processFlags(os.Args)

	runLoader(cfg)
}
