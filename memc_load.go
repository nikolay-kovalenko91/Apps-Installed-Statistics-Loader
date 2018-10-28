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

func insertRecord(dbPools map[string]*pool.Pool, appInstalled *AppInstalled, dryRun bool) error {
	userApp := appinstalled.UserApps{
		Apps: appInstalled.apps,
		Lat:  appInstalled.lat,
		Lon:  appInstalled.lon,
	}

	messagePacked, err := proto.Marshal(&userApp)
	if err != nil {
		return err
	}

	key := fmt.Sprintf("%s:%s", appInstalled.devType, appInstalled.devId)

	if dryRun {
		log.Printf("%s -> %s", key, strings.Replace(string(messagePacked), "\n", " ", -1))
	} else {
		dbPool, ok := dbPools[appInstalled.devType]
		if !ok {
			msg := fmt.Sprintf("Unknown device type: %s", appInstalled.devType)
			return errors.New(msg)
		}

		conn, err := dbPool.Get()
		if err != nil {
			return err
		}
		defer dbPool.Put(conn)

		resp := applyCmdReconnect(conn.Cmd)("SET", key, messagePacked)
		if resp.Err != nil {
			return resp.Err
		}

		//For testing DB interuction purposes TODO: delete it
		//fmt.Println("userApp is ", userApp)
		//fmt.Println("Packed is ", messagePacked)
		//
		//respT1 := conn.Cmd("GET", key)
		//fmt.Println("Got from DB", respT1)
		//conn.Cmd("DEL", key)
		//userAppUnpacked := &appinstalled.UserApps{}
		//respT1Bytes, _ := respT1.Bytes()
		//if err := proto.Unmarshal(respT1Bytes, userAppUnpacked); err != nil {
		//	fmt.Println("Unpack ", err)
		//}
		//fmt.Println("userAppUnpacked is ", userAppUnpacked)
	}

	return nil
}

func handleFile(dbPools map[string]*pool.Pool, filePath string, dryRun bool) error {
	lines, readingErrs, err := readFile(filePath)
	if err != nil {
		return err
	}

	var processed, processingErrors uint
	for {
		select {
		case line, ok := <-lines:
			if ok {
				appInstalled, err := parseRecord(line)
				if err != nil {
					log.Printf("Can not convert line into an inner entity %s: %s", filePath, err)
					processingErrors++
					continue
				}

				err = insertRecord(dbPools, appInstalled, dryRun)
				if err != nil {
					log.Printf("Can not save inner inner entity into DB: %s\n", err)
					processingErrors++
				}
				processed++

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

	return nil
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

	for _, path := range filesMatches {
		if strings.HasPrefix(filepath.Base(path), ".") {
			continue
		}

		err := handleFile(deviceIdVsDBPool, path, isRunDry)
		if err != nil {
			log.Printf("Can not parse file %s: %s\n", path, err)
		}
	}
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

// Todo: Apply concurrency
