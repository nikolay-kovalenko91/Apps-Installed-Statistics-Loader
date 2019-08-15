package store

import (
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/mediocregopher/radix.v2/pool"
	"github.com/mediocregopher/radix.v2/redis"
)

const second = 1000 // milliseconds

const dbConnectionPoolSize = 15
const dbConnectionTimeout = 2.5 * second
const dbConnectionMaxRetry = 5
const dbConnectionRetryStartTimeout = 0.2 * second

// Record represents a raw data to save onto store
type Record struct {
	Key  string
	Body []byte
}

// ConnPool is for managing connection pools
// for stores used(each for every App DevType
type ConnPool struct {
	connPoolByDeviceType map[string]*pool.Pool
}

// Init inits connection pools manager
func (sp *ConnPool) Init(storeHostByDevType map[string]*string, poolSize int, dialFunc pool.DialFunc) error {
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

// GetConnPoolManager is abstraction to init connection pool manage
func GetConnPoolManager(storeHostByDevType map[string]*string) *ConnPool {
	connPoolManager := new(ConnPool)
	connPoolManager.Init(storeHostByDevType, dbConnectionPoolSize, applyReconnect(dial))

	return connPoolManager
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

// GetByDeviceType gives a connection pool for exact App DevType Store
func (sp *ConnPool) GetByDeviceType(devType string) (*pool.Pool, error) {
	pool, ok := sp.connPoolByDeviceType[devType]
	if !ok {
		errMessage := fmt.Sprintf("Can't get pool for devType=%s", devType)
		return nil, errors.New(errMessage)
	}

	return pool, nil
}

// SaveRecord is for sending a record into store
func SaveRecord(connPoolManager *ConnPool, devType string, record *Record) error {
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
