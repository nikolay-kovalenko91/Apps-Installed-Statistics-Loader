package main

import (
	"flag"
	"log"
	"os"

	"github.com/nikolay-kovalenko91/Apps-Installed-Statistics-Loader/processing"
	"github.com/nikolay-kovalenko91/Apps-Installed-Statistics-Loader/store"
)

func processFlags(args []string) *processing.Config {
	flagSet := flag.NewFlagSet(args[0], flag.ExitOnError)

	var cfg processing.Config

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

func runLoader(cfg *processing.Config) {
	if cfg.LogFilePath != "" {
		f, err := os.OpenFile(cfg.LogFilePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			log.Fatalf("Error opening file: %v\n", err)
		}
		defer f.Close()

		log.SetOutput(f)
	}

	var connPoolManager *store.ConnPool
	if !cfg.IsRunDry {
		connPoolManager = store.GetConnPoolManager(cfg.StoreHostByDevType)
	}

	log.Println("Started...")

	processing.ProcessFilesMatched(cfg.FilesPattern, connPoolManager)

	log.Println("Finished!")
}

func main() {
	cfg := processFlags(os.Args)

	runLoader(cfg)
}
