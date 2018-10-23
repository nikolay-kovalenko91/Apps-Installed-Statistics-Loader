package main

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
)

var files_pattern = "./sandbox/*.tsv.gz"

func readFile(path string) <-chan string {
	output := make(chan string)
	go func() {
		file, err := os.Open(path)
		if err != nil {
			log.Fatal(err)
		}
		defer file.Close()

		// TODO: how to pass differnt unpackers as an argument?
		unpacked, err := gzip.NewReader(file)
		if err != nil {
			log.Fatal(err)
		}
		defer unpacked.Close()

		reader := bufio.NewReader(unpacked)
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				if err == io.EOF {
					break
				} else {
					log.Fatal(err)
				}
			}
			output <- line
		}
		close(output)
	}()

	return output
}

func main() {
	files_matches, err := filepath.Glob(files_pattern)
	if err != nil {
		log.Fatal(err)
	}

	for _, file_name := range files_matches {
		for line := range readFile(file_name) {
			fmt.Printf(line)
		}
		fmt.Println()
	}
}

// Todo: Parse tsv string to get data
// Todo: Load it to Redis
// Todo: Pass options to the script
// Todo: Count errors while files processing
// Todo: Implement reconnect / timeouts for Redis
// Todo: Apply concurrency
