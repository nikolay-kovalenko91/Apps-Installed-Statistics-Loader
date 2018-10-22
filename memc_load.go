package main

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
)

var gz_path = "./sandbox/sample.txt.gz"
var folder_path = "./sandbox"

func getDirFiles(path string) []string {
	dir, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	defer dir.Close()

	fileInfos, err := dir.Readdir(-1)
	if err != nil {
		log.Fatal(err)
	}

	var filesNames []string
	for _, fileInfo := range fileInfos {
		filesNames = append(filesNames, fileInfo.Name())
	}

	return filesNames
}

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
	for _, line := range getDirFiles(folder_path) {
		// TODO: use regexp here
		if strings.Contains(line, "tsv") {
			fmt.Println(line)
		}
	}

	for line := range readFile(gz_path) {
		fmt.Println(line)
	}
}
