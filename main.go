package main

import (
	"fmt"
	"log"
	"os"

	"Freq_Counter/processor"
)

const (
	chunkSize = 64 * 1024 // 64 КБ
	topN      = 10
)

func RunFromCLI() {
	if len(os.Args) < 2 {
		log.Fatalln("Usage: go run main.go <file>")
	}

	path := os.Args[1]
	file, err := os.Open(path)
	if err != nil {
		log.Fatalln("Error:", err)
	}
	defer file.Close()

	top, err := processor.ProcessFile(file, chunkSize, topN)
	if err != nil {
		log.Fatalln("Error:", err)
	}

	fmt.Println("Top words:")
	for _, wf := range top {
		fmt.Printf("%s: %d", wf.Word, wf.Count)
	}
}

func main() {
	RunFromCLI()
}
