package processor

import (
	"bufio"
	"io"
	"sort"
	"strings"
	"sync"
	"unicode"
)

type WordFreq struct {
	Word  string
	Count int
}

func tokenize(text string) []string {
	var words []string
	var current strings.Builder

	for _, r := range text {
		if unicode.IsLetter(r) {
			current.WriteRune(unicode.ToLower(r))
		} else if current.Len() > 0 {
			words = append(words, current.String())
			current.Reset()
		}
	}

	if current.Len() > 0 {
		words = append(words, current.String())
	}

	return words
}

func mapWords(chunk []byte) map[string]int {
	text := string(chunk)
	words := tokenize(text)

	freq := make(map[string]int)

	for _, word := range words {
		if len(word) > 0 {
			freq[word]++
		}
	}

	return freq
}

func readChunks(file io.Reader, chunkSize int, out chan<- []byte, wg *sync.WaitGroup) {
	defer wg.Done()

	reader := bufio.NewReader(file)

	for {
		chunk := make([]byte, chunkSize)

		n, err := reader.Read(chunk)

		if n > 0 {
			out <- chunk[:n]
		}

		if err != nil {
			break
		}
	}
}

func reduceWordCounts(in <-chan map[string]int, done chan<- map[string]int) {
	finalMap := make(map[string]int)

	for freqMap := range in {
		for word, count := range freqMap {
			finalMap[word] += count
		}
	}

	done <- finalMap
}

func getTopWords(freqMap map[string]int, n int) []WordFreq {
	words := make([]WordFreq, 0, len(freqMap))
	for word, count := range freqMap {
		words = append(words, WordFreq{Word: word, Count: count})
	}

	sort.Slice(words, func(i, j int) bool {
		if words[i].Count == words[j].Count {
			return words[i].Word < words[j].Word
		}
		return words[i].Count > words[j].Count
	})

	if n > len(words) {
		n = len(words)
	}
	return words[:n]
}

func ProcessFile(file io.ReadCloser, chunkSize int, topN int) ([]WordFreq, error) {
	defer file.Close()

	chunkChan := make(chan []byte, 100)
	freqChan := make(chan map[string]int, 100)
	doneChan := make(chan map[string]int, 1)

	var wg sync.WaitGroup

	wg.Add(1)
	go readChunks(file, chunkSize, chunkChan, &wg)

	numMappers := 4
	var mapperWg sync.WaitGroup

	for workerID := 0; workerID < numMappers; workerID++ {
		mapperWg.Add(1)

		go func(mapperID int) {
			defer mapperWg.Done()
			for chunk := range chunkChan {
				freqMap := mapWords(chunk)
				if len(freqMap) > 0 {
					freqChan <- freqMap
				}
			}
		}(workerID)
	}

	go reduceWordCounts(freqChan, doneChan)

	go func() {
		wg.Wait()
		close(chunkChan)
	}()

	go func() {
		mapperWg.Wait()
		close(freqChan)
	}()

	finalFreqMap := <-doneChan
	topWords := getTopWords(finalFreqMap, topN)
	return topWords, nil
}
