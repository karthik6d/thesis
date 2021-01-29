package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"time"
)

type Entry struct {
	key   int32
	value int32
}

type QueryType int

const (
	ReadQuery QueryType = iota
	WriteQuery
)

type Result struct {
	isPresent bool
	value     int32
}

type Query struct {
	queryType QueryType
	key       int32
	value     int32
}

func (q Query) String() string {
	switch q.queryType {
	case ReadQuery:
		return fmt.Sprintf("read %d\n", q.key)

	case WriteQuery:
		return fmt.Sprintf("write %d %d\n", q.key, q.value)
	}

	return ""
}

func (e Entry) String() string {
	return fmt.Sprintf("%d,%d\n", e.key, e.value)
}

type KeyDistribution int
type ValueDistribution int

const (
	KeyUniform KeyDistribution = iota
	// KeyNormal
	KeySequential
)

const (
	ValueUniform ValueDistribution = iota
	ValueSame
)

var entriesSet = make(map[int32]bool)
var entries = make([]Entry, 0)

func uniformKey(i int) int32 {
	return rand.Int31()
}

func sequentialKey(i int) int32 {
	return int32(2 * i)
}

func normalKey(i int) int32 {
	return int32((rand.NormFloat64()*math.MaxInt32/16) + math.MaxInt32/2)
}

func uniformValue(key int32) int32 {
	return int32(rand.Uint32())
}

func sameValue(key int32) int32 {
	return key + 1
}

var keyDistributions = map[KeyDistribution](func(int) int32){0: uniformKey, 1: sequentialKey, 2: normalKey}
var valueDistributions = map[ValueDistribution](func(int32) int32){0: uniformValue, 1: sameValue}

func main() {
	// seeding the random number generator
	rand.Seed(time.Now().UnixNano())

	// parse options for the generator
	N := flag.Int("N", int(math.Pow(10, 7)), "the number of entries to start the db with")
	numQueries := flag.Int("queries", int(math.Pow(10, 7)), "the number of queries to generate")
	keyDistribution := flag.Int("keyDistribution", 0, "the distribution of the keys; 0 for uniform, 1 for sequential, 2 for normal")
	valueDistribution := flag.Int("valueDistribution", 0, "the distribution of the values; 0 for uniform, 1 for same as key")
	selectivity := flag.Int("selectivity", 100, "the selectivity of reads; integer 0 to 100")
	dataFileName := flag.String("dataFile", "data.csv", "the file to write data to")
	queryFileName := flag.String("queryFile", "queries.dsl", "the file to write queries to")
	expectedFileName := flag.String("expectedFile", "test.exp", "the file to write expected results to")
	readPercentageFlag := flag.Int("readPercentage", 50, "the percent of queries that are reads; integer 0 to 100")
	flag.Parse()

	if *N > math.MaxInt32 {
		panic(errors.New("Too many initial entries"))
	}

	f, err := os.Create(*dataFileName)
	if err != nil {
		panic(err)
	}

	defer f.Close()

	w := bufio.NewWriterSize(f, 10000000)

	keyFunc := keyDistributions[KeyDistribution(*keyDistribution)]
	valueFunc := valueDistributions[ValueDistribution(*valueDistribution)]
	selectivityPct := float64(*selectivity) / 100
	readPct := float64(*readPercentageFlag) / 100

	var keyCount int

	for keyCount = 0; keyCount < *N; keyCount++ {
		var key int32

		for key = keyFunc(keyCount); ; key = keyFunc(keyCount) {
			_, present := entriesSet[key]

			if !present {
				break
			}
		}

		value := valueFunc(key)
		entriesSet[key] = true

		entries = append(entries, Entry{key: key, value: value})
	}

	for _, v := range entries {
		w.WriteString(v.String())
	}

	w.Flush()

	queryFile, err := os.Create(*queryFileName)
	if err != nil {
		panic(err)
	}
	defer queryFile.Close()

	queryWriter := bufio.NewWriter(queryFile)

	queryWriter.WriteString("create db\n")
	queryWriter.WriteString(fmt.Sprintf("load %s\n", *dataFileName))

	queries := make([]Query, 0)

	expectedResults := make([]int32, 0)

	for i := 0; i < *numQueries; i++ {
		queryChoice := rand.Float64()

		if queryChoice < readPct {
			choose := rand.Float64()

			if choose < selectivityPct {
				randIdx := rand.Intn(len(entries))

				queries = append(queries, Query{
					ReadQuery,
					entries[randIdx].key,
					int32(0),
				})
				expectedResults = append(expectedResults, entries[randIdx].value)
			} else {
				var key int32
				for key = rand.Int31(); ; key = rand.Int31() {
					_, present := entriesSet[key]

					if !present {
						break
					}
				}

				queries = append(queries, Query{
					ReadQuery,
					key,
					int32(0),
				})
			}
		} else {
			key := keyFunc(keyCount)
			keyCount++
			value := valueFunc(key)

			entriesSet[key] = true
			entries = append(entries, Entry{key, value})

			queries = append(queries, Query{
				WriteQuery,
				key,
				value,
			})
			queries = append(queries, Query{
				ReadQuery,
				key,
				int32(0),
			})
			expectedResults = append(expectedResults, value)
		}
	}

	for _, v := range queries {
		queryWriter.WriteString(v.String())
	}

	queryWriter.Flush()

	expectedFile, err := os.Create(*expectedFileName)
	if err != nil {
		panic(err)
	}
	defer expectedFile.Close()

	expectedWriter := bufio.NewWriter(expectedFile)

	for _, v := range expectedResults {
		expectedWriter.WriteString(fmt.Sprintf("%d\n", v))
	}
	expectedWriter.Flush()
}
