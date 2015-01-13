// pst is a command line tool for processing and combining columns across
// column oriented files
package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"unicode"
)

const version = "0.1"

// command line switches
var (
	inputSpec    string
	outputSpec   string
	inputSep     string
	outputSep    string
	rowSpec      string
	computeStats bool
	showHelp     bool
)

// parseSpec describes for each input files which columns to parse
type parseSpec []int

func init() {
	flag.StringVar(&inputSpec, "e", "",
		`specify the input columns to extract.
     The spec format is "<column list file1>|<column list file2>|..."
     where each column specifier is of the form col_i,col_j,col_k-col_n, ....
     If the number of specifiers is less than the number of files, the last
     specifier i will be applied to files i through N, where N is the total
     number of files provided.`)
	flag.BoolVar(&computeStats, "c", false,
		`compute statistics across column values in each output row.
     Please note that each value in the output has to be convertible into a float
     for this to work. Currently the mean and standard deviation are computed.`)
	flag.StringVar(&inputSep, "i", "",
		`column separator for input files. The default separator is whitespace.`)
	flag.StringVar(&outputSep, "o", " ",
		`column separator for output files. The default separator is a single space.`)
	flag.BoolVar(&showHelp, "h", false, "show basic usage info")
	flag.StringVar(&outputSpec, "p", "",
		`specify the order in which to paste the output columns.
     The spec format is "i,j,k,l,m,..", where 0 < i,j,k,l,m, ... < numCol, and
     numCol is the total number of columns extracted from the input files.
     Columns can be specified multiple times. If this option is not provided
     the columns are pasted in the order in which they are extracted.`)
	flag.StringVar(&rowSpec, "r", "",
		`specify which rows to process and output.
     This flag is optional. If not specified all rows will be output. Rows can
     be specified by a comma separated list of row IDs or row ID ranges. E.g.,
     "1,2,4-8,22"	will process rows 1, 2, 4, 5, 7, 22.`)
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	flag.Parse()
	if showHelp {
		usage()
		help()
		os.Exit(0)
	}

	if len(flag.Args()) < 1 || inputSpec == "" {
		usage()
		os.Exit(1)
	}
	fileNames := flag.Args()
	numFileNames := len(fileNames)

	inputSepFunc := getInputSepFunc(inputSep)

	// parse input column specs and pad with final element if we have more files
	// than provided spec entries
	inCols, err := parseInputSpec(inputSpec)
	if err != nil {
		log.Fatal(err)
	}
	if len(inCols) > numFileNames {
		log.Fatal("there are more per file column specifiers than supplied input files")
	}
	finalSpec := inCols[len(inCols)-1]
	pading := numFileNames - len(inCols)
	for i := 0; i <= pading; i++ {
		inCols = append(inCols, finalSpec)
	}

	// parse output column spec if requested
	var outCols parseSpec
	if outputSpec != "" {
		outCols, err = parseOutputSpec(outputSpec)
		if err != nil {
			log.Fatal(err)
		}
	}

	// parse row ranges to process
	var rowRanges rowRangeSlice
	if rowSpec != "" {
		rowRanges, err = parseRowSpec(rowSpec)
		if err != nil {
			log.Fatal(err)
		}
		sort.Sort(rowRanges)
	}

	err = parseData(fileNames, inCols, outCols, rowRanges, inputSepFunc, outputSep)
	if err != nil {
		log.Fatal(err)
	}
}

// parseData parses each of the data files provided on the command line in
// in a separate goroutine. The done channel used to signal each goroutine to
// shut down. The errCh channel signals any file opening/parsing issues back
// to the calling function.
func parseData(fileNames []string, inCols []parseSpec, outCols parseSpec,
	rowRanges []rowRange, inputSepFun func(rune) bool, outSep string) error {

	var wg sync.WaitGroup
	done := make(chan struct{})
	errCh := make(chan error, len(fileNames))
	defer close(errCh)

	var dataChs []chan string
	for i, name := range fileNames {
		dataCh := make(chan string)
		dataChs = append(dataChs, dataCh)
		go fileParser(name, inCols[i], rowRanges, inputSepFun, outputSep, dataCh,
			done, errCh, &wg)
	}

	var err error
	inRow := make([]string, len(dataChs))
	outRow := make([]string, len(outCols))
Loop:
	for {
		// process each data channel to read the column entries for the current row
		for i, ch := range dataChs {
			select {
			case c := <-ch:
				if c == "" {
					break Loop
				}
				inRow[i] = c
			case err = <-errCh:
				fmt.Println(err)
				break Loop
			}
		}

		// assemble output based on outCols if requested
		if len(outCols) == 0 {
			outRow = inRow
		} else {
			for i, c := range outCols {
				outRow[i] = inRow[c]
			}
		}

		if computeStats == true {
			items, err := splitIntoFloats(outRow)
			if err != nil {
				break Loop
			}
			fmt.Println(mean(items), variance(items))
		} else {
			fmt.Println(strings.Join(outRow, outSep))
		}
	}
	close(done)
	wg.Wait()

	return err
}

// fileParser opens fileName, parses it in a line by line fashion and sends
// the requested columns combined into a string down the data channel.
// If it receives on the done channel it stops processing and returns
func fileParser(fileName string, colSpec parseSpec, rowRanges rowRangeSlice,
	sepFun func(rune) bool, outSep string, data chan<- string, done <-chan struct{},
	errCh chan<- error, wg *sync.WaitGroup) {

	wg.Add(1)
	defer wg.Done()
	defer close(data)

	// open file
	file, err := os.Open(fileName)
	if err != nil {
		errCh <- err
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	count := -1
	maxRow := rowRanges.maxEntry()
	for scanner.Scan() {

		// logic for only printing requested rows
		count++
		if count > maxRow {
			break
		}
		if !rowRanges.contains(count) {
			continue
		}

		row := make([]string, len(colSpec))
		items := strings.FieldsFunc(strings.TrimSpace(scanner.Text()), sepFun)
		for i, c := range colSpec {
			if c >= len(items) {
				errCh <- fmt.Errorf("error parsing file %s: requested column %d "+
					"does not exist", fileName, c)
				return
			}
			row[i] = items[c]
		}

		select {
		case data <- strings.Join(row, outSep):
		case <-done:
			return
		}
	}
	return
}

// parseInputSpec parses the inputSpec and turns it into a slice of parseSpecs,
// one for each input file
func parseInputSpec(input string) ([]parseSpec, error) {

	// split according to file specs
	fileSpecs := strings.Split(input, "|")

	spec := make([]parseSpec, len(fileSpecs))
	// split according to column specs
	for i, f := range fileSpecs {
		colSpecs := strings.Split(f, ",")
		if len(colSpecs) == 0 {
			return nil, fmt.Errorf("empty input specification for file entry #%d: %s",
				i, f)
		}

		var ps parseSpec
		for _, cr := range colSpecs {
			c := strings.TrimSpace(cr)
			begin, end, err := parseRange(c)
			if err != nil {
				return nil, err
			}
			for i := begin; i <= end; i++ {
				ps = append(ps, i)
			}
		}
		spec[i] = ps
	}
	return spec, nil
}

// parseOutputSpec parses the comma separated list of output columns
func parseOutputSpec(input string) (parseSpec, error) {

	fileSpecs := strings.Split(input, ",")
	spec := make(parseSpec, len(fileSpecs))
	for i, f := range fileSpecs {
		a, err := strconv.Atoi(f)
		if err != nil {
			return spec, err
		}
		spec[i] = a
	}
	return spec, nil
}

// parseRowSpec parses the comma separated list of row ranges to output
func parseRowSpec(input string) ([]rowRange, error) {

	rowSpecs := strings.Split(input, ",")
	rowRanges := make([]rowRange, len(rowSpecs))
	for i, r := range rowSpecs {
		begin, end, err := parseRange(strings.TrimSpace(r))
		if err != nil {
			return nil, err
		}
		if end < begin {
			return nil, fmt.Errorf("the end of interval %s is smaller than its beginning", r)
		}
		rowRanges[i] = rowRange{begin, end}
	}
	return rowRanges, nil
}

// parseRange parses a range string of the form "a" or a-b", where both a and
// b are integers and "a" is equal to "a-(a+1)". It returns the beginning and
// end of the range
func parseRange(input string) (int, int, error) {

	// check for possible range
	rangeSpec := strings.Split(input, "-")

	var begin, end int
	var err error
	switch len(rangeSpec) {
	case 1: // no range, simple columns
		begin, err = strconv.Atoi(input)
		if err != nil {
			return begin, end, fmt.Errorf("could not convert %s into integer representation",
				input)
		}
		end = begin
	case 2: // range specified via begin and end
		begin, err = strconv.Atoi(rangeSpec[0])
		if err != nil {
			return begin, end, fmt.Errorf("could not convert %s into integer representation",
				rangeSpec[0])
		}

		end, err = strconv.Atoi(rangeSpec[1])
		if err != nil {
			return begin, end, fmt.Errorf("could not convert %s into integer representation",
				rangeSpec[1])
		}
	default:
		return begin, end, fmt.Errorf("incorrect column range specification %s", input)
	}
	return begin, end, nil
}

// splitIntoFloats splits a string consisting of whitespace separated floats
// into a list of floats.
func splitIntoFloats(items []string) ([]float64, error) {

	var floatList []float64
	for _, item := range items {
		val, err := strconv.ParseFloat(strings.TrimSpace(item), 64)
		if err != nil {
			return nil, err
		}
		floatList = append(floatList, val)
	}
	return floatList, nil
}

// mean computes the mean value of a list of float64 values
func mean(items []float64) float64 {
	var mean float64
	for _, x := range items {
		mean += x
	}
	return mean / float64(len(items))
}

// variance computes the variance of a list of float64 values
func variance(items []float64) float64 {
	var mk, qk float64 // helper values for one pass variance computation
	for i, d := range items {
		k := float64(i + 1)
		qk += (k - 1) * (d - mk) * (d - mk) / k
		mk += (d - mk) / k
	}

	var variance float64
	if len(items) > 1 {
		variance = qk / float64(len(items)-1)
	}
	return variance
}

// getInputSepFunc returns a closure used for separating the columns in the
// input files
func getInputSepFunc(inputSep string) func(rune) bool {
	inputSepFunc := unicode.IsSpace
	if len(inputSep) >= 1 {
		inputSepFunc = func(r rune) bool {
			for _, s := range inputSep {
				if s == r {
					return true
				}
			}
			return false
		}
	}
	return inputSepFunc
}

// rowRange is used to specify row ranges to be processed
type rowRange struct {
	b, e int
}

// contains tests if the provided integer value is contained within the supplied
// row range slice. The row range is assumed to be sorted.
// NOTE: An empty rowRangeSlice as a special case returns always true to
// enable the default case in which no row processing is specified
func (rr rowRangeSlice) contains(v int) bool {
	if len(rr) == 0 {
		return true
	}

	for _, r := range rr {
		if v < r.b {
			return false
		} else if v <= r.e {
			return true
		}
	}
	return false
}

// maxEntry contains the largest integer value in the rowRangeSlice
// NOTE: If the rowRangeSlice is empty we return MaxInt
func (rr rowRangeSlice) maxEntry() int {
	if len(rr) == 0 {
		return math.MaxInt64
	}

	var max int
	for _, r := range rr {
		if max < r.e {
			max = r.e
		}
	}
	return max
}

// rowRangeSlice is a helper type to enable sorting
type rowRangeSlice []rowRange

// sort functionality for rowRangeSlice
func (rr rowRangeSlice) Len() int {
	return len(rr)
}

func (rr rowRangeSlice) Swap(i, j int) {
	rr[i], rr[j] = rr[j], rr[i]
}

func (rr rowRangeSlice) Less(i, j int) bool {
	return rr[i].b < rr[j].b
}

// usage prints a simple usage message
func usage() {
	fmt.Printf("pst version %s  (C) 2015 M. Dittrich\n", version)
	fmt.Println()
	fmt.Println("usage: pst <options> file1 file2 ...")
	fmt.Println()
	fmt.Println("options:")
	flag.PrintDefaults()
}

// help prints a simple help message
func help() {
	fmt.Println(exampleText)
}

const exampleText = `Notes:

    The output file is assembled in memory and thus requires sufficient storage
    to hold the complete final output data.

    The input column specifiers are zero based and can include ranges. The end
    of a range is included in the output, i.e. the range 2-5 selects columns
    2, 3, 4, 5.

Examples:

    pst -e "0,1" file1 file2 file3 > outfile

    This command selects columns 0 and 1 from each of file1, file2, and file3
   	and outputs them to outfile (which thus contains 6 columns).


    pst -e "0,1|3" file1 file2 file3 > outfile

    This invocation selects columns 0 and 1 from file1, and column 3 from file2
    and file3. outfile contains 4 columns.


    pst -e "0,1|3|4-5" file1 file2 file3 > outfile

    This command selects column 0 and 1 from file1, column 3 from file2, and
    columns 4 and 5 from file 3. outfile contains 5 columns.


    pst -o "," -i ";" -e "0,1|3|4-5" file1 file2 file3 > outfile

    This command splits the input files into columns with ';' as
    separator. It selects column 0 and 1 from file1, column 3 from file2, and
    columns 4 and 5 from file 3. outfile contains 5 columns each separated
    by ','.


    pst -c -o "," -i ";" -e "0,1|3|4-5" file1 file2 file3 > outfile

    Same as above but instead of outputting 5 columns, it computes and prints
    for each row the mean and variance across each 5 columns. Please note that
    this assumes that each column entry can be converted into a float value.
`
