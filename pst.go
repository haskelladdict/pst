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

// Spec describes what to parse and how to assemble the output
type Spec struct {
	input     string
	output    string
	inputSep  string
	outputSep string
	rows      string
}

// command line switches
var (
	numThreads   int
	spec         Spec
	computeStats bool
	showHelp     bool
)

// parseSpec describes for each input files which columns to parse
type parseSpec []int

func init() {
	flag.StringVar(&spec.input, "e", "",
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
	flag.StringVar(&spec.inputSep, "i", "",
		`column separator for input files. The default separator is whitespace.`)
	flag.StringVar(&spec.outputSep, "o", " ",
		`column separator for output files. The default separator is a single space.`)
	flag.BoolVar(&showHelp, "h", false, "show basic usage info")
	flag.StringVar(&spec.output, "p", "",
		`specify the order in which to paste the output columns.
     The spec format is "i,j,k-l,m,..", where 0 < i,j,k,l,m, ... < numCol, and
     numCol is the total number of columns extracted from the input files.
          Columns can be specified multiple times and ranges are accepted. If this
     option is not provided the columns are pasted in the order in which they
     are extracted.`)
	flag.StringVar(&spec.rows, "r", "",
		`specify which rows to process and output.
     This flag is optional. If not specified all rows will be output. Rows can
     be specified by a comma separated list of row IDs or row ID ranges. E.g.,
     "1,2,4-8,22" will process rows 1, 2, 4, 5, 7, 22.`)
	flag.IntVar(&numThreads, "n", 1, "number of threads (default: 1)")
}

func main() {
	runtime.GOMAXPROCS(numThreads)

	flag.Parse()
	if showHelp {
		usage()
		help()
		os.Exit(0)
	}

	if len(flag.Args()) < 1 {
		usage()
		os.Exit(1)
	}
	fileNames := flag.Args()
	numFileNames := len(fileNames)

	// an outputSpec requires a valid inputSpec
	if len(spec.output) != 0 && len(spec.input) == 0 {
		log.Fatal("An output paste spec requires an input column spec.")
	}

	inputSepFunc := getInputSepFunc(spec.inputSep)

	// parse input column specs and pad with final element if we have more files
	// than provided spec entries
	inCols, err := parseInputSpec(spec.input)
	if err != nil {
		log.Fatal(err)
	}
	if len(inCols) > numFileNames {
		log.Fatal("there are more per file column specifiers than supplied input files")
	}
	finalSpec := inCols[len(inCols)-1]
	pading := numFileNames - len(inCols)
	for i := 0; i < pading; i++ {
		inCols = append(inCols, finalSpec)
	}

	// parse output column spec if requested
	var outCols parseSpec
	if spec.output != "" {
		outCols, err = parseOutputSpec(spec.output)
		if err != nil {
			log.Fatal(err)
		}
	}
	min, max := outCols.minMax()
	if max > len(inCols) || min < 0 {
		log.Fatal("at least one output column specifier is out of bounds or negative.")
	}

	// parse row ranges to process
	var rowRanges rowRangeSlice
	if spec.rows != "" {
		rowRanges, err = parseRowSpec(spec.rows)
		if err != nil {
			log.Fatal(err)
		}
		sort.Sort(rowRanges)
	}

	err = parseData(fileNames, inCols, outCols, rowRanges, inputSepFunc, spec.outputSep)
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

	var dataChs []chan []string
	for i, name := range fileNames {
		dataCh := make(chan []string, 10000) // use buffered channels to not stall IO
		dataChs = append(dataChs, dataCh)
		wg.Add(1)
		go fileParser(name, inCols[i], rowRanges, inputSepFun, dataCh, done, errCh, &wg)
	}

	err := processData(dataChs, errCh, outCols, outSep)
	close(done)
	wg.Wait()

	return err
}

// processData goes through all channels delivering data assembling each row
// and then printing it out
func processData(dataChs []chan []string, errCh <-chan error, outCols parseSpec,
	outSep string) error {

	var inRow []string
	defaultInRows := make([][]string, len(dataChs))
	deadChannels := make([]bool, len(dataChs))
	activeChannels := len(dataChs)
	outRow := make([]string, len(outCols))
	output := bufio.NewWriter(os.Stdout)
	defer output.Flush()
	for row := 0; ; row++ {
		// process each data channel to read the column entries for the current row
		var in int
		for i, ch := range dataChs {
			select {
			case cols := <-ch:
				if cols == nil {
					if !deadChannels[i] {
						deadChannels[i] = true
						activeChannels--
					}
					if activeChannels == 0 {
						return nil // all channels are done reading so we're done, too
					}
					cols = defaultInRows[i]
				}
				// When we hit the first row we initialize the inRow array. For all
				// subsequent rows we can recycle it for efficiency (UGLY I know)
				if row == 0 {
					for _, c := range cols {
						inRow = append(inRow, c)
					}
					defaultInRows[i] = make([]string, len(cols))
				} else {
					for _, c := range cols {
						inRow[in] = c
						in++
					}
				}
			case err := <-errCh:
				return err
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
				return err
			}
			fmt.Fprintf(output, "%15.15f %15.15f\n", mean(items), variance(items))
		} else {
			fmt.Fprintf(output, "%s\n", strings.Join(outRow, outSep))
		}
	}
}

// fileParser opens fileName, parses it in a line by line fashion and sends
// the requested columns combined into a string down the data channel.
// If it receives on the done channel it stops processing and returns
func fileParser(fileName string, colSpec parseSpec, rowRanges rowRangeSlice,
	sepFun func(rune) bool, data chan<- []string, done <-chan struct{},
	errCh chan<- error, wg *sync.WaitGroup) {

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

		var row []string
		// an empty colSpec signals all rows
		if len(colSpec) == 0 {
			row = append(row, scanner.Text())
		} else {
			row = make([]string, len(colSpec))
			items := strings.FieldsFunc(strings.TrimSpace(scanner.Text()), sepFun)
			for i, c := range colSpec {
				if c >= len(items) {
					errCh <- fmt.Errorf("error parsing file %s: requested column %d "+
						"does not exist", fileName, c)
					return
				}
				row[i] = items[c]
			}
		}

		select {
		case data <- row:
		case <-done:
			return
		}
	}
	if err := scanner.Err(); err != nil {
		errCh <- err
	}
	return
}

// parseInputSpec parses the inputSpec and turns it into a slice of parseSpecs,
// one for each input file. An empty inputSpec is assumed to imply that the
// user wants to grab all columns in each file
func parseInputSpec(input string) ([]parseSpec, error) {

	if len(input) == 0 {
		return []parseSpec{parseSpec{}}, nil
	}

	// split according to file specs
	fileSpecs := strings.Split(input, "|")

	spec := make([]parseSpec, len(fileSpecs))
	// split according to column specs
	for i, f := range fileSpecs {
		colSpecs := strings.Split(f, ",")
		if len(colSpecs) == 1 {
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
			ps = append(ps, makeIntRange(begin, end)...)
		}
		spec[i] = ps
	}
	return spec, nil
}

// parseOutputSpec parses the comma separated list of output columns
func parseOutputSpec(input string) (parseSpec, error) {

	fileSpecs := strings.Split(input, ",")
	var spec parseSpec
	for _, f := range fileSpecs {
		begin, end, err := parseRange(f)
		if err != nil {
			return spec, err
		}
		spec = append(spec, makeIntRange(begin, end)...)
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

// makeIntRange creates a slice of consecutive ints starting at begin until
// and includise end.
// NOTE: This function assumes end >= begin
func makeIntRange(begin, end int) []int {
	r := make([]int, 0, end-begin+1)
	for i := begin; i <= end; i++ {
		r = append(r, i)
	}
	return r
}

// minMax returns the minimum and maximum value in a parseSpec
func (p parseSpec) minMax() (int, int) {
	maxVal := -math.MaxInt64
	minVal := math.MaxInt64
	for _, v := range p {
		if v > maxVal {
			maxVal = v
		} else if v < minVal {
			minVal = v
		}
	}
	return minVal, maxVal
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
