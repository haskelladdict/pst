// pst is a command line tool for processing and combining columns across
// column oriented files
package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"unicode"
)

// command line switches
var (
	inputSpec    string
	inputSep     string
	outputSep    string
	computeStats bool
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
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	flag.Parse()
	if len(flag.Args()) < 1 || inputSpec == "" {
		usage()
		os.Exit(1)
	}
	fileNames := flag.Args()
	numFileNames := len(fileNames)

	inputSepFunc := getInputSepFunc(inputSep)

	// parse input column specs and pad with final element if we have more files
	// than provided spec entries
	colSpecs, err := parseInputSpec(inputSpec)
	if err != nil {
		log.Fatal(err)
	}
	if len(colSpecs) > numFileNames {
		log.Fatal("there are more per file column specifiers than supplied input files")
	}
	finalSpec := colSpecs[len(colSpecs)-1]
	pading := numFileNames - len(colSpecs)
	for i := 0; i <= pading; i++ {
		colSpecs = append(colSpecs, finalSpec)
	}

	// each input file is parsed in a separate goroutine. The done channel is
	// used to signal each goroutine to shut down. The errCh channel signals any
	// file opening/parsing issue to the main routine.
	var wg sync.WaitGroup
	done := make(chan struct{}, 1)
	errCh := make(chan error)
	var dataChs []chan string
	for i, name := range fileNames {
		wg.Add(1)
		dataCh := make(chan string)
		dataChs = append(dataChs, dataCh)
		go fileParser(name, colSpecs[i], inputSepFunc, outputSep, dataCh, done,
			errCh, &wg)
	}

Loop:
	for {
		var row string
		// process each data channel to read the column entries for the current row
		for _, ch := range dataChs {
			select {
			case c := <-ch:
				if c == "" {
					done <- struct{}{}
					break Loop
				}
				row += c
				row += outputSep
			case err := <-errCh:
				done <- struct{}{}
				log.Print(err)
				break Loop
			}
		}

		if computeStats == true {
			items, err := splitIntoFloats(row)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println(mean(items), variance(items))
		} else {
			fmt.Println(row)
		}
	}

	wg.Wait()

	/*
		// read input files and assemble output
		output, err := readData(flag.Args(), colSpecs, inputSepFunc, outputSep)
		if err != nil {
			log.Fatal(err)
		}

		// compute statistics or punch the data otherwise
		if computeStats == true {
			for _, row := range output {

				items, err := splitIntoFloats(row)
				if err != nil {
					log.Fatal(err)
				}
				fmt.Println(mean(items), variance(items))
			}
		} else {
			for _, row := range output {
				fmt.Println(row)
			}
		}
	*/
}

func fileParser(fileName string, colSpec parseSpec, sepFun func(rune) bool,
	outSep string, data chan<- string, done <-chan struct{}, errCh chan<- error,
	wg *sync.WaitGroup) {

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
	for scanner.Scan() {
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

			// check for possible range
			colRange := strings.Split(c, "-")

			switch len(colRange) {
			case 1: // no range, simple columns
				cInt, err := strconv.Atoi(c)
				if err != nil {
					return nil, fmt.Errorf("could not convert %s into integer representation", c)
				}
				ps = append(ps, cInt)
			case 2: // range specified via begin and end
				aInt, err := strconv.Atoi(colRange[0])
				if err != nil {
					return nil, fmt.Errorf("could not convert %s into integer representation",
						colRange[0])
				}

				bInt, err := strconv.Atoi(colRange[1])
				if err != nil {
					return nil, fmt.Errorf("could not convert %s into integer representation",
						colRange[1])
				}

				for i := aInt; i < bInt; i++ {
					ps = append(ps, i)
				}
			default:
				return nil, fmt.Errorf("incorrect column range specification %s", c)
			}
		}
		spec[i] = ps
	}
	return spec, nil
}

// splitIntoFloats splits a string consisting of whitespace separated floats
// into a list of floats.
func splitIntoFloats(floatString string) ([]float64, error) {

	items := strings.FieldsFunc(floatString, unicode.IsSpace)
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

// usage prints a simple usage/help message
func usage() {
	fmt.Println("pst             (C) 2015 M. Dittrich")
	fmt.Println()
	fmt.Println("usage: pst <options> file1 file2 ...")
	fmt.Println()
	fmt.Println("options:")
	flag.PrintDefaults()
	fmt.Println()
	fmt.Println(exampleText)
}

const exampleText = `Notes:

    The output file is assembled in memory and thus requires sufficient storage
    to hold the complete final output data.

    The input column specifiers are zero based and can include ranges. The end
    of a range is not included in the output, i.e. the range 2-5 selects columns
    2, 3, and 4.

Examples:

    pst -e "0,1" file1 file2 file3 > outfile

    This command selects columns 0 and 1 from each of file1, file2, and file3
   	and outputs them to outfile (which thus contains 6 columns).


    pst -e "0,1|3" file1 file2 file3 > outfile

    This invocation selects columns 0 and 1 from file1, and column 3 from file2
    and file3. outfile contains 4 columns.


    pst -e "0,1|3|4-6" file1 file2 file3 > outfile

    This command selects column 0 and 1 from file1, column 3 from file2, and
    columns 4 and 5 from file 3. outfile contains 5 columns.


    pst -o "," -i ";" -e "0,1|3|4-6" file1 file2 file3 > outfile

    This command splits the input files into columns with ';' as
    separator. It selects column 0 and 1 from file1, column 3 from file2, and
    columns 4 and 5 from file 3. outfile contains 5 columns each separated
    by ','.


    pst -c -o "," -i ";" -e "0,1|3|4-6" file1 file2 file3 > outfile

    Same as above but instead of outputting 5 columns, it computes and prints
    for each row the mean and variance across each 5 columns. Please note that
    this assumes that each column entry can be converted into a float value.
`
