// pst is a command line tool for processing and combining columns across
// column oriented files
package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
	"unicode"
)

// command line switches
var (
	inputSpec    string
	inputSep     string
	outputSep    string
	computeStats bool
)

// outData collects a row oriented list of column entries
type outData []string

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
	flag.Parse()
	if len(flag.Args()) < 1 || inputSpec == "" {
		usage()
		os.Exit(1)
	}

	inputSepFunc := getInputSepFunc(inputSep)

	colSpecs, err := parseInputSpec(inputSpec)
	if err != nil {
		log.Fatal(err)
	}

	if len(colSpecs) > len(flag.Args()) {
		log.Fatal("there are more per file column specifiers than supplied input files")
	}

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
}

// parseFile reads the passed in file, extracts the columns requested per spec
// and the returns a slice with the requested column info.
func parseFile(fileName string, spec parseSpec, sepFun func(rune) bool,
	outSep string) (outData, error) {

	file, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}

	var out outData
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		items := strings.FieldsFunc(strings.TrimSpace(scanner.Text()), sepFun)
		var row string
		for _, i := range spec {
			if i >= len(items) {
				return nil, fmt.Errorf("error parsing file %s: requested column %d "+
					"does not exist", fileName, i)
			}
			row += items[i]
			row += outSep
		}
		out = append(out, row)
	}
	return out, nil
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

// readData parses all the output files and populates and returns the output
// data set
func readData(files []string, colSpecs []parseSpec, sepFun func(rune) bool,
	outSep string) (outData, error) {

	var output outData
	for i, file := range files {

		// pick the correct specification for parsing columns
		var spec parseSpec
		if i < len(colSpecs) {
			spec = colSpecs[i]
		} else {
			spec = colSpecs[len(colSpecs)-1]
		}

		parsedRows, err := parseFile(file, spec, sepFun, outSep)
		if err != nil {
			log.Fatal(err)
		}

		// initialize output after parsing the first data file
		if i == 0 {
			output = make([]string, len(parsedRows))
		}

		// make sure input files have consistent length
		if len(parsedRows) != len(output) {
			return nil, fmt.Errorf("input file %s has %d rows which differs from %d "+
				"in previous files", file, len(parsedRows), len(output))
		}

		for j, row := range parsedRows {
			output[j] += row
		}

		// force a GC cycle
		parsedRows = nil
		debug.FreeOSMemory()
	}

	// remove bogus final output separator
	for i, row := range output {
		output[i] = row[:len(row)-len(outSep)]
	}

	return output, nil
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
