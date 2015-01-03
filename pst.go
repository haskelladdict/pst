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
	"sort"
	"strconv"
	"strings"
	"unicode"
)

// vars for the command line parser
var (
	inputSpec    string
	computeStats bool
)

// outData collects a row oriented list of column entries
type outData []string

// parseSpec describes for each input files which columns to parse
type parseSpec []int

func init() {
	flag.StringVar(&inputSpec, "c", "", "specify the input columns to parse for "+
		"each of the input files")
	flag.BoolVar(&computeStats, "s", false, "compute statistics across column values "+
		"in each row of the final output. Please note that each value in the output "+
		"has to be convertible into a float for this to work. Currently "+
		"only mean and standard deviation are computed")
}

func main() {
	flag.Parse()
	if len(flag.Args()) < 1 {
		usage()
		os.Exit(1)
	}

	colSpecs, err := parseInputSpec(inputSpec)
	if err != nil {
		log.Fatal(err)
	}

	if len(colSpecs) > len(flag.Args()) {
		log.Fatal("there are more per file column specifiers than supplied input files")
	}

	// read input files and assemble output
	output, err := readData(flag.Args(), colSpecs)
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
func parseFile(fileName string, spec parseSpec) (outData, error) {

	file, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}

	var out outData
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		items := strings.FieldsFunc(strings.TrimSpace(scanner.Text()), unicode.IsSpace)
		//row := make([]string, 0, len(spec))
		var row string
		for _, i := range spec {
			if i >= len(items) {
				return nil, fmt.Errorf("error parsing file %s: requested column %d "+
					"does not exist", fileName, i)
			}
			row += items[i]
			row += " "
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

		var ps sort.IntSlice
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
		ps.Sort()
		spec[i] = parseSpec(ps)
	}
	return spec, nil
}

// readData parses all the output files and populates and returns the output
// data set
func readData(files []string, colSpecs []parseSpec) (outData, error) {

	var output outData
	for i, file := range files {

		// pick the correct specification for parsing columns
		var spec parseSpec
		if i < len(colSpecs) {
			spec = colSpecs[i]
		} else {
			spec = colSpecs[len(colSpecs)-1]
		}

		parsedCols, err := parseFile(file, spec)
		if err != nil {
			log.Fatal(err)
		}

		// initialize output after parsing the first data file
		if i == 0 {
			output = make([]string, len(parsedCols))
		}

		// make sure input files have consistent length
		if len(parsedCols) != len(output) {
			return nil, fmt.Errorf("input file %s has %d rows which differs from %d "+
				"in previous files", file, len(parsedCols), len(output))
		}

		// append parsed data to output
		for i, row := range parsedCols {
			output[i] += row
		}

		// force a GC cycle
		parsedCols = nil
		debug.FreeOSMemory()
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

// usage prints a simple usage/help message
func usage() {
	fmt.Println("pst (C) 2015 M. Dittrich")
	fmt.Println()
	fmt.Println("usage: pst <options> file1 file2 ...")
	fmt.Println()
	fmt.Println("options:")
	flag.PrintDefaults()
}
