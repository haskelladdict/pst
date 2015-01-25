// unit tests for pst
package main

import (
	"sort"
	"testing"
)

// Test_rowRangeSlices tests the rowRangeSlice data structure
func Test_rowRangeSlice(t *testing.T) {
	var rr rowRangeSlice
	rr = append(rr, rowRange{11, 20})
	rr = append(rr, rowRange{4, 9})
	rr = append(rr, rowRange{2, 5})

	sort.Sort(rr)
	if rr[0].b != 2 || rr[1].b != 4 || rr[2].b != 11 {
		t.Error("error sorting rowRangeSlice")
	}

	if rr.contains(1) || rr.contains(10) || rr.contains(21) {
		t.Error("false positive during rowRangeSLice.contains lookup")
	}

	if !rr.contains(2) || !rr.contains(9) || !rr.contains(3) || !rr.contains(11) ||
		!rr.contains(17) {
		t.Error("false negative during rowRangeSLice.contains lookup")
	}

	if rr.maxEntry() != 20 {
		t.Error("error during rowRangeSLice.maxEntry")
		return
	}
}

// Test_parseInputSpec checks that parseInputSpec() properly parses the provided
// input spec string
func Test_parseInputSpec(t *testing.T) {

	inputString := "0,1-3,10|14,7,2|1,1-4"
	expectedResult := []parseSpec{parseSpec{0, 1, 2, 3, 10}, parseSpec{14, 7, 2},
		parseSpec{1, 1, 2, 3, 4}}
	result, err := parseInputSpec(inputString)
	if err != nil {
		t.Error(err)
		return
	}

	if len(result) != len(expectedResult) {
		t.Errorf("length mismatch between expected and computed result %v", result)
		return
	}

	for i, r := range result {
		if !parseSpecsIdentical(r, expectedResult[i]) {
			t.Errorf("expected %v and computed %v results don't match", r, expectedResult[i])
			return
		}
	}
}

// Test_parseInputSpec checks that parseInputSpec() properly parses the provided
// input spec string
func Test_parseOutputSpec(t *testing.T) {

	inputString := "0,1,10,14,7,2,1,4"
	expectedResult := parseSpec{0, 1, 10, 14, 7, 2, 1, 4}
	result, err := parseOutputSpec(inputString)
	if err != nil {
		t.Error(err)
		return
	}

	if len(result) != len(expectedResult) {
		t.Errorf("length mismatch between expected and computed result %v", result)
		return
	}

	if !parseSpecsIdentical(result, expectedResult) {
		t.Errorf("expected %v and computed %v results don't match", result, expectedResult)
		return
	}
}

// parseSpecsIdentical is a helper function for checking two parseSpecs for identity
func parseSpecsIdentical(x, y parseSpec) bool {
	if len(x) != len(y) {
		return false
	}

	for i, v := range x {
		if v != y[i] {
			return false
		}
	}

	return true

}
