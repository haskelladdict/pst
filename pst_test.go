// unit tests for pst
package main

import (
	"sort"
	"testing"
)

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
	}
}
