// Copyright 2014 Markus Dittrich
// Licensed under BSD license, see LICENSE file for details

package main

import (
	"container/heap"
	"log"
	"math"
)

// min returns the minumum value of an array of floats
func min(fs []float64) float64 {
	minVal := math.MaxFloat64
	for _, f := range fs {
		if f < minVal {
			minVal = f
		}
	}
	return minVal
}

// max returns the maximum value of an array of floats
func max(fs []float64) float64 {
	maxVal := -math.MaxFloat64
	for _, f := range fs {
		if f > maxVal {
			maxVal = f
		}
	}
	return maxVal
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

// median computes the median of the provided
func median(fs []float64) float64 {
	m := newMedData()
	for _, f := range fs {
		updateMedian(m, f)
	}
	return m.val
}

// medData holds the data structures needed to compute a running median.
// Currently, the running median is implemented via a min and max heap data
// structure and thus requires storage on the order of the data set size
type medData struct {
	smaller, larger FloatHeap
	val             float64
}

// newMedData initializes the data structure for computing the running median
func newMedData() *medData {
	var m medData
	heap.Init(&m.smaller)
	heap.Init(&m.larger)
	return &m
}

// updateMedian updates the running median using two heaps the each keep
// track of elements smaller and larger than the current median.
func updateMedian(m *medData, v float64) *medData {
	if len(m.smaller) == 0 && len(m.larger) == 0 {
		// insert first element
		heap.Push(&m.smaller, -v)
	} else if len(m.smaller) == 0 {
		// insert second element (first case)
		if v > m.larger[0] {
			heap.Push(&m.smaller, -heap.Pop(&m.larger).(float64))
			heap.Push(&m.larger, v)
		} else {
			heap.Push(&m.smaller, -v)
		}
	} else if len(m.larger) == 0 {
		// insert second element (second case)
		if v < -m.smaller[0] {
			heap.Push(&m.larger, -heap.Pop(&m.smaller).(float64))
			heap.Push(&m.smaller, -v)
		} else {
			heap.Push(&m.larger, v)
		}
	} else {
		// insert third and following elements
		if v < m.val {
			heap.Push(&m.smaller, -v)
		} else if v > m.val {
			heap.Push(&m.larger, v)
		} else {
			if len(m.smaller) <= len(m.larger) {
				heap.Push(&m.smaller, -v)
			} else {
				heap.Push(&m.larger, v)
			}
		}
	}

	// fix up heaps if they differ in length by more than 2
	if len(m.smaller) == len(m.larger)+2 {
		heap.Push(&m.larger, -heap.Pop(&m.smaller).(float64))
	} else if len(m.larger) == len(m.smaller)+2 {
		heap.Push(&m.smaller, -heap.Pop(&m.larger).(float64))
	}

	// compute new median
	if len(m.smaller) == len(m.larger) {
		m.val = 0.5 * (m.larger[0] - m.smaller[0])
	} else if len(m.smaller) > len(m.larger) {
		m.val = -m.smaller[0]
	} else {
		m.val = m.larger[0]
	}

	if math.Abs(float64(len(m.smaller)-len(m.larger))) > 1 {
		log.Panic("median heaps differ by more than 2")
	}

	return m
}

// FloatHeap is a min-heap of float64
type FloatHeap []float64

// implement heap interface for FloatHeap
func (f FloatHeap) Len() int {
	return len(f)
}

func (f FloatHeap) Less(i, j int) bool {
	return f[i] < f[j]
}

func (f FloatHeap) Swap(i, j int) {
	f[i], f[j] = f[j], f[i]
}

// Push is part of heap interface
func (f *FloatHeap) Push(x interface{}) {
	*f = append(*f, x.(float64))
}

// Pop is part of heap interface
func (f *FloatHeap) Pop() interface{} {
	old := *f
	n := len(old)
	x := old[n-1]
	*f = old[0 : n-1]
	return x
}
