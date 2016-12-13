package aggro

import (
	"fmt"
	"math"
	"sort"

	"github.com/shopspring/decimal"
)

type Metric struct {
	Type  string
	Field string
}

func (m *Metric) measurer() (measurer, error) {
	switch m.Type {
	case "mean":
		return &mean{}, nil
	case "median":
		return &median{}, nil
	case "mode":
		return &mode{}, nil
	case "min":
		return &min{}, nil
	case "max":
		return &max{}, nil
	case "cardinality":
		return &cardinality{}, nil
	case "sum":
		return &sum{}, nil
	case "stdev":
		return &stdev{}, nil
	default:
		return nil, fmt.Errorf("Unknown metric: %s", m.Type)
	}
}

type measurer interface {
	AddDatum(interface{})
	Result() interface{}
}

// Mean
type mean struct {
	count int
	sum   decimal.Decimal
}

func (a *mean) AddDatum(datum interface{}) {
	amount := datum.(*decimal.Decimal)
	a.count++
	a.sum = a.sum.Add(*amount)
}

func (a *mean) Result() interface{} {
	if a.count == 0 {
		return nil
	}
	result, _ := a.sum.Div(decimal.NewFromFloat(float64(a.count))).Float64()
	return result
}

// Median
type median struct {
	list []decimal.Decimal
}

func (a *median) AddDatum(datum interface{}) {
	amount := datum.(*decimal.Decimal)
	a.list = append(a.list, *amount)
}

func (a *median) Result() interface{} {
	if len(a.list) == 0 {
		return nil
	}

	// Sort our list in numerical order.
	sort.Sort(decimalSortNumerical(a.list))

	// Determine median value.
	middle := len(a.list) / 2
	median, _ := a.list[middle].Float64()

	// Even slice? Get the mean of the middle values.
	if len(a.list)%2 == 0 {
		prev, _ := a.list[middle-1].Float64()
		median = (median + prev) / 2
	}

	return median
}

// Mode
type mode struct {
	count int
	list  []decimal.Decimal
	value decimal.Decimal
}

func (a *mode) AddDatum(datum interface{}) {
	amount := datum.(*decimal.Decimal)
	a.list = append(a.list, *amount)
	a.count++
}

func (a *mode) Result() interface{} {
	if len(a.list) == 0 {
		return nil
	}

	numbers := []float64{}
	for _, number := range a.list {
		value, _ := number.Float64()
		numbers = append(numbers, value)
	}

	modes := []float64{}
	freq := make(map[float64]int, len(numbers))
	tip := 0

	// Range our values determining the highest frequency (tip).
	for _, x := range numbers {
		freq[x]++
		if freq[x] > tip {
			tip = freq[x]
		}
	}
	for x, f := range freq {
		if f == tip {
			modes = append(modes, x)
		}
	}
	if tip == 1 || len(modes) == len(numbers) {
		modes = []float64{}
	}
	return modes
}

// Min
type min struct {
	amount *decimal.Decimal
}

func (a *min) AddDatum(datum interface{}) {
	amount := datum.(*decimal.Decimal)
	if a.amount == nil {
		a.amount = amount
	}
	if (a.amount).Cmp(*amount) > -1 {
		a.amount = amount
	}
}

func (a *min) Result() interface{} {
	if a.amount == nil {
		return nil
	}
	result, _ := a.amount.Float64()
	return result
}

// Max
type max struct {
	amount *decimal.Decimal
}

func (a *max) AddDatum(datum interface{}) {
	amount := datum.(*decimal.Decimal)
	if a.amount == nil {
		a.amount = amount
	}
	if (a.amount).Cmp(*amount) < 0 {
		a.amount = amount
	}
}

func (a *max) Result() interface{} {
	if a.amount == nil {
		return nil
	}
	result, _ := a.amount.Float64()
	return result
}

// Cadinality
type cardinality struct {
	size int
}

func (a *cardinality) AddDatum(datum interface{}) {
	a.size++
}

func (a *cardinality) Result() interface{} {
	return a.size
}

// Sum
type sum struct {
	sum decimal.Decimal
}

func (a *sum) AddDatum(datum interface{}) {
	amount := datum.(*decimal.Decimal)
	a.sum = a.sum.Add(*amount)
}

func (a *sum) Result() interface{} {
	result, _ := a.sum.Float64()
	return result
}

// Standard deviation
type stdev struct {
	count int
	sum   decimal.Decimal
	list  []decimal.Decimal
}

func (a *stdev) AddDatum(datum interface{}) {
	amount := datum.(*decimal.Decimal)
	a.count++
	a.sum = a.sum.Add(*amount)
	a.list = append(a.list, *amount)
}

func (a *stdev) Result() interface{} {
	if a.count == 0 {
		return nil
	}

	// Find the mean.
	mean, _ := a.sum.Div(decimal.NewFromFloat(float64(a.count))).Float64()

	// Find and add each values distance to the mean.
	total := 0.0
	for _, number := range a.list {
		val, _ := number.Float64()
		total += math.Pow(val-mean, 2)
	}

	// Divide by result set length.
	variance := total / float64(len(a.list)-1)
	return math.Sqrt(variance)
}

// Sorters
type decimalSortNumerical []decimal.Decimal

func (s decimalSortNumerical) Len() int      { return len(s) }
func (s decimalSortNumerical) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s decimalSortNumerical) Less(i, j int) bool {
	a, _ := s[i].Float64()
	b, _ := s[j].Float64()
	return a < b
}
