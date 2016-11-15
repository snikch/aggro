package aggro

import "fmt"

type Query struct {
	Bucket  *Bucket
	Metrics []Metric
}

type Bucket struct {
	Bucket *Bucket
	Field  *Field
}

type Metric struct {
	Type  string
	Field string
}

func (m *Metric) measurer() (measurer, error) {
	switch m.Type {
	case "avg":
		return &averager{}, nil
	case "min":
		return &min{}, nil
	case "max":
		return &max{}, nil
	default:
		return nil, fmt.Errorf("Unknown metric: %s", m.Type)
	}
}
