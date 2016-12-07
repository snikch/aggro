package aggro

import (
	"fmt"

	"github.com/shopspring/decimal"
)

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

type measurer interface {
	AddDatum(interface{})
	Result() interface{}
}

type averager struct {
	count int
	sum   decimal.Decimal
}

func (a *averager) AddDatum(datum interface{}) {
	amount := datum.(*decimal.Decimal)
	a.count++
	a.sum = a.sum.Add(*amount)
}

func (a *averager) Result() interface{} {
	result, _ := a.sum.Div(decimal.NewFromFloat(float64(a.count))).Float64()
	return result
}

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
	result, _ := a.amount.Float64()
	return result
}

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
	result, _ := a.amount.Float64()
	return result
}
