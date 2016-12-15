package aggro

import (
	"errors"
	"fmt"
	"time"

	"github.com/shopspring/decimal"
)

type Dataset struct {
	Table *Table
	Rows  []map[string]Cell
}

func (set *Dataset) Run(query *Query) (*Resultset, error) {
	return (&queryProcessor{
		dataset: set,
		query:   query,
	}).Run()
}

func (set *Dataset) AddRows(rows ...map[string]interface{}) error {
	var err error
	// Add each row.
	for i, data := range rows {
		row := map[string]Cell{}
		// For each row, we need to create a cell for each field definition and
		// ensure that we have received data that conforms to the supposed format.
		for j, field := range set.Table.Fields {
			datum, ok := data[field.Name]
			if !ok {
				return fmt.Errorf("Error adding row %d, cell %d: Data key %s not present", i, j, field.Name)
			}
			if datum == nil {
				// Skip if datum is nil
				continue
			}

			row[field.Name], err = newCell(datum, &field)
			if err != nil {
				return fmt.Errorf("Error adding row %d, cell %d: %s", i, j, err.Error())
			}
		}
		set.Rows = append(set.Rows, row)
	}
	return nil
}

const (
	fieldTypeString   = "string"
	fieldTypeNumber   = "number"
	fieldTypeDatetime = "datetime"
)

func newCell(datum interface{}, field *Field) (Cell, error) {
	var cell Cell

	switch field.Type {
	case fieldTypeString:
		stringValue, ok := datum.(string)
		if !ok {
			return nil, fmt.Errorf("Expected string datatype, got %T", datum)
		}
		cell = &StringCell{
			value: stringValue,
			field: field,
		}
	case fieldTypeDatetime:
		datetimeCell := &DatetimeCell{
			field: field,
		}
		switch datumTyped := datum.(type) {
		case time.Time:
			datetimeCell.value = &datumTyped
		case *time.Time:
			if datumTyped == nil {
				return nil, errors.New("Got nil *time.Time for datetime field")
			}
			datetimeCell.value = datumTyped
		case string:
			t, err := time.Parse(time.RFC3339, datumTyped)
			if err != nil {
				return nil, errors.New("Invalid date string passed for datetime field. RFC3339 datetime string required.")
			}
			datetimeCell.value = &t
		default:
			return nil, fmt.Errorf("Expected string or time.Time, got %T", datum)
		}
		cell = datetimeCell
	case fieldTypeNumber:
		numberCell := &NumberCell{
			field: field,
		}
		var d decimal.Decimal
		switch datumTyped := datum.(type) {
		case int:
			d = decimal.NewFromFloat(float64(datumTyped))
		case int32:
			d = decimal.NewFromFloat(float64(datumTyped))
		case int64:
			d = decimal.NewFromFloat(float64(datumTyped))
		case float32:
			d = decimal.NewFromFloat(float64(datumTyped))
		case float64:
			d = decimal.NewFromFloat(datumTyped)
		default:
			return nil, fmt.Errorf("Expected number, got %T", datum)
		}
		numberCell.value = &d
		cell = numberCell
	default:
		return nil, fmt.Errorf("Unknown field type: %s", field.Type)
	}
	return cell, nil
}

type Cell interface {
	FieldDefinition() *Field
	IsMetricable(m measurer) bool
	MeasurableCell() MeasurableCell
}

type MeasurableCell interface {
	Value() interface{}
}

type NumberCell struct {
	value *decimal.Decimal
	field *Field
}

func (cell *NumberCell) FieldDefinition() *Field {
	return cell.field
}

func (cell *NumberCell) IsMetricable(m measurer) bool {
	return true
}

func (cell *NumberCell) Value() interface{} { //*decimal.Decimal {
	return cell.value
}

func (cell *NumberCell) MeasurableCell() MeasurableCell {
	return cell
}

type DatetimeCell struct {
	value *time.Time
	field *Field
}

func (cell *DatetimeCell) FieldDefinition() *Field {
	return cell.field
}

func (cell *DatetimeCell) IsMetricable(m measurer) bool {
	return false
}

func (cell *DatetimeCell) Value() interface{} { //*time.Time {
	return cell.value
}

func (cell *DatetimeCell) MeasurableCell() MeasurableCell {
	return nil
}

// ValueForPeriod returns the start of a given period.
func (cell *DatetimeCell) ValueForPeriod(period DatetimePeriod, location *time.Location) (string, error) {
	return datetimeValueForPeriod(cell.value, period, location)
}

type StringCell struct {
	value string
	field *Field
}

func (cell *StringCell) FieldDefinition() *Field {
	return cell.field
}

func (cell *StringCell) IsMetricable(m measurer) bool {
	// We allow certain metrics to run on StringCells.
	switch m.(type) {
	case *cardinality, *valueCount:
		return true
	default:
		return false
	}
}

func (cell *StringCell) Value() interface{} {
	return cell.value
}

func (cell *StringCell) MeasurableCell() MeasurableCell {
	return cell
}
