package aggro

import (
	"fmt"
	"time"
)

// DatetimePeriod provides a string type to represent a date bucketing period.
type DatetimePeriod string

// Helper constants representing acceptable DatetimePeriods.
const (
	Year    DatetimePeriod = "year"
	Quarter DatetimePeriod = "quarter"
	Month   DatetimePeriod = "month"
	Week    DatetimePeriod = "week"
	Day     DatetimePeriod = "hour"
)

func datetimeValueForPeriod(value *time.Time, period DatetimePeriod) (string, error) {
	switch period {
	case Year:
		return time.Date(value.Year(), 1, 1, 0, 0, 0, 0, value.Location()).Format(time.RFC3339), nil
	case Quarter:
		// Get the month, but as a quarter start, rather than month start.
		month := (((value.Month() - 1) / 3) * 3) + 1
		return time.Date(value.Year(), month, 1, 0, 0, 0, 0, value.Location()).Format(time.RFC3339), nil
	case Month:
		return time.Date(value.Year(), value.Month(), 1, 0, 0, 0, 0, value.Location()).Format(time.RFC3339), nil
	case Week:
		day := value.Day() - int(value.Weekday())
		return time.Date(value.Year(), value.Month(), day, 0, 0, 0, 0, value.Location()).Format(time.RFC3339), nil
	case Day:
		return time.Date(value.Year(), value.Month(), value.Day(), 0, 0, 0, 0, value.Location()).Format(time.RFC3339), nil
	default:
		return "", fmt.Errorf("Unknown datetime period: %s", period)
	}
}

func datetimeAddPeriod(value *time.Time, period DatetimePeriod) (*time.Time, error) {
	var t time.Time
	switch period {
	case Year:
		t = value.AddDate(1, 0, 0)
	case Quarter:
		t = value.AddDate(0, 3, 0)
	case Month:
		t = value.AddDate(0, 1, 0)
	case Week:
		t = value.AddDate(0, 0, 7)
	case Day:
		t = value.AddDate(0, 0, 1)
	default:
		return &t, fmt.Errorf("Unknown datetime period: %s", period)
	}
	return &t, nil
}
