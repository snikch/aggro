package aggro

import "time"

type Query struct {
	Bucket  *Bucket
	Metrics []Metric
}

type Bucket struct {
	Bucket          *Bucket
	Field           *Field
	DatetimeOptions *DatetimeBucketOptions
}

type DatetimePeriod string

const (
	Year    DatetimePeriod = "year"
	Quarter DatetimePeriod = "quarter"
	Month   DatetimePeriod = "month"
	Week    DatetimePeriod = "week"
	Day     DatetimePeriod = "hour"
)

// DatetimeBucketOptions provides additional configuration for datetime bucketing.
type DatetimeBucketOptions struct {
	// Start will, if provided, ensure buckets start at this date.
	Start *time.Time
	// End will, if provided, ensure buckets continue to this date.
	End    *time.Time
	Period DatetimePeriod
}
