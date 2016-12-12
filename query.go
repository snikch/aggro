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

// DatetimeBucketOptions provides additional configuration for datetime bucketing.
type DatetimeBucketOptions struct {
	// Start will, if provided, ensure buckets start at this date.
	Start *time.Time
	// End will, if provided, ensure buckets continue to this date.
	End *time.Time
	// What interval period are the results to be bucketed at.
	Period DatetimePeriod
	// Datetimes should be bucketed based on the date in this location.
	Location *time.Location
}
