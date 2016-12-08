package aggro

import (
	"encoding/json"
	"testing"
	"time"

	. "github.com/onsi/gomega"
)

var (
	table = &Table{
		Fields: []Field{
			{"location", "string"},
			{"department", "string"},
			// {"name", "string"},
			{"salary", "number"},
			{"start_date", "datetime"},
		},
	}

	rows = []map[string]interface{}{
		{"location": "Auckland", "department": "Engineering", "salary": 120000, "start_date": "2016-01-23T12:00:00Z"},
		{"location": "Auckland", "department": "Engineering", "salary": 80000, "start_date": "2016-03-23T12:00:00Z"},
		{"location": "Auckland", "department": "Marketing", "salary": 90000, "start_date": "2016-01-23T12:00:00Z"},
		{"location": "Auckland", "department": "Marketing", "salary": 150000, "start_date": "2016-01-23T12:00:00Z"},
		{"location": "Wellington", "department": "Engineering", "salary": 120000, "start_date": "2016-01-23T12:00:00Z"},
		{"location": "Wellington", "department": "Engineering", "salary": 160000, "start_date": "2016-03-23T12:00:00Z"},
	}
)

func TestBucketByString(t *testing.T) {
	RegisterTestingT(t)
	dataset := &Dataset{
		Table: table,
	}

	err := dataset.AddRows(rows...)
	if err != nil {
		t.Fatalf("Unexpected error creating dataset: %s", err.Error())
	}

	query := &Query{
		Metrics: []Metric{
			{Type: "avg", Field: "salary"},
			{Type: "max", Field: "salary"},
			{Type: "min", Field: "salary"},
		},
		Bucket: &Bucket{
			Field: &Field{
				Name: "location",
				Type: "string",
			},
			Bucket: &Bucket{
				Field: &Field{
					Name: "department",
					Type: "string",
				},
			},
		},
	}

	results, err := dataset.Run(query)
	if err != nil {
		t.Fatalf("Unexpected error running query: %s", err.Error())
	}
	if results == nil {
		t.Fatalf("Unexpectedly got an empty resultset running query")
	}

	expected := Resultset{
		Buckets: map[string]*ResultBucket{
			"Auckland": {
				Value: "Auckland",
				Buckets: map[string]*ResultBucket{
					"Engineering": {
						Value:   "Engineering",
						Buckets: map[string]*ResultBucket{},
						Metrics: map[string]interface{}{
							"salary:avg": 100000,
							"salary:max": 120000,
							"salary:min": 80000,
						},
					},
					"Marketing": {
						Value:   "Marketing",
						Buckets: map[string]*ResultBucket{},
						Metrics: map[string]interface{}{
							"salary:avg": 120000,
							"salary:max": 150000,
							"salary:min": 90000,
						},
					},
				},
			},
			"Wellington": {
				Value: "Wellington",
				Buckets: map[string]*ResultBucket{
					"Engineering": {
						Value:   "Engineering",
						Buckets: map[string]*ResultBucket{},
						Metrics: map[string]interface{}{
							"salary:avg": 140000,
							"salary:max": 160000,
							"salary:min": 120000,
						},
					},
				},
			},
		},
	}
	rm, _ := json.Marshal(*results)
	em, _ := json.Marshal(expected)
	Expect(rm).To(MatchJSON(em))
}

func TestBucketByDate(t *testing.T) {
	RegisterTestingT(t)
	dataset := &Dataset{
		Table: table,
	}

	err := dataset.AddRows(rows...)
	if err != nil {
		t.Fatalf("Unexpected error creating dataset: %s", err.Error())
	}

	start := time.Date(2015, 12, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2016, 4, 1, 0, 0, 0, 0, time.UTC)
	query := &Query{
		Metrics: []Metric{
			{Type: "avg", Field: "salary"},
			{Type: "max", Field: "salary"},
			{Type: "min", Field: "salary"},
		},
		Bucket: &Bucket{
			Field: &Field{
				Name: "location",
				Type: "string",
			},
			Bucket: &Bucket{
				Field: &Field{
					Name: "start_date",
					Type: "datetime",
				},
				DatetimeOptions: &DatetimeBucketOptions{
					Period: Month,
					Start:  &start,
					End:    &end,
				},
			},
		},
	}

	results, err := dataset.Run(query)
	if err != nil {
		t.Fatalf("Unexpected error running query: %s", err.Error())
	}
	if results == nil {
		t.Fatalf("Unexpectedly got an empty resultset running query")
	}

	expected := Resultset{
		Buckets: map[string]*ResultBucket{
			"Auckland": {
				Value: "Auckland",
				Buckets: map[string]*ResultBucket{
					"2015-12-01T00:00:00Z": {
						Value:   "2015-12-01T00:00:00Z",
						Buckets: map[string]*ResultBucket{},
						Metrics: map[string]interface{}{
							"salary:avg": nil,
							"salary:max": nil,
							"salary:min": nil,
						},
					},
					"2016-01-01T00:00:00Z": {
						Value:   "2016-01-01T00:00:00Z",
						Buckets: map[string]*ResultBucket{},
						Metrics: map[string]interface{}{
							"salary:avg": 120000,
							"salary:max": 150000,
							"salary:min": 90000,
						},
					},
					"2016-02-01T00:00:00Z": {
						Value:   "2016-02-01T00:00:00Z",
						Buckets: map[string]*ResultBucket{},
						Metrics: map[string]interface{}{
							"salary:avg": nil,
							"salary:max": nil,
							"salary:min": nil,
						},
					},
					"2016-03-01T00:00:00Z": {
						Value:   "2016-03-01T00:00:00Z",
						Buckets: map[string]*ResultBucket{},
						Metrics: map[string]interface{}{
							"salary:avg": 80000,
							"salary:max": 80000,
							"salary:min": 80000,
						},
					},
					"2016-04-01T00:00:00Z": {
						Value:   "2016-04-01T00:00:00Z",
						Buckets: map[string]*ResultBucket{},
						Metrics: map[string]interface{}{
							"salary:avg": nil,
							"salary:max": nil,
							"salary:min": nil,
						},
					},
				},
			},
			"Wellington": {
				Value: "Wellington",
				Buckets: map[string]*ResultBucket{
					"2015-12-01T00:00:00Z": {
						Value:   "2015-12-01T00:00:00Z",
						Buckets: map[string]*ResultBucket{},
						Metrics: map[string]interface{}{
							"salary:avg": nil,
							"salary:max": nil,
							"salary:min": nil,
						},
					},
					"2016-01-01T00:00:00Z": {
						Value:   "2016-01-01T00:00:00Z",
						Buckets: map[string]*ResultBucket{},
						Metrics: map[string]interface{}{
							"salary:avg": 120000,
							"salary:max": 120000,
							"salary:min": 120000,
						},
					},
					"2016-02-01T00:00:00Z": {
						Value:   "2016-02-01T00:00:00Z",
						Buckets: map[string]*ResultBucket{},
						Metrics: map[string]interface{}{
							"salary:avg": nil,
							"salary:max": nil,
							"salary:min": nil,
						},
					},
					"2016-03-01T00:00:00Z": {
						Value:   "2016-03-01T00:00:00Z",
						Buckets: map[string]*ResultBucket{},
						Metrics: map[string]interface{}{
							"salary:avg": 160000,
							"salary:max": 160000,
							"salary:min": 160000,
						},
					},
					"2016-04-01T00:00:00Z": {
						Value:   "2016-04-01T00:00:00Z",
						Buckets: map[string]*ResultBucket{},
						Metrics: map[string]interface{}{
							"salary:avg": nil,
							"salary:max": nil,
							"salary:min": nil,
						},
					},
				},
			},
		},
	}
	rm, _ := json.MarshalIndent(*results, "", "  ")
	em, _ := json.MarshalIndent(expected, "", "  ")
	Expect(rm).To(MatchJSON(em))
}
