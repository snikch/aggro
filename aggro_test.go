package aggro

import (
	"encoding/json"
	"testing"

	. "github.com/onsi/gomega"
)

func TestFull(t *testing.T) {
	RegisterTestingT(t)
	table := &Table{
		Fields: []Field{
			{"location", "string"},
			{"department", "string"},
			// {"name", "string"},
			{"salary", "number"},
			{"start_date", "datetime"},
		},
	}

	dataset := &Dataset{
		Table: table,
	}

	err := dataset.AddRows([]map[string]interface{}{
		{"location": "Auckland", "department": "Engineering", "salary": 120000, "start_date": "2016-01-23T12:00:00Z"},
		{"location": "Auckland", "department": "Engineering", "salary": 80000, "start_date": "2016-03-23T12:00:00Z"},
		{"location": "Auckland", "department": "Marketing", "salary": 90000, "start_date": "2016-01-23T12:00:00Z"},
		{"location": "Auckland", "department": "Marketing", "salary": 150000, "start_date": "2016-01-23T12:00:00Z"},
		{"location": "Wellington", "department": "Engineering", "salary": 120000, "start_date": "2016-01-23T12:00:00Z"},
		{"location": "Wellington", "department": "Engineering", "salary": 160000, "start_date": "2016-03-23T12:00:00Z"},
	}...)
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
