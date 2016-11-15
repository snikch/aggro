package aggro

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestFull(t *testing.T) {
	table := &Table{
		Fields: []Field{
			{"location", "string"},
			{"department", "string"},
			// {"name", "string"},
			{"salary", "number"},
			{"start_date", "datetime"},
		},
	}

	/*
	   type Dataset struct {
	   	Table Table
	   	Rows  [][]Cell
	   }

	*/
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
				Metrics: []Metric{
					{Type: "avg"},
					{Type: "max"},
					{Type: "min"},
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
						Value: "Engineering",
						Metrics: map[string]interface{}{
							"avg": 120000,
							"max": 80000,
							"min": 100000,
						},
					},
					"Marketing": {
						Value: "Marketing",
						Metrics: map[string]interface{}{
							"avg": 120000,
							"max": 150000,
							"min": 90000,
						},
					},
				},
			},
			"Wellington": {
				Value: "Wellington",
				Buckets: map[string]*ResultBucket{
					"Engineering": {
						Value: "Engineering",
						Metrics: map[string]interface{}{
							"avg": 140000,
							"max": 160000,
							"min": 120000,
						},
					},
				},
			},
		},
	}

	if !reflect.DeepEqual(*results, expected) {
		em, _ := json.MarshalIndent(expected, "", "  ")
		rm, _ := json.MarshalIndent(*results, "", "  ")
		t.Fatalf("Result did not match expectation:\n\nExpected:\n\n%s\n\nGot:\n\n%s", string(em), string(rm))
	}
	/*
		  Buckets []ResultBucket
		}

		type ResultBucket struct {
			Value   string
			Metrics map[string]interface{}
			Buckets []ResultBucket
		}

	*/

}
