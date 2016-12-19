package aggro

import (
	"encoding/json"
	"testing"

	. "github.com/onsi/gomega"
)

func TestFlattenResultSetSimple(t *testing.T) {
	RegisterTestingT(t)
	expected := ResultTable{
		Rows: [][]map[string]interface{}{
			{
				{
					"salary:max": nil,
				},
				{
					"salary:max": 150000,
				},
				{
					"salary:max": 120000,
				},
			},
			{
				{
					"salary:max": nil,
				},
				{
					"salary:max": 120000,
				},
				{
					"salary:max": 220000,
				},
			},
		},
		RowTitles: [][]string{
			{"Auckland"},
			{"Wellington"},
		},
		ColumnTitles: [][]string{
			{"2015-12-01T00:00:00+13:00"},
			{"2016-01-01T00:00:00+13:00"},
			{"2016-02-01T00:00:00+13:00"},
		},
	}
	input := &Resultset{
		Buckets: map[string]*ResultBucket{
			"Auckland": {
				Value: "Auckland",
				Buckets: map[string]*ResultBucket{
					"2015-12-01T00:00:00+13:00": {
						Value:   "2015-12-01T00:00:00+13:00",
						Buckets: map[string]*ResultBucket{},
						Metrics: map[string]interface{}{
							"salary:max": nil,
						},
					},
					"2016-01-01T00:00:00+13:00": {
						Value:   "2016-01-01T00:00:00+13:00",
						Buckets: map[string]*ResultBucket{},
						Metrics: map[string]interface{}{
							"salary:max": 150000,
						},
					},
					"2016-02-01T00:00:00+13:00": {
						Value:   "2016-02-01T00:00:00+13:00",
						Buckets: map[string]*ResultBucket{},
						Metrics: map[string]interface{}{
							"salary:max": 120000,
						},
					},
				},
			},
			"Wellington": {
				Value: "Wellington",
				Buckets: map[string]*ResultBucket{
					"2015-12-01T00:00:00+13:00": {
						Value:   "2015-12-01T00:00:00+13:00",
						Buckets: map[string]*ResultBucket{},
						Metrics: map[string]interface{}{
							"salary:max": nil,
						},
					},
					"2016-01-01T00:00:00+13:00": {
						Value:   "2016-01-01T00:00:00+13:00",
						Buckets: map[string]*ResultBucket{},
						Metrics: map[string]interface{}{
							"salary:max": 120000,
						},
					},
					"2016-02-01T00:00:00+13:00": {
						Value:   "2016-02-01T00:00:00+13:00",
						Buckets: map[string]*ResultBucket{},
						Metrics: map[string]interface{}{
							"salary:max": 220000,
						},
					},
				},
			},
		},
	}
	results, err := Tabulate(input, 1)
	if err != nil {
		t.Fatalf("Unexpected error converting results: %s", err.Error())
	}

	rm, _ := json.MarshalIndent(*results, "", "  ")
	em, _ := json.MarshalIndent(expected, "", "  ")
	Expect(rm).To(MatchJSON(em))
}

func TestFlattenResultSetWithHoles(t *testing.T) {
	RegisterTestingT(t)
	expected := ResultTable{
		Rows: [][]map[string]interface{}{
			{
				{
					"salary:max": nil,
				},
				{
					"salary:max": 150000,
				},
				nil,
			},
			{
				{
					"salary:max": nil,
				},
				nil,
				{
					"salary:max": 120000,
				},
			},
		},
		RowTitles: [][]string{
			{"Auckland"},
			{"Wellington"},
		},
		ColumnTitles: [][]string{
			{"2015-12-01T00:00:00+13:00"},
			{"2016-01-01T00:00:00+13:00"},
			{"2016-02-01T00:00:00+13:00"},
		},
	}
	input := &Resultset{
		Buckets: map[string]*ResultBucket{
			"Auckland": {
				Value: "Auckland",
				Buckets: map[string]*ResultBucket{
					"2015-12-01T00:00:00+13:00": {
						Value:   "2015-12-01T00:00:00+13:00",
						Buckets: map[string]*ResultBucket{},
						Metrics: map[string]interface{}{
							"salary:max": nil,
						},
					},
					"2016-01-01T00:00:00+13:00": {
						Value:   "2016-01-01T00:00:00+13:00",
						Buckets: map[string]*ResultBucket{},
						Metrics: map[string]interface{}{
							"salary:max": 150000,
						},
					},
				},
			},
			"Wellington": {
				Value: "Wellington",
				Buckets: map[string]*ResultBucket{
					"2015-12-01T00:00:00+13:00": {
						Value:   "2015-12-01T00:00:00+13:00",
						Buckets: map[string]*ResultBucket{},
						Metrics: map[string]interface{}{
							"salary:max": nil,
						},
					},
					"2016-02-01T00:00:00+13:00": {
						Value:   "2016-02-01T00:00:00+13:00",
						Buckets: map[string]*ResultBucket{},
						Metrics: map[string]interface{}{
							"salary:max": 120000,
						},
					},
				},
			},
		},
	}
	results, err := Tabulate(input, 1)
	if err != nil {
		t.Fatalf("Unexpected error converting results: %s", err.Error())
	}

	rm, _ := json.MarshalIndent(*results, "", "  ")
	em, _ := json.MarshalIndent(expected, "", "  ")
	Expect(rm).To(MatchJSON(em))
}

func TestFlattenResultDeep(t *testing.T) {
	RegisterTestingT(t)
	expected := ResultTable{
		Rows: [][]map[string]interface{}{
			{
				{
					"salary:max": 1111,
				},
				nil,
				{
					"salary:max": 1121,
				},
			},
			{
				{
					"salary:max": 1211,
				},
				{
					"salary:max": 1212,
				},
				nil,
			},
			{
				nil,
				nil,
				{
					"salary:max": 2321,
				},
			},
		},
		RowTitles: [][]string{
			{"A1", "B1"},
			{"A1", "B2"},
			{"A2", "B3"},
		},
		ColumnTitles: [][]string{
			{"C1", "D1"},
			{"C1", "D2"},
			{"C2", "D1"},
		},
	}
	input := &Resultset{
		Buckets: map[string]*ResultBucket{
			"A1": {
				Value: "A1",
				Buckets: map[string]*ResultBucket{
					"B1": {
						Value: "B1",
						Buckets: map[string]*ResultBucket{
							"C1": {
								Value: "C1",
								Buckets: map[string]*ResultBucket{
									"D1": {
										Value:   "D1",
										Buckets: map[string]*ResultBucket{},
										Metrics: map[string]interface{}{
											"salary:max": 1111,
										},
									},
								},
							},
							"C2": {
								Value: "C2",
								Buckets: map[string]*ResultBucket{
									"D1": {
										Value:   "D1",
										Buckets: map[string]*ResultBucket{},
										Metrics: map[string]interface{}{
											"salary:max": 1121,
										},
									},
								},
							},
						},
					},
					"B2": {
						Value: "B2",
						Buckets: map[string]*ResultBucket{
							"C1": {
								Value: "C1",
								Buckets: map[string]*ResultBucket{
									"D1": {
										Value:   "D1",
										Buckets: map[string]*ResultBucket{},
										Metrics: map[string]interface{}{
											"salary:max": 1211,
										},
									},
									"D2": {
										Value:   "D2",
										Buckets: map[string]*ResultBucket{},
										Metrics: map[string]interface{}{
											"salary:max": 1212,
										},
									},
								},
							},
						},
					},
				},
			},
			"A2": {
				Value: "A2",
				Buckets: map[string]*ResultBucket{
					"B3": {
						Value: "B3",
						Buckets: map[string]*ResultBucket{
							"C2": {
								Value: "C2",
								Buckets: map[string]*ResultBucket{
									"D1": {
										Value:   "D1",
										Buckets: map[string]*ResultBucket{},
										Metrics: map[string]interface{}{
											"salary:max": 2321,
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}
	results, err := Tabulate(input, 2)
	if err != nil {
		t.Fatalf("Unexpected error converting results: %s", err.Error())
	}

	rm, _ := json.MarshalIndent(*results, "", "  ")
	em, _ := json.MarshalIndent(expected, "", "  ")
	Expect(rm).To(MatchJSON(em))
}
