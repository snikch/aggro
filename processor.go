package aggro

import (
	"errors"
	"fmt"
)

type queryProcessor struct {
	dataset     *Dataset
	query       *Query
	tipBuckets  map[*ResultBucket]bool
	measurables []*[]Cell
	err         error
	results     *Resultset
}

func (p *queryProcessor) Run() (*Resultset, error) {
	p.prepare()
	p.aggregate()
	p.measure()
	return p.results, p.err
}

func (p *queryProcessor) prepare() {
	if p.err != nil {
		return
	}

	// Initialise the root & tip buckets, and full bucket lookup.
	p.tipBuckets = map[*ResultBucket]bool{}
}

// aggregate is responsible for sorting the dataset's rows into buckets.
func (p *queryProcessor) aggregate() {
	if p.err != nil {
		return
	}
	if p.query.Bucket == nil {
		p.err = errors.New("Query has no root bucket")
		return
	}
	// Loop over each row, adding all nest query buckets to the value buckets.
	buckets := map[string]*ResultBucket{}
	for i, row := range p.dataset.Rows {
		buckets = p.recurse(0, i, row, p.query.Bucket, buckets)
	}
	p.results = &Resultset{
		Buckets: buckets,
	}
}
func (p *queryProcessor) recurse(depth, index int, row map[string]Cell, aggregate *Bucket, results map[string]*ResultBucket) map[string]*ResultBucket {
	// If there's no aggregate, we're done.
	if aggregate == nil {
		return results
	}

	// Grab the cell that we're aggregating on.
	cell := row[aggregate.Field.Name]
	if !cell.IsAggregatable() {
		p.err = fmt.Errorf("Non aggregatable cell found at depth %d, index %d", depth, index)
		return results
	}

	// And grab the underlying aggregatable string value.
	value := cell.AggregatableCell().Value()

	// Ensure we have a result bucket for this value, making on if we don't.
	bucket := results[value]
	if bucket == nil {
		bucket = &ResultBucket{
			Value:   value,
			Buckets: map[string]*ResultBucket{},
		}
	}

	// If there's no next bucket, we're at the deepest point. Add data to measure.
	if aggregate.Bucket == nil {
		bucket.sourceRows = append(bucket.sourceRows, row)
		p.tipBuckets[bucket] = true
	}

	// Bump depth and recurse to next level, passing in the children as the results.
	depth++
	bucket.Buckets = p.recurse(depth, index, row, aggregate.Bucket, bucket.Buckets)
	// Update the current results bucket with the new values, then return.
	results[value] = bucket
	return results
}

func (p *queryProcessor) measure() {
	if p.err != nil {
		return
	}

	//sourceRows []map[string]Cell
	for bucket, _ := range p.tipBuckets {
		// Create measurers for each of the metrics, then feed data into them.
		measurers := make([]measurer, len(p.query.Metrics))
		for i, metric := range p.query.Metrics {
			measurers[i], p.err = metric.measurer()
			if p.err != nil {
				return
			}
		}

		for i := range p.query.Metrics {
			metric := &p.query.Metrics[i]
			for j := range bucket.sourceRows {
				row := bucket.sourceRows[j]
				measurers[i].AddDatum(row[metric.Field].MeasurableCell().Value())
			}
		}
		bucket.Metrics = map[string]interface{}{}
		for i, _ := range p.query.Metrics {
			metric := &p.query.Metrics[i]
			bucket.Metrics[metric.Field+":"+metric.Type] = measurers[i].Result()
		}
	}
}
