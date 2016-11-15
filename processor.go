package aggro

import (
	"errors"
	"fmt"
)

type queryProcessor struct {
	dataset     *Dataset
	query       *Query
	rootBuckets map[string]*ResultBucket
	tipBuckets  map[*ResultBucket]bool
	buckets     map[*Field]map[string]*ResultBucket
	measurables []*[]Cell
	rows        []*queryRow
	err         error
	results     *Resultset
}

type queryBucket struct {
	buckets map[string]*ResultBucket
	values  []*queryRow
}

type queryRow struct {
	Data map[string]Cell
	Tip  *ResultBucket
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
	p.rootBuckets = map[string]*ResultBucket{}
	p.tipBuckets = map[*ResultBucket]bool{}
	p.buckets = map[*Field]map[string]*ResultBucket{}

	// Decorate each of the dataset rows.
	for _, row := range p.dataset.Rows {
		p.rows = append(p.rows, &queryRow{
			Data: row,
		})
	}
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
	for i, row := range p.rows {
		buckets = p.recurse(0, i, row, p.query.Bucket, buckets)
	}
	p.results = &Resultset{
		Buckets: buckets,
	}
}
func (p *queryProcessor) recurse(depth, index int, row *queryRow, aggregate *Bucket, results map[string]*ResultBucket) map[string]*ResultBucket {
	// If there's no aggregate, we're done.
	if aggregate == nil {
		return results
	}

	// Grab the cell that we're aggregating on.
	cell := row.Data[aggregate.Field.Name]
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
		bucket.sourceRows = append(bucket.sourceRows, row.Data)
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
}
