package aggro

import (
	"encoding/json"
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
	if aggregate == nil {
		return results
	}
	// As long as we continue to have an aggregate bucket, loop.
	// For each of our rows, we need to add the value to exactly one bucket.
	// On the first aggregate, we add new buckets to the rootBuckets field,
	// whereas any other loop just adds to a lazily loaded bucket in the tree.
	// When there are no buckets left, we append the row itself as this is where
	// the data that is measured comes from.

	// for aggregate != nil {
	cell := row.Data[aggregate.Field.Name]
	if !cell.IsAggregatable() {
		p.err = fmt.Errorf("Non aggregatable cell found at depth %d, index %d", depth, index)
		return results
	}
	value := cell.AggregatableCell().Value()
	bucket := results[value]
	if bucket == nil {
		bucket = &ResultBucket{Value: value}
	}
	children := bucket.Buckets
	if children == nil {
		children = map[string]*ResultBucket{}
		bucket.Buckets = children
	}

	// If there's no next bucket, we're at the deepest point. Add data.
	if aggregate.Bucket == nil {
		bucket.sourceRows = append(bucket.sourceRows, row.Data)
		p.tipBuckets[bucket] = true
	}

	fmt.Println("All", results)
	depth++
	bucket.Buckets = p.recurse(depth, index, row, aggregate.Bucket, children)
	results[value] = bucket
	return results
}

func (p *queryProcessor) measure() {
	if p.err != nil {
		return
	}
	m, _ := json.MarshalIndent(p, "", "  ")
	fmt.Printf("%s\n", string(m))
}
