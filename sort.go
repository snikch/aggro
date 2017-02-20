package aggro

import "sort"

// Sortable provides an interface that various sorters can implement to compare
// two result buckets. This enables access to both the value and any metrics
// that the query may have contained.
type Sortable interface {
	Less(a, b *ResultBucket) bool
}

// sortableForOptions returns the appropriate sorter for the supplied sort
// options. If the options type provided is invalid, it is ignored an no sorter
// is returned. A nil response here will end up with results being in a
// non-deterministic order due to randomisation when ranging over maps.
func sortableForOptions(options *SortOptions) Sortable {
	if options == nil {
		return nil
	}
	switch options.Type {
	case "alphabetical":
		s := AlphabeticalSortable(!options.Desc)
		return &s
	}
	return nil
}

// AlphabeticalSortable is a simple  sorter that sorts alphabetically in the
// direction of the boolean, true meaning ascending and false being descending.
type AlphabeticalSortable bool

// Less implements Sortable by comparing the values field of each result.
func (sortable *AlphabeticalSortable) Less(a, b *ResultBucket) bool {
	return a.Value < b.Value == bool(*sortable)
}

// bucketSorter is an implementation of the sort.Sort interface that is capable
// or sorting the supplied slice of results with the supplied Sortable.
type bucketSorter struct {
	results  []*ResultBucket
	sortable Sortable
}

// Len implements the sort.Sort interface Len method.
func (sorter *bucketSorter) Len() int {
	return len(sorter.results)
}

// Swap implements the sort.Sort interface Swap method.
func (sorter *bucketSorter) Swap(i, j int) {
	sorter.results[i], sorter.results[j] = sorter.results[j], sorter.results[i]
}

// Less implement the sort.Sort less method by calling the Sortable that the
// sorter has been supplied, with the two appropriate results.
func (sorter *bucketSorter) Less(i, j int) bool {
	return sorter.sortable.Less(sorter.results[i], sorter.results[j])
}

// sortMap takes a map of results and returns it as a sorted slice.
func sortMap(bucket *Bucket, results map[string]*ResultBucket) []*ResultBucket {
	resultSlice := []*ResultBucket{}
	for _, result := range results {
		resultSlice = append(resultSlice, result)
	}
	return sortSlice(bucket, resultSlice)
}

// sortSlice takes a slice of results and sorts it via a bucketSorter instance.
func sortSlice(bucket *Bucket, results []*ResultBucket) []*ResultBucket {
	sorter := &bucketSorter{
		results:  results,
		sortable: sortableForOptions(bucket.Sort),
	}
	if sorter.sortable != nil {
		sort.Sort(sorter)
	}
	for _, result := range sorter.results {
		if bucket.Bucket != nil {
			result.Buckets = sortMap(bucket.Bucket, result.bucketLookup)
		}
	}
	return sorter.results
}
