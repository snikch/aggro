package aggro

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
