package aggro

type Sortable interface {
	Less(a, b *ResultBucket) bool
}

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

type AlphabeticalSortable bool

func (sortable *AlphabeticalSortable) Less(a, b *ResultBucket) bool {
	return a.Value < b.Value == bool(*sortable)
}
