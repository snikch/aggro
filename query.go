package aggro

type Query struct {
	Bucket *Bucket
}

type Bucket struct {
	Metrics []Metric
	Bucket  *Bucket
	Field   *Field
}

type Metric struct {
	Type string
}
