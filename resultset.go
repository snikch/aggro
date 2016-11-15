package aggro

type Resultset struct {
	Errors  []error
	Buckets map[string]*ResultBucket
}

type ResultBucket struct {
	Value      string
	Metrics    map[string]interface{}
	Buckets    map[string]*ResultBucket
	sourceRows []map[string]Cell
}
