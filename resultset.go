package aggro

type Resultset struct {
	Errors  []error                  `json:"errors"`
	Buckets map[string]*ResultBucket `json:"buckets"`
}

type ResultBucket struct {
	Value      string                   `json:"value"`
	Metrics    map[string]interface{}   `json:"metrics"`
	Buckets    map[string]*ResultBucket `json:"buckets"`
	sourceRows []map[string]Cell
}
