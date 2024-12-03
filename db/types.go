package db

// KVPair represents a key-value pair stored in the database
type KVPair struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}
