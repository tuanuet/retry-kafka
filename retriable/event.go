package retriable

// Event abstraction for all struct
type Event interface {
	// GetTopicName should return main topic name
	GetTopicName() string
	// GetPartitionValue  should return value to
	GetPartitionValue() string
}

// NormalizeMainTopicName return main topic when consume
func NormalizeMainTopicName(e Event) string {
	return e.GetTopicName()
}
