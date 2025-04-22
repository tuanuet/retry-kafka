package retriable

import (
	"reflect"
	"time"
)

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

// Message is the interface of consumer message
type Message interface {
	GetTopicName() string
	GetHeaders() []*Header
	GetSinceTime() time.Duration
	GetHeaderByKey(key []byte) []byte
	SetHeaderByKey(key []byte, val []byte)
	GetRaw() interface{}
	Unmarshal(evtType reflect.Type) (Event, error)
}
