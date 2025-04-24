package consumer

import (
	"context"
	"time"

	"github.com/tuanuet/retry-kafka/v2/retriable"
)

// RetryOption is the option for retry topic.
type RetryOption struct {
	Pending time.Duration
}

// HandleFunc ...
type HandleFunc func(evt retriable.Event, headers []*retriable.Header) error

// BatchHandleFunc ...
type BatchHandleFunc func(evts []retriable.Event, headers [][]*retriable.Header) error

// Consumer ...
type Consumer interface {
	Consume(ctx context.Context, handlerFunc HandleFunc) error
	BatchConsume(ctx context.Context, handlerFunc BatchHandleFunc) error
	ShouldReBalance() (bool, error)
	Close() error
}
