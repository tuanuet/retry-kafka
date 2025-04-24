package producer

import "github.com/tuanuet/retry-kafka/v2/retriable"

type Producer interface {
	SendMessage(event retriable.Event, headers []*retriable.Header, opts ...SendOption) error
	Close() error
}

type SendOpt struct {
	Topic *retriable.Topic
}

// SendOption ...
type SendOption func(so *SendOpt)

// WithTopic can overwrite Topic want to send
func WithTopic(t *retriable.Topic) SendOption {
	return func(so *SendOpt) {
		so.Topic = t
	}
}
