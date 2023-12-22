package retriable

import (
	"time"
)

// TopicOption ...
type TopicOption func(topic *Topic)

// WithPending use for retry job
func WithPending(pending time.Duration) TopicOption {
	return func(topic *Topic) {
		topic.Pending = pending
	}
}

// Topic action about topic
type Topic struct {
	Name string

	Pending time.Duration

	// next topic should be push to
	Next *Topic
}

// NewTopic init new topic
func NewTopic(name string, options ...TopicOption) *Topic {
	t := &Topic{
		Name:    name,
		Pending: 0,
		Next:    nil,
	}

	for _, option := range options {
		option(t)
	}

	return t
}
