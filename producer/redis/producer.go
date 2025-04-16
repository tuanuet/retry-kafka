package redis

import (
	"context"
	"fmt"
	"github.com/tuanuet/retry-kafka/marshaller"
	"github.com/tuanuet/retry-kafka/producer"
	"github.com/tuanuet/retry-kafka/retriable"
)

// Option ...
type Option func(*rProducer)

// WithMarshaller can overwrite marshaller want to send
func WithMarshaller(mr marshaller.Marshaller) Option {
	return func(k *rProducer) {
		k.marshaller = mr
	}
}

// WithAsync can write to kafka by async Publisher
func WithAsync() Option {
	return func(so *rProducer) {
		so.async = true
	}
}

type Publisher interface {
	Close() error
	SendMessage(ctx context.Context, msg *ProducerMessage) error
}

type rProducer struct {
	event    retriable.Event
	topic    *retriable.Topic
	async    bool
	brokers  []string
	producer Publisher

	marshaller marshaller.Marshaller
}

// NewProducer initial kafka Publisher
func NewProducer(event retriable.Event, brokers []string, options ...Option) *rProducer {
	p := &rProducer{
		event:   event,
		brokers: brokers,
		async:   false,
		topic:   retriable.NewTopic(retriable.NormalizeMainTopicName(event)),
		// default JSONMarshaler
		marshaller: marshaller.DefaultMarshaller,
	}

	for _, opt := range options {
		opt(p)
	}

	if err := p.initProducer(); err != nil {
		panic(fmt.Errorf("error when init producer: %v", err))
	}
	return p
}

// SendMessage ...
func (k *rProducer) SendMessage(event retriable.Event, headers []*retriable.Header, opts ...producer.SendOption) error {
	if event == nil {
		return fmt.Errorf("error event is nil")
	}
	body, err := k.marshaller.Marshal(event)
	if err != nil {
		panic(err)
	}

	so := &producer.SendOpt{
		Topic: k.topic,
	}

	for _, opt := range opts {
		opt(so)
	}

	newMsg := &ProducerMessage{
		Stream: so.Topic.Name,
		Key:    event.GetPartitionValue(),
		Value:  body,
	}
	/**
	|-------------------------------------------------------------------------
	| Merge header
	|-----------------------------------------------------------------------*/
	if len(headers) > 0 {
		headerMap := make(map[string]string)
		for _, header := range headers {
			headerMap[string(header.Key)] = string(header.Value)
		}
		newMsg.Header = headerMap
	}

	return k.producer.SendMessage(context.Background(), newMsg)
}

// Close ...
func (k *rProducer) Close() error {
	if err := k.producer.Close(); err != nil {
		return err
	}

	return nil
}

func (k *rProducer) initProducer() error {

	var err error
	k.producer, err = newSyncPublisher(k.brokers)
	return err
}
