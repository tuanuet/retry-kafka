package redis

import (
	"context"
	"fmt"
	"github.com/cespare/xxhash"
	"github.com/tuanuet/retry-kafka/v2/marshaller"
	"github.com/tuanuet/retry-kafka/v2/producer"
	"github.com/tuanuet/retry-kafka/v2/retriable"
)

// Option ...
type Option func(*rProducer)

// WithMarshaller can overwrite marshaller want to send
func WithMarshaller(mr marshaller.Marshaller) Option {
	return func(k *rProducer) {
		k.marshaller = mr
	}
}

// WithPartitionNum can write to partition
func WithPartitionNum(num int32) Option {
	return func(so *rProducer) {
		if num <= 0 {
			return
		}
		so.partitionNum = num
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

	marshaller   marshaller.Marshaller
	partitionNum int32
}

// NewProducer initial kafka Publisher
func NewProducer(event retriable.Event, brokers []string, options ...Option) *rProducer {
	p := &rProducer{
		event:   event,
		brokers: brokers,
		async:   false,
		topic:   retriable.NewTopic(retriable.NormalizeMainTopicName(event)),
		// default JSONMarshaler
		marshaller:   marshaller.DefaultMarshaller,
		partitionNum: 1,
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

	partition := xxhash.Sum64([]byte(event.GetPartitionValue())) % uint64(k.partitionNum)

	newMsg := &ProducerMessage{
		Stream:    so.Topic.Name,
		Key:       event.GetPartitionValue(),
		Value:     body,
		Partition: partition,
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
