package producer

import (
	"fmt"
	"github.com/tuanuet/retry-kafka/marshaller"

	"github.com/IBM/sarama"
	"github.com/tuanuet/retry-kafka/retriable"
)

type Producer interface {
	SendMessage(event retriable.Event, headers []*retriable.Header, opts ...SendOption) error
	Close() error
}

// Option ...
type Option func(*kProducer)

// WithMarshaler can overwrite marshaller want to send
func WithMarshaler(mr marshaller.Marshaller) Option {
	return func(k *kProducer) {
		k.marshaler = mr
	}
}

// WithAsync can write to kafka by async publisher
func WithAsync() Option {
	return func(so *kProducer) {
		so.async = true
	}
}

type sendOption struct {
	topic *retriable.Topic
}

// SendOption ...
type SendOption func(so *sendOption)

// WithTopic can overwrite topic want to send
func WithTopic(t *retriable.Topic) SendOption {
	return func(so *sendOption) {
		so.topic = t
	}
}

type publisher interface {
	Close() error
	SendMessage(msg *sarama.ProducerMessage) error
}

type kProducer struct {
	event    retriable.Event
	topic    *retriable.Topic
	async    bool
	conf     config
	producer publisher

	marshaler marshaller.Marshaller
}

// NewProducer initial kafka publisher
func NewProducer(event retriable.Event, brokers []string, options ...Option) *kProducer {
	p := &kProducer{
		event: event,
		conf: config{
			Brokers:  brokers,
			KafkaCfg: newProducerKafkaConfig(false),
		},
		async: false,
		topic: retriable.NewTopic(retriable.NormalizeMainTopicName(event)),
		// default JSONMarshaler
		marshaler: marshaller.DefaultMarshaller,
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
func (k *kProducer) SendMessage(event retriable.Event, headers []*retriable.Header, opts ...SendOption) error {
	body, err := k.marshaler.Marshal(event)
	if err != nil {
		panic(err)
	}

	so := &sendOption{
		topic: k.topic,
	}

	for _, opt := range opts {
		opt(so)
	}

	newMsg := &sarama.ProducerMessage{
		Topic: so.topic.Name,
		Key:   sarama.StringEncoder(event.GetPartitionValue()),
		Value: sarama.ByteEncoder(body),
	}
	/**
	|-------------------------------------------------------------------------
	| Merge header
	|-----------------------------------------------------------------------*/
	if len(headers) > 0 {
		newMsg.Headers = []sarama.RecordHeader{}
		for _, header := range headers {
			newMsg.Headers = append(newMsg.Headers, sarama.RecordHeader{
				Key:   header.Key,
				Value: header.Value,
			})
		}
	}

	return k.producer.SendMessage(newMsg)
}

// Close ...
func (k *kProducer) Close() error {
	if err := k.producer.Close(); err != nil {
		return err
	}

	return nil
}

func (k *kProducer) initProducer() error {
	k.conf.KafkaCfg = newProducerKafkaConfig(k.async)
	if !k.async {
		producer, err := newSyncPublisher(k.conf.Brokers, k.conf.KafkaCfg)
		k.producer = producer
		return err
	}

	producer, err := newAsyncPublisher(k.conf.Brokers, k.conf.KafkaCfg)
	k.producer = producer
	return err
}
