package producer

import (
	"github.com/IBM/sarama"
	"github.com/tuanuet/retry-kafka/marshaler"
	"github.com/tuanuet/retry-kafka/retriable"
)

type Producer interface {
	SendMessage(event retriable.Event, headers []*retriable.Header, opts ...SendOption) error
	Close() error
}

// Option ...
type Option func(*kProducer)

// WithMarshaler can overwrite marshaler want to send
func WithMarshaler(mr marshaler.Marshaler) Option {
	return func(k *kProducer) {
		k.marshaler = mr
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

type kProducer struct {
	event    retriable.Event
	topic    *retriable.Topic
	conf     config
	producer sarama.SyncProducer

	marshaler marshaler.Marshaler
}

// NewProducer initial kafka producer
func NewProducer(event retriable.Event, brokers []string, options ...Option) *kProducer {
	p := &kProducer{
		event: event,
		conf: config{
			Brokers:  brokers,
			KafkaCfg: newProducerKafkaConfig(),
		},
		topic: retriable.NewTopic(retriable.NormalizeMainTopicName(event)),
		// default JSONMarshaler
		marshaler: marshaler.DefaultMarshaler,
	}

	for _, opt := range options {
		opt(p)
	}

	producer, err := sarama.NewSyncProducer(brokers, p.conf.KafkaCfg)
	if err != nil {
		panic(err)
	}

	p.producer = producer
	return p
}

// SendMessage ...
func (p *kProducer) SendMessage(event retriable.Event, headers []*retriable.Header, opts ...SendOption) error {
	body, err := p.marshaler.Marshal(event)
	if err != nil {
		panic(err)
	}

	so := &sendOption{
		topic: p.topic,
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

	_, _, err = p.producer.SendMessage(newMsg)
	return err
}

// Close ...
func (k *kProducer) Close() error {
	if err := k.producer.Close(); err != nil {
		return err
	}

	return nil
}
