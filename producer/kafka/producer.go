package kafka

import (
	"fmt"
	"github.com/IBM/sarama"
	"github.com/tuanuet/retry-kafka/marshaller"
	"github.com/tuanuet/retry-kafka/producer"
	"github.com/tuanuet/retry-kafka/retriable"
)

// Option ...
type Option func(*kProducer)

// WithMarshaler can overwrite marshaller want to send
func WithMarshaler(mr marshaller.Marshaller) Option {
	return func(k *kProducer) {
		k.marshaler = mr
	}
}

// WithAsync can write to kafka by async Publisher
func WithAsync() Option {
	return func(so *kProducer) {
		so.async = true
	}
}

type kProducer struct {
	event     retriable.Event
	topic     *retriable.Topic
	async     bool
	conf      config
	publisher Publisher

	marshaler marshaller.Marshaller
}

// NewProducer initial kafka Publisher
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
		panic(fmt.Errorf("error when init publisher: %v", err))
	}
	return p
}

// SendMessage ...
func (k *kProducer) SendMessage(event retriable.Event, headers []*retriable.Header, opts ...producer.SendOption) error {
	if event == nil {
		return fmt.Errorf("error event is nil")
	}
	body, err := k.marshaler.Marshal(event)
	if err != nil {
		panic(err)
	}

	so := &producer.SendOpt{
		Topic: k.topic,
	}

	for _, opt := range opts {
		opt(so)
	}

	newMsg := &sarama.ProducerMessage{
		Topic: so.Topic.Name,
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

	return k.publisher.SendMessage(newMsg)
}

// Close ...
func (k *kProducer) Close() error {
	if err := k.publisher.Close(); err != nil {
		return err
	}

	return nil
}

func (k *kProducer) initProducer() error {
	k.conf.KafkaCfg = newProducerKafkaConfig(k.async)
	if !k.async {
		publisher, err := newSyncPublisher(k.conf.Brokers, k.conf.KafkaCfg)
		k.publisher = publisher
		return err
	}

	publisher, err := newAsyncPublisher(k.conf.Brokers, k.conf.KafkaCfg)
	k.publisher = publisher
	return err
}
