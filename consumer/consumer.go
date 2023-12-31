package consumer

import (
	"context"
	"fmt"
	"github.com/tuanuet/retry-kafka/marshaler"
	"reflect"
	"time"

	"github.com/IBM/sarama"
	"github.com/tuanuet/retry-kafka/producer"
	"github.com/tuanuet/retry-kafka/retriable"
)

// Consumer ...
type Consumer interface {
	Consume(ctx context.Context, handlerFunc HandleFunc) error
	BatchConsume(ctx context.Context, handlerFunc BatchHandleFunc) error
	Close() error
}

// Option ...
type Option func(*kConsumer)

// WithBatchFlush ...
func WithBatchFlush(size int32, timeFlush time.Duration) Option {
	return func(consumer *kConsumer) {
		consumer.batchFlushConf.size = size
		consumer.batchFlushConf.duration = timeFlush
	}
}

// WithMarshaler can overwrite marshaler want to send
func WithMarshaler(mr marshaler.Marshaler) Option {
	return func(k *kConsumer) {
		k.marshaler = mr
	}
}

// WithRetries can overwrite retry want to send
func WithRetries(opts []RetryOption) Option {
	return func(k *kConsumer) {
		k.retryConfigs = opts
	}
}

type kConsumer struct {
	subscriberName string

	event         retriable.Event
	consumerGroup sarama.ConsumerGroup
	conf          config
	mainTopic     *retriable.Topic
	retryTopics   []*retriable.Topic
	dlqTopic      *retriable.Topic

	nameToTopics map[string]*retriable.Topic
	publisher    producer.Producer

	enableRetry  bool
	retryConfigs []RetryOption

	enableDlq bool

	batchFlushConf struct {
		size     int32
		duration time.Duration
	}

	marshaler marshaler.Marshaler
}

// NewConsumer ...
func NewConsumer(subscriberName string, event retriable.Event, brokers []string, options ...Option) *kConsumer {
	mainTopicName := retriable.NormalizeMainTopicName(event)
	c := &kConsumer{
		subscriberName: subscriberName,
		event:          event,
		retryTopics:    []*retriable.Topic{},
		nameToTopics:   make(map[string]*retriable.Topic),
		enableDlq:      true,
		enableRetry:    true,
		retryConfigs: []RetryOption{
			{Pending: 15 * time.Second},
			{Pending: 1 * time.Minute},
			{Pending: 10 * time.Minute},
		},

		conf:      config{Brokers: brokers, KafkaCfg: newConsumerKafkaConfig()},
		marshaler: marshaler.DefaultMarshaler,
	}

	for _, opt := range options {
		opt(c)
	}

	// make publisher
	c.publisher = producer.NewProducer(event, c.conf.Brokers)

	// Make main-topic
	mainTopic := retriable.NewTopic(mainTopicName)
	c.mainTopic = mainTopic
	c.nameToTopics[mainTopic.Name] = mainTopic

	// Make retry-topic
	if c.enableRetry {
		for _, retryConfig := range c.retryConfigs {
			topicName := fmt.Sprintf("%s_retry_%v", c.mainTopic.Name, retryConfig.Pending)
			topic := retriable.NewTopic(topicName, retriable.WithPending(retryConfig.Pending))
			c.retryTopics = append(c.retryTopics, topic)
			c.nameToTopics[topic.Name] = topic
			if len(c.retryTopics) == 1 {
				c.mainTopic.Next = topic
			} else {
				// last retry topic - 1
				previous := len(c.retryTopics) - 2
				c.retryTopics[previous].Next = topic
			}
		}
	}

	// Make dql-topic
	if c.enableDlq {
		topicName := fmt.Sprintf("%s_dlq", c.mainTopic.Name)
		topic := retriable.NewTopic(topicName)
		c.dlqTopic = topic
		c.nameToTopics[topic.Name] = topic
	}

	return c
}

// Consume is used to consume the message.
func (k *kConsumer) Consume(ctx context.Context, handlerFunc HandleFunc) error {
	var err error
	k.consumerGroup, err = sarama.NewConsumerGroup(k.conf.Brokers, k.subscriberName, k.conf.KafkaCfg)
	if err != nil {
		return err
	}
	topicNames := []string{k.mainTopic.Name}
	for _, t := range k.retryTopics {
		topicNames = append(topicNames, t.Name)
	}
	handler := newKafkaSubscriberHandler(reflect.TypeOf(k.event), k, handlerFunc)

	return k.consumerGroup.Consume(ctx, topicNames, handler)
}

// BatchConsume is used to consume the batch messages.
func (k *kConsumer) BatchConsume(ctx context.Context, handlerFunc BatchHandleFunc) error {
	var err error
	k.consumerGroup, err = sarama.NewConsumerGroup(k.conf.Brokers, k.subscriberName, k.conf.KafkaCfg)
	if err != nil {
		return err
	}
	topicNames := []string{k.mainTopic.Name}
	for _, t := range k.retryTopics {
		topicNames = append(topicNames, t.Name)
	}

	handler := newKafkaSubscriberBatchHandler(
		reflect.TypeOf(k.event),
		k,
		handlerFunc,
		withBatchConfig(k.batchFlushConf.size, k.batchFlushConf.duration),
	)

	return k.consumerGroup.Consume(ctx, topicNames, handler)
}

// sendRetry Retry sends the message to retry topic
func (k *kConsumer) sendRetry(msg *retriable.Message) (err error) {
	if !k.enableRetry {
		return k.sendDQL(msg)
	}

	topic := k.getTopic(msg.GetTopicName())
	if topic.Next == nil {
		return k.sendDQL(msg)
	}
	var evt retriable.Event
	evt, err = msg.Unmarshal(reflect.TypeOf(k.event))
	if err != nil {
		return err
	}
	err = k.publisher.SendMessage(evt, msg.GetHeaders(), producer.WithTopic(topic.Next))
	return
}

// sendDQL sends a message DLQ topic
func (k *kConsumer) sendDQL(msg *retriable.Message) (err error) {
	if !k.enableDlq {
		return
	}

	if k.dlqTopic != nil {
		// TODO: Should add Function for send raw message
		var evt retriable.Event
		evt, err = msg.Unmarshal(reflect.TypeOf(k.event))
		if err != nil {
			return err
		}

		err = k.publisher.SendMessage(evt, msg.GetHeaders(), producer.WithTopic(k.dlqTopic))
	}
	return
}

// getTopic returns the topic by given name.
func (k *kConsumer) getTopic(name string) *retriable.Topic {
	return k.nameToTopics[name]
}

func (k *kConsumer) Close() error {
	if err := k.consumerGroup.Close(); err != nil {
		return err
	}

	// TODO: flush all message on RAM

	// close publisher
	if err := k.publisher.Close(); err != nil {
		return err
	}
	return nil
}
