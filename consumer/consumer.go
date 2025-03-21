package consumer

import (
	"context"
	"errors"
	"fmt"
	"github.com/tuanuet/retry-kafka/marshaller"
	"log"
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
	ShouldReBalance() (bool, error)
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

// WithMarshaler can overwrite marshaller want to send
func WithMarshaler(mr marshaller.Marshaller) Option {
	return func(k *kConsumer) {
		k.marshaller = mr
	}
}

// WithRetries can overwrite retry want to send
func WithRetries(opts []RetryOption) Option {
	return func(k *kConsumer) {
		k.retryConfigs = opts
	}
}

// WithMessageBytes can handle memory consume message
func WithMessageBytes(bytes int32) Option {
	return func(k *kConsumer) {
		k.conf.KafkaCfg.Consumer.Fetch.Default = bytes
	}
}

// WithMaxProcessDuration ...
func WithMaxProcessDuration(duration time.Duration) Option {
	return func(opt *kConsumer) {
		opt.conf.KafkaCfg.Consumer.MaxProcessingTime = duration
	}
}

// WithBalanceStrategy ...
func WithBalanceStrategy(balance sarama.BalanceStrategy) Option {
	return func(opt *kConsumer) {
		opt.conf.KafkaCfg.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{balance}
		opt.conf.KafkaCfg.Consumer.Group.Rebalance.Strategy = balance
	}
}

// WithSessionTimeout ...
func WithSessionTimeout(duration time.Duration) Option {
	return func(opt *kConsumer) {
		opt.conf.KafkaCfg.Consumer.Group.Session.Timeout = duration
	}
}

// WithHeartbeatInterval ...
func WithHeartbeatInterval(duration time.Duration) Option {
	return func(opt *kConsumer) {
		opt.conf.KafkaCfg.Consumer.Group.Heartbeat.Interval = duration
	}
}

// WithKafkaVersion ...
func WithKafkaVersion(version string) Option {
	return func(opt *kConsumer) {
		if ver, err := sarama.ParseKafkaVersion(version); err != nil {
			sarama.Logger.Printf("error when parse kafka version: %v", err)
		} else {
			opt.conf.KafkaCfg.Version = ver
		}
	}
}

// WithLongProcessing ...
func WithLongProcessing(isLongProcessing bool) Option {
	return func(opt *kConsumer) {
		opt.isLongProcessing = isLongProcessing
	}
}

// WithLogger ...
func WithLogger(logger Logger) Option {
	return func(opt *kConsumer) {
		opt.logger = logger
	}
}

// WithEnableRetry ...
func WithEnableRetry(enable bool) Option {
	return func(opt *kConsumer) {
		opt.enableRetry = enable
	}
}

// WithEnableDlq ...
func WithEnableDlq(enable bool) Option {
	return func(opt *kConsumer) {
		opt.enableDlq = enable
	}
}

type Logger = sarama.StdLogger

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
	isLongProcessing bool

	marshaller marshaller.Marshaller
	logger     Logger
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

		conf: config{
			Brokers:  brokers,
			KafkaCfg: newConsumerKafkaConfig(),
		},
		marshaller: marshaller.DefaultMarshaller,
		logger:     log.Default(),
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
			topicName := fmt.Sprintf("sub_%s_%s_retry_%v", c.subscriberName, c.mainTopic.Name, retryConfig.Pending)
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
		topicName := fmt.Sprintf("sub_%s_%s_dlq", c.subscriberName, c.mainTopic.Name)
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

	go func() {
		for {
			select {
			case <-ctx.Done():
				k.consumerGroup.Close()
				k.logger.Println("Shutting down consumer...")
				return
			default:
				if err = k.consumerGroup.Consume(ctx, topicNames, handler); err != nil {
					if errors.Is(err, sarama.ErrClosedConsumerGroup) {
						k.logger.Printf("error from consumer: %v", err)
						return
					}
					k.logger.Printf("error from consumer: %v", err)
					return
				}
				// check if context was cancelled, signaling that the consumer should stop
				if err = ctx.Err(); err != nil {
					k.logger.Printf("error from consumer: %v", err)
					return
				}
				handler.ready = make(chan bool)
			}
		}
	}()

	<-handler.ready

	return nil
}

// ShouldReBalance check member of group is not empty
func (k *kConsumer) ShouldReBalance() (bool, error) {
	admin, err := sarama.NewClusterAdmin(k.conf.Brokers, k.conf.KafkaCfg)
	if err != nil {
		return false, err
	}

	groups, err := admin.DescribeConsumerGroups([]string{k.subscriberName})
	if err != nil {
		return false, err
	}

	if len(groups) != 1 {
		return false, fmt.Errorf("error when fetch groups is not single: %v", groups)
	}
	group := groups[0]
	if !errors.Is(group.Err, sarama.ErrNoError) {
		return false, group.Err
	}

	if len(group.Members) == 0 {
		return true, nil
	}

	return false, nil
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

	go func() {
		for {
			select {
			case <-ctx.Done():
				k.consumerGroup.Close()
				k.logger.Println("Shutting down consumer...")
				return
			default:
				if err = k.consumerGroup.Consume(ctx, topicNames, handler); err != nil {
					if errors.Is(err, sarama.ErrClosedConsumerGroup) {
						k.logger.Printf("error from consumer: %v", err)
						return
					}
					k.logger.Printf("error from consumer: %v", err)
					return
				}
				// check if context was cancelled, signaling that the consumer should stop
				if err = ctx.Err(); err != nil {
					k.logger.Printf("error from consumer: %v", err)
					return
				}
				handler.ready = make(chan bool)
			}
		}
	}()

	<-handler.ready

	return nil
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
		// Should add Function for send raw message
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
	if k.consumerGroup == nil {
		return nil
	}
	if err := k.consumerGroup.Close(); err != nil {
		return err
	}
	return nil
}
