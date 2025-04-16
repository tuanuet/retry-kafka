package redis

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/tuanuet/retry-kafka/consumer"
	"github.com/tuanuet/retry-kafka/marshaller"
	"github.com/tuanuet/retry-kafka/producer"
	redisp "github.com/tuanuet/retry-kafka/producer/redis"
	"log"
	"reflect"
	"time"

	"github.com/IBM/sarama"
	"github.com/tuanuet/retry-kafka/retriable"
)

// Option ...
type Option func(*rConsumer)

// WithBatchFlush ...
func WithBatchFlush(size int32, timeFlush time.Duration) Option {
	return func(consumer *rConsumer) {
		consumer.batchFlushConf.size = size
		consumer.batchFlushConf.duration = timeFlush
	}
}

// WithMarshaler can overwrite marshaller want to send
func WithMarshaler(mr marshaller.Marshaller) Option {
	return func(k *rConsumer) {
		k.marshaller = mr
	}
}

// WithRetries can overwrite retry want to send
func WithRetries(opts []consumer.RetryOption) Option {
	return func(k *rConsumer) {
		k.retryConfigs = opts
	}
}

// WithLongProcessing ...
func WithLongProcessing(isLongProcessing bool) Option {
	return func(opt *rConsumer) {
		opt.isLongProcessing = isLongProcessing
	}
}

// WithLogger ...
func WithLogger(logger Logger) Option {
	return func(opt *rConsumer) {
		opt.logger = logger
	}
}

// WithEnableRetry ...
func WithEnableRetry(enable bool) Option {
	return func(opt *rConsumer) {
		opt.enableRetry = enable
	}
}

// WithEnableDlq ...
func WithEnableDlq(enable bool) Option {
	return func(opt *rConsumer) {
		opt.enableDlq = enable
	}
}

type Logger = sarama.StdLogger

type rConsumer struct {
	subscriberName string

	event  retriable.Event
	client *redis.Client

	brokers []string

	mainTopic   *retriable.Topic
	retryTopics []*retriable.Topic
	dlqTopic    *retriable.Topic

	nameToTopics map[string]*retriable.Topic
	publisher    producer.Producer

	enableRetry  bool
	retryConfigs []consumer.RetryOption

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
func NewConsumer(subscriberName string, event retriable.Event, brokers []string, options ...Option) *rConsumer {
	mainTopicName := retriable.NormalizeMainTopicName(event)
	c := &rConsumer{
		subscriberName: subscriberName,
		event:          event,
		retryTopics:    []*retriable.Topic{},
		nameToTopics:   make(map[string]*retriable.Topic),
		enableDlq:      true,
		enableRetry:    true,
		retryConfigs: []consumer.RetryOption{
			{Pending: 15 * time.Second},
			{Pending: 1 * time.Minute},
			{Pending: 10 * time.Minute},
		},
		client: redis.NewClient(&redis.Options{
			Addr: brokers[0],
		}),
		brokers:    brokers,
		marshaller: marshaller.DefaultMarshaller,
		logger:     log.Default(),
	}

	for _, opt := range options {
		opt(c)
	}

	// make publisher
	c.publisher = redisp.NewProducer(event, c.brokers)

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
func (r *rConsumer) Consume(ctx context.Context, handlerFunc consumer.HandleFunc) error {

	topicNames := []string{r.mainTopic.Name}
	for _, t := range r.retryTopics {
		topicNames = append(topicNames, t.Name)
	}

	for _, mainStream := range topicNames {
		_, err := r.client.XGroupCreateMkStream(ctx, mainStream, r.subscriberName, "0").Result()
		if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
			return fmt.Errorf("failed to create consumer group: %v", err)
		}
	}

	handler := newRedisSubscriberHandler(reflect.TypeOf(r.event), r, handlerFunc)

	// each stream is a goroutine
	for _, stream := range topicNames {
		go func(stream string) {
			consumerName := fmt.Sprintf("%s_%s", r.subscriberName, uuid.New().String())
			for {
				select {
				case <-ctx.Done():
					r.logger.Println("Shutting down consumer...")
					return
				default:
					// Read messages from all main streams
					args := &redis.XReadGroupArgs{
						Group:    r.subscriberName,
						Consumer: consumerName,
						Streams:  []string{stream, ">"},
						Count:    1,
						Block:    0,
						NoAck:    false,
					}
					r.logger.Println("running read group", stream)
					results, err := r.client.XReadGroup(ctx, args).Result()
					if err != nil {
						r.logger.Printf("Error reading streams: %v", err)
						continue
					}

					if err = handler.ConsumeClaim(ctx, r, results); err != nil {
						r.logger.Println("error when ConsumeClaim: %v", err)
						continue
					}
				}
			}
		}(stream)
	}

	return nil
}
func (r *rConsumer) Ack(ctx context.Context, stream, msgID string) error {
	_, err := r.client.XAck(ctx, stream, r.subscriberName, msgID).Result()
	return err
}

// ShouldReBalance check member of group is not empty
func (r *rConsumer) ShouldReBalance() (bool, error) {
	return false, nil
}

// BatchConsume is used to consume the batch messages.
func (r *rConsumer) BatchConsume(ctx context.Context, handlerFunc consumer.BatchHandleFunc) error {
	return nil
}

// sendRetry Retry sends the message to retry topic
func (r *rConsumer) sendRetry(msg retriable.Message) (err error) {
	if !r.enableRetry {
		return r.sendDQL(msg)
	}

	topic := r.getTopic(msg.GetTopicName())
	if topic.Next == nil {
		return r.sendDQL(msg)
	}
	var evt retriable.Event
	evt, err = msg.Unmarshal(reflect.TypeOf(r.event))
	if err != nil {
		return err
	}
	err = r.publisher.SendMessage(evt, msg.GetHeaders(), producer.WithTopic(topic.Next))
	return
}

// sendDQL sends a message DLQ topic
func (r *rConsumer) sendDQL(msg retriable.Message) (err error) {
	if !r.enableDlq {
		return
	}

	if r.dlqTopic != nil {
		// Should add Function for send raw message
		var evt retriable.Event
		evt, err = msg.Unmarshal(reflect.TypeOf(r.event))
		if err != nil {
			return err
		}

		err = r.publisher.SendMessage(evt, msg.GetHeaders(), producer.WithTopic(r.dlqTopic))
	}
	return
}

// getTopic returns the topic by given name.
func (r *rConsumer) getTopic(name string) *retriable.Topic {
	return r.nameToTopics[name]
}

func (r *rConsumer) Close() error {
	// TODO: close consumer
	return nil
}
