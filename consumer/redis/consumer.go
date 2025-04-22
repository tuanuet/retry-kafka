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
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/tuanuet/retry-kafka/retriable"
)

// Option ...
type Option func(*rConsumer)

// WithMarshaller can overwrite marshaller want to send
func WithMarshaller(mr marshaller.Marshaller) Option {
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

// WithUnOrder M:N subscribe
func WithUnOrder(unOrder bool) Option {
	return func(opt *rConsumer) {
		opt.unOrder = unOrder
	}
}

type Logger = sarama.StdLogger

type rConsumer struct {
	subscriberName   string
	consumerFullName string

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
	unOrder   bool

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
	c.consumerFullName = fmt.Sprintf("%s_%s", c.subscriberName, uuid.New().String())

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

	handler := newRedisSubscriberHandler(reflect.TypeOf(r.event), r, handlerFunc)

	// each stream is a goroutine
	for _, stream := range topicNames {
		go func(stream string) {
			if err := r.processStream(ctx, stream, handler); err != nil {
				r.logger.Printf("error when process stream %v\n", err)
			}
		}(stream)

	}

	return nil
}

func (r *rConsumer) processStream(ctx context.Context, stream string, handle *redisSubscriberHandler) error {
	keysCh, closeChan, err := r.getKeyDistributor(ctx, stream)
	if err != nil {
		return fmt.Errorf("error when get key distributor: %w", err)
	}

	// Create new context for new goroutines
	ctxPartition, cancelPartition := context.WithCancel(ctx)

	// Manager goroutine to handle key goroutines
	var wg sync.WaitGroup

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-closeChan:
				// Cancel existing goroutines
				cancelPartition()
				wg.Wait() // Wait for all goroutines to finish
				ctxPartition, cancelPartition = context.WithCancel(ctx)

			case keys, ok := <-keysCh:
				if !ok {
					return
				}

				r.logger.Printf("Starting new goroutines for keys: %v\n", keys)
				// Start a goroutine for each key
				for _, partition := range keys {
					wg.Add(1)
					go func(partition string) {
						defer wg.Done()
						// Process key until context is canceled
						for {
							select {
							case <-ctxPartition.Done():
								return
							default:
								// Placeholder: Process key continuously
								r.logger.Printf("Processing key %s\n", partition)

								if err = r.processPartition(ctxPartition, partition, handle); err != nil {
									r.logger.Printf("error processing partition %s: %v\n", partition, err)
									return
								}
							}
						}
					}(partition)
				}
			}
		}
	}()

	return nil
}

// getKeyDistributor creates a new KeyDistributor and initializes it with the given
// pattern and stream. The returned channels are used to send new keys from
// SubscribeRebalance and to signal when the distributor is closed. The returned
// error is used to signal any errors that occur during initialization.
//
// The returned channels have the following behavior:
//   - keysCh: receives new keys from SubscribeRebalance and sends them to the
//     caller. The channel is closed when the distributor is closed.
//   - closeChan: receives a signal when the distributor is closed. The channel
//     is closed when the distributor is closed.
//
// The distributor will register the sub-program with Redis and start watching for
// new keys. When new keys are received, they are sent to the caller through the
// keysCh channel. If the distributor is closed, the closeChan channel is closed.
//
// If the unOrder flag is set, the distributor will return all keys from Redis
// matching the given pattern and send them to the caller through the keysCh
// channel. The closeChan channel is not used in this case.
func (r *rConsumer) getKeyDistributor(ctx context.Context, stream string) (chan []string, chan struct{}, error) {
	pattern := fmt.Sprintf("%s:*", stream)
	keyDistributorPrefix := fmt.Sprintf("%s_%s", r.subscriberName, stream)
	config := Config{
		RedisClient:       r.client,
		Pattern:           pattern,
		ProcessID:         r.consumerFullName,
		Prefix:            keyDistributorPrefix,
		TTL:               10 * time.Second,
		StabilityDuration: 5 * time.Second,
		Logger:            r.logger,
	}

	// Channel to send new keys from SubscribeRebalance
	keysCh := make(chan []string, 1)
	closeChan := make(chan struct{})

	// Initialize distributor
	distributor, err := NewKeyDistributor(config)
	if err != nil {
		return keysCh, closeChan, fmt.Errorf("error to initialize distributor: %w", err)
	}

	rebalanceSignal := distributor.ConsumeRebalance(ctx)

	if r.unOrder {
		partitions, err := distributor.GetKeys(ctx, pattern)
		if err != nil {
			return keysCh, closeChan, err
		}
		keysCh <- partitions
		return keysCh, closeChan, nil
	}

	// Register the sub-program
	if err = distributor.Register(ctx); err != nil {
		return keysCh, closeChan, fmt.Errorf("error to register distributor: %w", err)
	}

	go func() {
		ttl := time.NewTicker(config.TTL / 2)
		stableCheck := time.NewTicker(time.Second)
		isRebalance := false
		for {
			select {
			case <-rebalanceSignal:
				closeChan <- struct{}{}
				isRebalance = true
			case <-stableCheck.C:
				if !isRebalance {
					continue
				}
				stable, err := distributor.IsStable(ctx)
				if err != nil {
					r.logger.Printf("error when check stable %v\n", err)
					continue
				}
				if !stable {
					continue
				}

				if isRebalance {
					ks, err := distributor.GetMyKeys(ctx)
					if err != nil {
						continue
					}
					keysCh <- ks
					isRebalance = false
				}

			case <-ttl.C:
				if err = distributor.KeepAlive(ctx); err != nil {
					r.logger.Printf("Keep-alive failed: %v", err)
				}
			}

		}
	}()

	return keysCh, closeChan, nil
}

func (r *rConsumer) processPartition(ctx context.Context, partitionStream string, handleFunc *redisSubscriberHandler) error {
	_, err := r.client.XGroupCreateMkStream(ctx, partitionStream, r.subscriberName, "0").Result()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		return fmt.Errorf("error creating partitionStream: %v", err)
	}

	for {
		select {
		case <-ctx.Done():
			r.logger.Printf("Shutting down consumer %s...\n", partitionStream)
			return nil
		default:
			// Read messages from all main streams
			args := &redis.XReadGroupArgs{
				Group:    r.subscriberName,
				Consumer: r.consumerFullName,
				Streams:  []string{partitionStream, ">"},
				Count:    1,
				Block:    0,
				NoAck:    false,
			}
			//r.logger.Println("running read group", partitionStream)
			results, err := r.client.XReadGroup(ctx, args).Result()
			if err != nil {
				r.logger.Printf("Error reading streams: %v", err)
				continue
			}

			if err = handleFunc.ConsumeClaim(ctx, r, results); err != nil {
				r.logger.Printf("error when ConsumeClaim: %v\n", err)
				continue
			}
		}
	}
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
		return r.sendDLQ(msg)
	}

	topic := r.getTopic(msg.GetTopicName())
	if topic.Next == nil {
		return r.sendDLQ(msg)
	}
	var evt retriable.Event
	evt, err = msg.Unmarshal(reflect.TypeOf(r.event))
	if err != nil {
		return err
	}
	err = r.publisher.SendMessage(evt, msg.GetHeaders(), producer.WithTopic(topic.Next))
	return
}

// sendDLQ sends a message DLQ topic
func (r *rConsumer) sendDLQ(msg retriable.Message) (err error) {
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
// The topic name is expected to be in the format "subscriberName:topicName".
// The function returns the topic with the given name, or nil if no such topic exists.
func (r *rConsumer) getTopic(name string) *retriable.Topic {
	// Find the last occurrence of the colon character to split the topic name
	// into the subscriber name and the topic name.
	last := strings.LastIndex(name, ":")
	// Return the topic with the given name.
	return r.nameToTopics[name[:last]]
}

func (r *rConsumer) Close() error {
	return r.client.Close()
}
