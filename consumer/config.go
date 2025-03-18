package consumer

import (
	"time"

	"github.com/IBM/sarama"
	"github.com/rcrowley/go-metrics"

	"github.com/tuanuet/retry-kafka/retriable"
)

type config struct {
	Brokers  []string
	KafkaCfg *sarama.Config
}

// newConsumerKafkaConfig is used to create config.
func newConsumerKafkaConfig() *sarama.Config {
	// sometime memory leak when default=false
	metrics.UseNilMetrics = true

	kafkaConfig := sarama.NewConfig()
	kafkaConfig.Version = sarama.V3_1_0_0
	//sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)

	kafkaConfig.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{
		sarama.NewBalanceStrategySticky(),
		sarama.NewBalanceStrategyRoundRobin(),
	}
	kafkaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	kafkaConfig.Consumer.Offsets.AutoCommit.Enable = true
	kafkaConfig.Consumer.Offsets.AutoCommit.Interval = 100 * time.Millisecond
	kafkaConfig.Consumer.Offsets.Retry.Max = 5
	kafkaConfig.Consumer.Group.Session.Timeout = 30 * time.Second
	kafkaConfig.Consumer.Group.Heartbeat.Interval = 10 * time.Second
	kafkaConfig.Consumer.Group.Rebalance.Retry.Max = 5
	kafkaConfig.Consumer.Group.Rebalance.Retry.Backoff = 1 * time.Second
	kafkaConfig.Consumer.MaxProcessingTime = 30 * time.Second
	kafkaConfig.Consumer.Fetch.Default = 64 * 1024
	kafkaConfig.Consumer.Fetch.Max = 1024 * 1024

	kafkaConfig.MetricRegistry.UnregisterAll()

	kafkaConfig.Consumer.Group.Heartbeat.Interval = 10 * time.Second

	return kafkaConfig
}

// RetryOption is the option for retry topic.
type RetryOption struct {
	Pending time.Duration
}

// HandleFunc ...
type HandleFunc func(evt retriable.Event, headers []*retriable.Header) error

// BatchHandleFunc ...
type BatchHandleFunc func(evts []retriable.Event, headers [][]*retriable.Header) error
