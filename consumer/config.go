package consumer

import (
	"log"
	"os"
	"time"

	"github.com/IBM/sarama"
	"github.com/tuanuet/retry-kafka/retriable"
)

type config struct {
	Brokers  []string
	KafkaCfg *sarama.Config
}

// newConsumerKafkaConfig is used to create config.
func newConsumerKafkaConfig() *sarama.Config {
	kafkaConfig := sarama.NewConfig()
	kafkaConfig.Version = retriable.KafkaDefaultVersion
	sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)

	kafkaConfig.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{
		sarama.NewBalanceStrategySticky(),
		sarama.NewBalanceStrategyRoundRobin(),
	}
	kafkaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	kafkaConfig.Consumer.Offsets.AutoCommit.Enable = false
	kafkaConfig.Consumer.Offsets.Retry.Max = 5
	kafkaConfig.Consumer.Group.Session.Timeout = 30 * time.Second
	kafkaConfig.Consumer.Group.Rebalance.Retry.Max = 10
	kafkaConfig.Consumer.Group.Rebalance.Retry.Backoff = 10 * time.Second
	kafkaConfig.Consumer.MaxProcessingTime = 5 * time.Second

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
