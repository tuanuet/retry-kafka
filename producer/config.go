package producer

import (
	"time"

	"github.com/IBM/sarama"
	"github.com/tuanuet/retry-kafka/retriable"
)

type config struct {
	Brokers  []string
	KafkaCfg *sarama.Config
}

// newProducerKafkaConfig is used to create config.
func newProducerKafkaConfig(async bool) *sarama.Config {
	/**
	|-------------------------------------------------------------------------
	| Sarama configuration
	|-----------------------------------------------------------------------*/
	kafkaConfig := sarama.NewConfig()
	kafkaConfig.Version = sarama.DefaultVersion
	kafkaConfig.Version = retriable.KafkaDefaultVersion
	kafkaConfig.Producer.Retry.Max = 5
	kafkaConfig.Producer.Flush.Frequency = 100 * time.Millisecond
	kafkaConfig.Producer.Flush.Messages = 500

	kafkaConfig.Producer.Return.Successes = !async
	kafkaConfig.Producer.Return.Errors = true

	kafkaConfig.Producer.RequiredAcks = sarama.WaitForAll
	kafkaConfig.Producer.Compression = sarama.CompressionLZ4

	return kafkaConfig
}
