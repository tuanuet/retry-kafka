package producer

import (
	"github.com/IBM/sarama"
	"github.com/tuanuet/retry-kafka/retriable"
)

type config struct {
	Brokers  []string
	KafkaCfg *sarama.Config
}

// newProducerKafkaConfig is used to create config.
func newProducerKafkaConfig() *sarama.Config {
	/**
	|-------------------------------------------------------------------------
	| Sarama configuration
	|-----------------------------------------------------------------------*/
	kafkaConfig := sarama.NewConfig()
	kafkaConfig.ChannelBufferSize = 128 //reduce /2x from default of sarama lib
	kafkaConfig.Version = retriable.KafkaDefaultVersion
	kafkaConfig.Producer.Retry.Max = 5
	kafkaConfig.Producer.Return.Successes = true
	kafkaConfig.Producer.Return.Errors = true
	kafkaConfig.Producer.RequiredAcks = sarama.WaitForAll

	return kafkaConfig
}
