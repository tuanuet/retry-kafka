package retriable

import "github.com/IBM/sarama"

var KafkaDefaultVersion = sarama.V2_6_0_0

// PublisherKafkaConfig is config of Kafka producer
type PublisherKafkaConfig struct {
	// BrokerAddrs list of broker addresses
	BrokerAddrs []string `json:"broker_addrs" mapstructure:"broker_addrs" yaml:"broker_addrs"`
	// KafkaConfig is kafka config
	KafkaConfig *sarama.Config
}

// Header contain in header of message
type Header struct {
	Key   []byte
	Value []byte
}
