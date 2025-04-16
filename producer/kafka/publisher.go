package kafka

import "github.com/IBM/sarama"

type Publisher interface {
	Close() error
	SendMessage(msg *sarama.ProducerMessage) error
}
