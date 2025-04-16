package kafka

import (
	"fmt"

	"github.com/IBM/sarama"
)

type asyncPublisher struct {
	producer sarama.AsyncProducer
}

func newAsyncPublisher(brokers []string, cfg *sarama.Config) (*asyncPublisher, error) {
	ap, err := sarama.NewAsyncProducer(brokers, cfg)

	if err != nil {
		return nil, err
	}

	go func() {
		for errProducer := range ap.Errors() {
			fmt.Printf("error when Publisher async: %v", errProducer)
		}
	}()

	return &asyncPublisher{
		ap,
	}, nil
}

func (ap *asyncPublisher) SendMessage(msg *sarama.ProducerMessage) error {
	ap.producer.Input() <- msg
	return nil
}

func (ap *asyncPublisher) Close() error {
	return ap.producer.Close()
}
