package producer

import (
	"github.com/IBM/sarama"
)

type syncPublisher struct {
	producer sarama.SyncProducer
}

func newSyncPublisher(brokers []string, cfg *sarama.Config) (*syncPublisher, error) {
	ap, err := sarama.NewSyncProducer(brokers, cfg)

	if err != nil {
		return nil, err
	}

	return &syncPublisher{
		ap,
	}, nil
}

func (ap *syncPublisher) SendMessage(msg *sarama.ProducerMessage) error {
	_, _, err := ap.producer.SendMessage(msg)
	return err
}

func (ap *syncPublisher) Close() error {
	return ap.producer.Close()
}
