package retriable

import (
	"bytes"
	"reflect"
	"time"

	"github.com/IBM/sarama"

	"github.com/tuanuet/retry-kafka/v2/marshaller"
)

type KafkaMessage struct {
	msg   sarama.ConsumerMessage
	value []byte
	topic string

	marshaller marshaller.Marshaller
}

// NewKafkaMessage create a new message.
func NewKafkaMessage(msg *sarama.ConsumerMessage, marshaller marshaller.Marshaller) Message {
	newMsg := &KafkaMessage{
		msg:        *msg,
		marshaller: marshaller,
	}
	return newMsg
}

// GetData gets data of msg
func (m *KafkaMessage) GetData() []byte {
	return m.msg.Value
}

// GetTopicName gets topic of msg
func (m *KafkaMessage) GetTopicName() string {
	return m.msg.Topic
}

// GetHeaders gets header of msg
func (m *KafkaMessage) GetHeaders() []*Header {
	headers := make([]*Header, 0)
	for _, header := range m.msg.Headers {
		headers = append(headers, &Header{
			Key:   header.Key,
			Value: header.Value,
		})
	}
	return headers
}

// GetSinceTime gets the remain time of a message before retried.
func (m *KafkaMessage) GetSinceTime() time.Duration {
	return time.Since(m.msg.Timestamp)
}

func (m *KafkaMessage) GetHeaderByKey(key []byte) []byte {
	for _, h := range m.msg.Headers {
		if h != nil && bytes.Equal(h.Key, key) {
			return h.Value
		}
	}
	return nil
}

func (m *KafkaMessage) SetHeaderByKey(key []byte, val []byte) {
	m.msg.Headers = append(m.msg.Headers, &sarama.RecordHeader{
		Key:   key,
		Value: val,
	})
}

func (m *KafkaMessage) GetRaw() interface{} {
	return m.msg
}

// Unmarshal ...
func (m *KafkaMessage) Unmarshal(evtType reflect.Type) (Event, error) {
	evtInstance := reflect.New(evtType.Elem()).Interface()

	if err := m.marshaller.Unmarshal(m.GetData(), evtInstance); err != nil {
		return nil, err
	}

	return evtInstance.(Event), nil
}
