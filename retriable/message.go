package retriable

import (
	"bytes"
	"github.com/tuanuet/retry-kafka/marshaller"
	"reflect"
	"time"

	"github.com/IBM/sarama"
)

type Message struct {
	msg       sarama.ConsumerMessage
	marshaler marshaller.Marshaller
}

// NewMessage create a new message.
func NewMessage(msg *sarama.ConsumerMessage, marshaler marshaller.Marshaller) *Message {
	newMsg := &Message{
		msg:       *msg,
		marshaler: marshaler,
	}
	return newMsg
}

// GetData gets data of msg
func (m *Message) GetData() []byte {
	return m.msg.Value
}

// GetTopicName gets topic of msg
func (m *Message) GetTopicName() string {
	return m.msg.Topic
}

// GetHeaders gets header of msg
func (m *Message) GetHeaders() []*Header {
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
func (m *Message) GetSinceTime() time.Duration {
	return time.Since(m.msg.Timestamp)
}

func (m *Message) GetHeaderByKey(key []byte) []byte {
	for _, h := range m.msg.Headers {
		if h != nil && bytes.Equal(h.Key, key) {
			return h.Value
		}
	}
	return nil
}

func (m *Message) SetHeaderByKey(key []byte, val []byte) {
	m.msg.Headers = append(m.msg.Headers, &sarama.RecordHeader{
		Key:   key,
		Value: val,
	})
}

func (m *Message) GetRaw() sarama.ConsumerMessage {
	return m.msg
}

// Unmarshal ...
func (m *Message) Unmarshal(evtType reflect.Type) (Event, error) {
	evtInstance := reflect.New(evtType.Elem()).Interface()

	if err := m.marshaler.Unmarshal(m.GetData(), evtInstance); err != nil {
		return nil, err
	}

	return evtInstance.(Event), nil
}
