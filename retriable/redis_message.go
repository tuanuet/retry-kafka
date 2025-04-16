package retriable

import (
	"bytes"
	jsoniter "github.com/json-iterator/go"
	"github.com/redis/go-redis/v9"
	"reflect"
	"strconv"
	"time"

	"github.com/tuanuet/retry-kafka/marshaller"
)

type Message interface {
	GetTopicName() string
	GetHeaders() []*Header
	GetSinceTime() time.Duration
	GetHeaderByKey(key []byte) []byte
	SetHeaderByKey(key []byte, val []byte)
	GetRaw() interface{}
	Unmarshal(evtType reflect.Type) (Event, error)
}

type RMessage struct {
	msg        redis.XMessage
	value      []byte
	topic      string
	headers    []*Header
	timestamp  time.Time
	marshaller marshaller.Marshaller
}

// NewRedisMessage create a new message.
func NewRedisMessage(msg redis.XMessage, stream string, marshaller marshaller.Marshaller) Message {
	var headers []*Header
	if msg.Values["header"] != nil {
		var h map[string]string
		_ = jsoniter.Unmarshal([]byte(msg.Values["header"].(string)), &h)
		for k, v := range h {
			headers = append(headers, &Header{
				Key:   []byte(k),
				Value: []byte(v),
			})
		}
	}

	ts, err := strconv.Atoi(msg.Values["timestamp"].(string))
	if err != nil {
		ts = int(time.Now().UnixMilli())
	}

	newMsg := &RMessage{
		msg:        msg,
		value:      []byte(msg.Values["value"].(string)),
		topic:      stream,
		timestamp:  time.UnixMilli(int64(ts)),
		marshaller: marshaller,
		headers:    headers,
	}
	return newMsg
}

// GetData gets data of msg
func (m *RMessage) GetData() []byte {
	return m.value
}

// GetTopicName gets topic of msg
func (m *RMessage) GetTopicName() string {
	return m.topic
}

// GetHeaders gets header of msg
func (m *RMessage) GetHeaders() []*Header {
	return m.headers
}

// GetSinceTime gets the remain time of a message before retried.
func (m *RMessage) GetSinceTime() time.Duration {
	return time.Since(m.timestamp)
}

func (m *RMessage) GetHeaderByKey(key []byte) []byte {
	for _, h := range m.headers {
		if h != nil && bytes.Equal(h.Key, key) {
			return h.Value
		}
	}
	return nil
}

func (m *RMessage) SetHeaderByKey(key []byte, val []byte) {
	// TODO: set header for each type
	m.headers = append(m.headers, &Header{Key: key, Value: val})
}

func (m *RMessage) GetRaw() interface{} {
	return m.msg
}

// Unmarshal ...
func (m *RMessage) Unmarshal(evtType reflect.Type) (Event, error) {
	evtInstance := reflect.New(evtType.Elem()).Interface()

	if err := m.marshaller.Unmarshal(m.GetData(), evtInstance); err != nil {
		return nil, err
	}

	return evtInstance.(Event), nil
}
