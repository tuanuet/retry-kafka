package marshaler

import (
	jsoniter "github.com/json-iterator/go"
	"github.com/vmihailenco/msgpack/v5"
)

// Marshaler abstraction for marshal and unmarshal
type Marshaler interface {
	Marshal(v interface{}) ([]byte, error)
	Unmarshal(data []byte, v interface{}) error
}

var DefaultMarshaler = &JSONMarshaler{}

// JSONMarshaler /**
type JSONMarshaler struct {
}

func (json JSONMarshaler) Marshal(v interface{}) ([]byte, error) {
	return jsoniter.Marshal(v)
}

func (json JSONMarshaler) Unmarshal(data []byte, v interface{}) error {
	return jsoniter.Unmarshal(data, v)
}

// MsgpackMarshaler /**
type MsgpackMarshaler struct {
}

func (m MsgpackMarshaler) Marshal(v interface{}) ([]byte, error) {
	return msgpack.Marshal(v)
}

func (m MsgpackMarshaler) Unmarshal(data []byte, v interface{}) error {
	return msgpack.Unmarshal(data, v)
}
