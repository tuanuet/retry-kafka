package marshaler

import (
	"errors"
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

// ByteMarshaler ...
type ByteMarshaler struct {
}

func (m ByteMarshaler) Marshal(v interface{}) ([]byte, error) {
	switch data := v.(type) {
	case []byte:
		return data, nil
	case string:
		return []byte(data), nil
	default:
		return nil, errors.New("invalid byte marshaler")
	}
}

func (m ByteMarshaler) Unmarshal(data []byte, v interface{}) error {
	switch v.(type) {
	case *[]byte:
		*(v.(*[]byte)) = data
	default:
		return errors.New("invalid byte marshaler")
	}
	return nil
}
