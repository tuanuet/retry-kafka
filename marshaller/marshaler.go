package marshaller

import (
	"errors"
	jsoniter "github.com/json-iterator/go"
	"github.com/vmihailenco/msgpack/v5"
)

// Marshaller abstraction for marshal and unmarshal
type Marshaller interface {
	Marshal(v interface{}) ([]byte, error)
	Unmarshal(data []byte, v interface{}) error
}

var DefaultMarshaller = &JSONMarshaller{}

// JSONMarshaller /**
type JSONMarshaller struct {
}

func (json JSONMarshaller) Marshal(v interface{}) ([]byte, error) {
	return jsoniter.Marshal(v)
}

func (json JSONMarshaller) Unmarshal(data []byte, v interface{}) error {
	return jsoniter.Unmarshal(data, v)
}

// MsgpackMarshaller /**
type MsgpackMarshaller struct {
}

func (m MsgpackMarshaller) Marshal(v interface{}) ([]byte, error) {
	return msgpack.Marshal(v)
}

func (m MsgpackMarshaller) Unmarshal(data []byte, v interface{}) error {
	return msgpack.Unmarshal(data, v)
}

// ByteMarshaller ...
type ByteMarshaller struct {
}

func (m ByteMarshaller) Marshal(v interface{}) ([]byte, error) {
	switch data := v.(type) {
	case []byte:
		return data, nil
	case string:
		return []byte(data), nil
	default:
		return nil, errors.New("invalid byte marshaller")
	}
}

func (m ByteMarshaller) Unmarshal(data []byte, v interface{}) error {
	switch v.(type) {
	case *[]byte:
		*(v.(*[]byte)) = data
	default:
		return errors.New("invalid byte marshaller")
	}
	return nil
}
