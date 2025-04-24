package retriable

import (
	"github.com/redis/go-redis/v9"
	"github.com/tuanuet/retry-kafka/v2/marshaller"
	"reflect"
	"testing"
)

func getTestRedisMessage() redis.XMessage {
	return redis.XMessage{
		Values: map[string]interface{}{
			"value":     "{\"foo\":\"bar\"}",
			"header":    "{\"k1\":\"v1\",\"k2\":\"v2\"}",
			"timestamp": "1700000000000",
		},
	}
}

func TestNewRedisMessage(t *testing.T) {
	msg := getTestRedisMessage()
	stream := "test-stream"
	mar := &marshaller.JSONMarshaller{}
	m := NewRedisMessage(msg, stream, mar)
	rmsg, ok := m.(*RMessage)
	if !ok {
		t.Fatalf("Expected *RMessage, got %T", m)
	}
	if rmsg.topic != stream {
		t.Errorf("Expected topic %s, got %s", stream, rmsg.topic)
	}
	if len(rmsg.headers) != 2 {
		t.Errorf("Expected 2 headers, got %d", len(rmsg.headers))
	}
	if rmsg.GetSinceTime() < 0 {
		t.Errorf("GetSinceTime should be >= 0")
	}
}

func TestGetHeaderByKey(t *testing.T) {
	msg := getTestRedisMessage()
	mar := &marshaller.JSONMarshaller{}
	m := NewRedisMessage(msg, "test", mar)
	rmsg := m.(*RMessage)
	val := rmsg.GetHeaderByKey([]byte("k1"))
	if string(val) != "v1" {
		t.Errorf("Expected header k1 to be v1, got %s", val)
	}
	val = rmsg.GetHeaderByKey([]byte("notfound"))
	if val != nil {
		t.Errorf("Expected nil for non-existent header, got %s", val)
	}
}

func TestSetHeaderByKey(t *testing.T) {
	msg := getTestRedisMessage()
	mar := &marshaller.JSONMarshaller{}
	m := NewRedisMessage(msg, "test", mar)
	rmsg := m.(*RMessage)
	rmsg.SetHeaderByKey([]byte("k3"), []byte("v3"))
	val := rmsg.GetHeaderByKey([]byte("k3"))
	if string(val) != "v3" {
		t.Errorf("Expected header k3 to be v3, got %s", val)
	}
}

func TestGetRaw(t *testing.T) {
	msg := getTestRedisMessage()
	mar := &marshaller.JSONMarshaller{}
	m := NewRedisMessage(msg, "test", mar)
	rmsg := m.(*RMessage)
	if !reflect.DeepEqual(rmsg.GetRaw(), msg) {
		t.Errorf("GetRaw should return original redis.XMessage")
	}
}

// DummyEvent for Unmarshal test
type DummyEvent struct {
	Foo string `json:"foo"`
}

func (d *DummyEvent) GetTopicName() string      { return "dummy" }
func (d *DummyEvent) GetPartitionValue() string { return "dummy" }

func TestUnmarshal(t *testing.T) {
	msg := getTestRedisMessage()
	mar := &marshaller.JSONMarshaller{}
	m := NewRedisMessage(msg, "test", mar)
	rmsg := m.(*RMessage)
	var evt DummyEvent
	out, err := rmsg.Unmarshal(reflect.TypeOf(&evt))
	if err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}
	devt, ok := out.(*DummyEvent)
	if !ok {
		t.Fatalf("Expected *DummyEvent, got %T", out)
	}
	if devt.Foo != "bar" {
		t.Errorf("Expected Foo to be 'bar', got %s", devt.Foo)
	}
}
