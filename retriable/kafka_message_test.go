package retriable

import (
	"reflect"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/tuanuet/retry-kafka/v2/marshaller"
)

func getTestKafkaMessage() *sarama.ConsumerMessage {
	return &sarama.ConsumerMessage{
		Topic: "test-topic",
		Value: []byte("{\"foo\":\"bar\"}"),
		Headers: []*sarama.RecordHeader{
			{Key: []byte("k1"), Value: []byte("v1")},
			{Key: []byte("k2"), Value: []byte("v2")},
		},
		Timestamp: time.Now().Add(-time.Minute),
	}
}

func TestNewKafkaMessage(t *testing.T) {
	msg := getTestKafkaMessage()
	mar := &marshaller.JSONMarshaller{}
	m := NewKafkaMessage(msg, mar)
	kmsg, ok := m.(*KafkaMessage)
	if !ok {
		t.Fatalf("Expected *KafkaMessage, got %T", m)
	}
	if kmsg.GetTopicName() != msg.Topic {
		t.Errorf("Expected topic %s, got %s", msg.Topic, kmsg.GetTopicName())
	}
	if len(kmsg.GetHeaders()) != 2 {
		t.Errorf("Expected 2 headers, got %d", len(kmsg.GetHeaders()))
	}
	if kmsg.GetSinceTime() < 0 {
		t.Errorf("GetSinceTime should be >= 0")
	}
}

func TestKafkaGetHeaderByKey(t *testing.T) {
	msg := getTestKafkaMessage()
	mar := &marshaller.JSONMarshaller{}
	m := NewKafkaMessage(msg, mar)
	kmsg := m.(*KafkaMessage)
	val := kmsg.GetHeaderByKey([]byte("k1"))
	if string(val) != "v1" {
		t.Errorf("Expected header k1 to be v1, got %s", val)
	}
	val = kmsg.GetHeaderByKey([]byte("notfound"))
	if val != nil {
		t.Errorf("Expected nil for non-existent header, got %s", val)
	}
}

func TestKafkaSetHeaderByKey(t *testing.T) {
	msg := getTestKafkaMessage()
	mar := &marshaller.JSONMarshaller{}
	m := NewKafkaMessage(msg, mar)
	kmsg := m.(*KafkaMessage)
	kmsg.SetHeaderByKey([]byte("k3"), []byte("v3"))
	val := kmsg.GetHeaderByKey([]byte("k3"))
	if string(val) != "v3" {
		t.Errorf("Expected header k3 to be v3, got %s", val)
	}
}

func TestKafkaGetRaw(t *testing.T) {
	msg := getTestKafkaMessage()
	mar := &marshaller.JSONMarshaller{}
	m := NewKafkaMessage(msg, mar)
	kmsg := m.(*KafkaMessage)
	if !reflect.DeepEqual(kmsg.GetRaw(), *msg) {
		t.Errorf("GetRaw should return original sarama.ConsumerMessage")
	}
}

// KafkaDummyEvent for Unmarshal test
// Implements Event interface

type KafkaDummyEvent struct {
	Foo string `json:"foo"`
}

func (d *KafkaDummyEvent) GetTopicName() string      { return "dummy" }
func (d *KafkaDummyEvent) GetPartitionValue() string { return "dummy" }

func TestKafkaUnmarshal(t *testing.T) {
	msg := getTestKafkaMessage()
	mar := &marshaller.JSONMarshaller{}
	m := NewKafkaMessage(msg, mar)
	kmsg := m.(*KafkaMessage)
	var evt KafkaDummyEvent
	out, err := kmsg.Unmarshal(reflect.TypeOf(&evt))
	if err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}
	devt, ok := out.(*KafkaDummyEvent)
	if !ok {
		t.Fatalf("Expected *KafkaDummyEvent, got %T", out)
	}
	if devt.Foo != "bar" {
		t.Errorf("Expected Foo to be 'bar', got %s", devt.Foo)
	}
}
