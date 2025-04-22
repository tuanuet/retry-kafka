package redis

import (
	"testing"
	"time"
	"encoding/json"
)

func TestProducerMessage_ToValues(t *testing.T) {
	headers := Header{"foo": "bar", "baz": "qux"}
	val := []byte("test-value")
	key := "test-key"
	stream := "test-stream"
	ts := time.Now()
	msg := &ProducerMessage{
		Header: headers,
		Value:  val,
		Key:    key,
		Stream: stream,
		Timestamp: ts,
	}
	values := msg.ToValues()

	headerJson, ok := values["header"].([]byte)
	if !ok {
		t.Fatalf("header field should be []byte, got %T", values["header"])
	}
	var decoded Header
	if err := json.Unmarshal(headerJson, &decoded); err != nil {
		t.Errorf("Header json unmarshal failed: %v", err)
	}
	if decoded["foo"] != "bar" || decoded["baz"] != "qux" {
		t.Errorf("Header values not match, got %+v", decoded)
	}

	if string(values["value"].([]byte)) != "test-value" {
		t.Errorf("Value not match, got %s", values["value"])
	}
	if values["key"].(string) != key {
		t.Errorf("Key not match, got %s", values["key"])
	}
	if values["timestamp"].(int64) != ts.UnixMilli() {
		t.Errorf("Timestamp not match, got %v", values["timestamp"])
	}
}
