package redis

import (
	jsoniter "github.com/json-iterator/go"
	"time"
)

type Header map[string]string

type ProducerMessage struct {
	Header    Header
	Value     []byte
	Key       string
	Stream    string
	Timestamp time.Time
}

func (p *ProducerMessage) ToValues() map[string]interface{} {
	h, _ := jsoniter.Marshal(p.Header)

	return map[string]interface{}{
		"header":    h,
		"value":     p.Value,
		"key":       p.Key,
		"timestamp": p.Timestamp.UnixMilli(),
	}
}
