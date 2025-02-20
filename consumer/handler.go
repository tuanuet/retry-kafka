package consumer

import (
	"errors"
	"reflect"
	"time"

	"github.com/IBM/sarama"
	"github.com/tuanuet/retry-kafka/retriable"
)

// kafkaSubscriberBatchHandler is the struct of handler.
type kafkaSubscriberHandler struct {
	process    HandleFunc
	subscriber *kConsumer
	evtType    reflect.Type
}

// newKafkaSubscriberHandler create an instance from consumer.
func newKafkaSubscriberHandler(evtType reflect.Type, subscriber *kConsumer, handler HandleFunc) kafkaSubscriberHandler {
	h := kafkaSubscriberHandler{
		subscriber: subscriber,
		process:    handler,
		evtType:    evtType,
	}
	return h
}

// Setup implements the method of interface.
func (kafkaSubscriberHandler) Setup(_ sarama.ConsumerGroupSession) error {
	return nil
}

// Cleanup implements the method of interface.
func (kafkaSubscriberHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim implements the method of interface.
func (h kafkaSubscriberHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		newMsg := retriable.NewMessage(msg, h.subscriber.marshaller)
		topic := h.subscriber.getTopic(newMsg.GetTopicName())
		since := newMsg.GetSinceTime()

		topicPause := map[string][]int32{msg.Topic: {msg.Partition}}
		if topic.Pending-since > time.Millisecond*100 {
			// pause consume
			h.subscriber.consumerGroup.Pause(topicPause)
			time.Sleep(topic.Pending - since)
		}

		if !h.subscriber.isLongProcessing {
			h.subscriber.consumerGroup.Resume(topicPause)
		}
		if err := h.handleMessage(newMsg); err != nil {
			// resume when done handler
			if h.subscriber.isLongProcessing {
				h.subscriber.consumerGroup.Resume(topicPause)
			}

			// Append more header
			newMsg.SetHeaderByKey([]byte("_retry_error"), []byte(err.Error()))

			if ok := errors.Is(err, retriable.ErrorWithoutRetry); ok {
				if err := h.subscriber.sendDQL(newMsg); err != nil {
					return err
				}
			} else if err := h.subscriber.sendRetry(newMsg); err != nil {
				return err
			}
		}
		sess.MarkMessage(msg, "")
		//sess.Commit()
	}
	return nil
}

func (h kafkaSubscriberHandler) handleMessage(msg *retriable.Message) error {
	var evt retriable.Event
	var err error
	if evt, err = msg.Unmarshal(h.evtType); err != nil {
		return err
	}

	if err = h.process(evt, msg.GetHeaders()); err != nil {
		return err
	}

	return nil
}
