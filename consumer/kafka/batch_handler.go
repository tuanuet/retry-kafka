package kafka

import (
	"errors"
	"fmt"
	"log"
	"reflect"
	"time"

	"github.com/IBM/sarama"
	"github.com/tuanuet/retry-kafka/v2/consumer"
	"github.com/tuanuet/retry-kafka/v2/retriable"
)

// kafkaSubscriberBatchHandler is the struct of handler.
type kafkaSubscriberBatchHandler struct {
	process       consumer.BatchHandleFunc
	subscriber    *kConsumer
	evtType       reflect.Type
	batchSize     int32
	batchDuration time.Duration
	ready         chan bool
}

// BatchHandlerOption ...
type BatchHandlerOption func(handler *kafkaSubscriberBatchHandler)

// withBatchConfig ...
func withBatchConfig(size int32, timeFlush time.Duration) BatchHandlerOption {
	return func(handler *kafkaSubscriberBatchHandler) {
		if size != 0 {
			handler.batchSize = size
		}

		if timeFlush != 0 {
			handler.batchDuration = timeFlush
		}

	}
}

// newKafkaSubscriberBatchHandler create an instance from consumer.
func newKafkaSubscriberBatchHandler(
	evtType reflect.Type,
	subscriber *kConsumer,
	handler consumer.BatchHandleFunc,
	opts ...BatchHandlerOption,
) *kafkaSubscriberBatchHandler {
	h := &kafkaSubscriberBatchHandler{
		subscriber:    subscriber,
		process:       handler,
		evtType:       evtType,
		batchSize:     10,
		batchDuration: 100 * time.Millisecond,
		ready:         make(chan bool),
	}

	for _, opt := range opts {
		opt(h)
	}

	return h
}

// Setup implements the method of interface.
func (h *kafkaSubscriberBatchHandler) Setup(_ sarama.ConsumerGroupSession) error {
	close(h.ready)
	return nil
}

// Cleanup implements the method of interface.
func (h *kafkaSubscriberBatchHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim implements the method of interface.
func (h *kafkaSubscriberBatchHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	eventChan := h.produceEvents(sess, claim)

	for events := range eventChan {
		// flush message
		if err := h.execMessages(sess, events); err != nil {
			return err
		}
	}

	return nil
}

func (h *kafkaSubscriberBatchHandler) produceEvents(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) chan []retriable.Message {
	eventChan := make(chan []retriable.Message, 0)
	batchSize := h.batchSize
	batchDuration := h.batchDuration
	events := make([]retriable.Message, 0)
	go func() {
		defer close(eventChan)
		for {
			select {
			case <-time.After(batchDuration):
				eventChan <- events
				events = make([]retriable.Message, 0)
			case msg := <-claim.Messages():
				if msg == nil {
					log.Println("message was nil")
					continue
				}

				newMsg := retriable.NewKafkaMessage(msg, h.subscriber.marshaller)
				topic := h.subscriber.getTopic(newMsg.GetTopicName())
				since := newMsg.GetSinceTime()
				topicPause := map[string][]int32{msg.Topic: {msg.Partition}}
				if topic.Pending-since > time.Millisecond*200 {
					h.subscriber.consumerGroup.Pause(topicPause)
					// flush all message
					eventChan <- events
					events = make([]retriable.Message, 0)
					time.Sleep(topic.Pending - since)
				}

				h.subscriber.consumerGroup.Resume(topicPause)
				events = append(events, newMsg)

				if len(events) >= int(batchSize) {
					eventChan <- events
					events = make([]retriable.Message, 0)
					continue
				}

			case <-sess.Context().Done():
				eventChan <- events
				events = make([]retriable.Message, 0)
				return
			}
		}
	}()

	return eventChan
}

func (h *kafkaSubscriberBatchHandler) execMessages(sess sarama.ConsumerGroupSession, msgs []retriable.Message) error {
	if len(msgs) == 0 {
		return nil
	}
	if errs := h.handleMessages(msgs); errs != nil {
		for i, msg := range msgs {
			if err := errs[i]; err != nil {
				if ok := errors.Is(err, retriable.ErrorWithoutRetry); ok {
					if err := h.subscriber.sendDQL(msg); err != nil {
						return err
					}
				} else if err := h.subscriber.sendRetry(msg); err != nil {
					return err
				}
			}
		}
	}
	msg := msgs[len(msgs)-1].GetRaw()
	m, ok := msg.(sarama.ConsumerMessage)
	if !ok {
		return fmt.Errorf("message was not a ConsumerMessage")
	}
	sess.MarkOffset(m.Topic, m.Partition, m.Offset, "")
	//sess.Commit()
	return nil
}

func (h *kafkaSubscriberBatchHandler) handleMessages(msgs []retriable.Message) (errs []error) {
	type destWithError struct {
		dest retriable.Event
		err  error
	}

	// Preallocate for efficiency
	dests := make([]retriable.Event, 0, len(msgs))
	errIdxMap := make([]int, 0)
	errSlice := make([]error, len(msgs))

	// Unmarshal all messages, track errors and successes
	for i, msg := range msgs {
		evt, err := msg.Unmarshal(h.evtType)
		if err != nil {
			errSlice[i] = err
			continue
		}
		dests = append(dests, evt)
		errSlice[i] = nil
	}

	if len(dests) > 0 {
		err := h.process(dests, nil)
		if err != nil {
			berr, castOk := err.(*retriable.ErrorBatchHandler)
			if !castOk {
				// Mark all as error
				for i := range dests {
					errIdxMap = append(errIdxMap, i)
				}
			} else {
				for _, idx := range berr.Indexes {
					errIdxMap = append(errIdxMap, idx)
				}
			}
			// Map errors back to original positions
			j := 0
			for i := 0; i < len(msgs) && j < len(errIdxMap); i++ {
				if errSlice[i] == nil {
					if contains(errIdxMap, j) {
						errSlice[i] = errors.New("process partial error")
						j++
					}
				}
			}
		}
	}

	return errSlice
}

func contains(arr []int, val int) bool {
	for _, v := range arr {
		if v == val {
			return true
		}
	}
	return false
}
