package consumer

import (
	"errors"
	"reflect"
	"sort"
	"time"

	"github.com/IBM/sarama"
	"github.com/tuanuet/retry-kafka/retriable"
)

// kafkaSubscriberBatchHandler is the struct of handler.
type kafkaSubscriberBatchHandler struct {
	process       BatchHandleFunc
	subscriber    *kConsumer
	evtType       reflect.Type
	batchSize     int32
	batchDuration time.Duration
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
	handler BatchHandleFunc,
	opts ...BatchHandlerOption,
) *kafkaSubscriberBatchHandler {
	h := &kafkaSubscriberBatchHandler{
		subscriber:    subscriber,
		process:       handler,
		evtType:       evtType,
		batchSize:     10,
		batchDuration: 100 * time.Millisecond,
	}

	for _, opt := range opts {
		opt(h)
	}

	return h
}

// Setup implements the method of interface.
func (kafkaSubscriberBatchHandler) Setup(_ sarama.ConsumerGroupSession) error {
	return nil
}

// Cleanup implements the method of interface.
func (kafkaSubscriberBatchHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim implements the method of interface.
func (h kafkaSubscriberBatchHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	batchSize := h.batchSize
	batchDuration := h.batchDuration
	events := make([]*retriable.Message, 0)

	for {
		select {
		case <-time.After(batchDuration):
			// exec handler
			if err := h.execMessages(sess, events); err != nil {
				return err
			}
			events = make([]*retriable.Message, 0)
		case msg := <-claim.Messages():
			newMsg := retriable.NewMessage(msg, h.subscriber.marshaler)
			topic := h.subscriber.getTopic(newMsg.GetTopicName())
			since := newMsg.GetSinceTime()
			topicPause := map[string][]int32{msg.Topic: {msg.Partition}}
			if topic.Pending-since > time.Millisecond*100 {
				h.subscriber.consumerGroup.Pause(topicPause)
				// flush message
				if err := h.execMessages(sess, events); err != nil {
					return err
				}
				events = make([]*retriable.Message, 0)

				time.Sleep(topic.Pending - since)
			}

			h.subscriber.consumerGroup.Resume(topicPause)
			events = append(events, newMsg)

			if len(events) >= int(batchSize) {
				// exec handler
				if err := h.execMessages(sess, events); err != nil {
					return err
				}
				events = make([]*retriable.Message, 0)
				continue
			}

		case <-sess.Context().Done():
			// exec handler
			if err := h.execMessages(sess, events); err != nil {
				return err
			}
			return nil
		}
	}
}

func (h kafkaSubscriberBatchHandler) execMessages(sess sarama.ConsumerGroupSession, msgs []*retriable.Message) error {
	if len(msgs) == 0 {
		return nil
	}
	if errs := h.handleMessages(msgs); errs != nil {
		for i, msg := range msgs {
			err := errs[i]
			if err != nil {
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
	sess.MarkMessage(msgs[len(msgs)-1].GetRaw(), "")
	sess.Commit()
	return nil
}

func (h kafkaSubscriberBatchHandler) handleMessages(msgs []*retriable.Message) (errs []error) {
	type destWithError struct {
		dest retriable.Event
		err  error
		idx  int
	}
	destMap := make(map[int]destWithError)
	for i, msg := range msgs {
		evt, err := msg.Unmarshal(h.evtType)
		destMap[i] = destWithError{
			dest: evt,
			err:  err,
			idx:  i,
		}
	}

	destSuccesses := make([]destWithError, 0)
	for _, dwe := range destMap {
		if dwe.err != nil {
			continue
		}
		destSuccesses = append(destSuccesses, dwe)
	}

	dests := make([]retriable.Event, 0)
	for _, d := range destSuccesses {
		dests = append(dests, d.dest)
	}

	if len(dests) != 0 {
		err := h.process(dests, nil)
		if err != nil {
			// handler error here
			errIndexes := make([]int, 0)
			berr, castOk := err.(*retriable.ErrorBatchHandler)

			if !castOk {
				// return all virtual message index
				for i, _ := range dests {
					errIndexes = append(errIndexes, i)
				}
			} else {
				// return virtual indexes
				for _, index := range berr.Indexes {
					errIndexes = append(errIndexes, index)
				}
			}
			// update for destMap
			for _, vIdx := range errIndexes {
				trueErrorIdx := destSuccesses[vIdx].idx
				dest := destMap[trueErrorIdx]
				dest.err = errors.New("process partial error")
				destMap[trueErrorIdx] = dest
			}
		}
	}

	// return full error here
	// Wrong order
	keys := make([]int, 0, len(destMap))
	for idx := range destMap {
		keys = append(keys, idx)
	}
	sort.Ints(keys)

	for _, idx := range keys {
		errs = append(errs, destMap[idx].err)
	}
	// return all error by each message
	if len(errs) != 0 {
		return
	}

	return nil
}
