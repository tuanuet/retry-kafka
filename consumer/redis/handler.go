package redis

import (
	"context"
	"errors"
	"fmt"
	"github.com/redis/go-redis/v9"
	"github.com/tuanuet/retry-kafka/consumer"
	"reflect"
	"time"

	"github.com/tuanuet/retry-kafka/retriable"
)

// redisSubscriberHandler is the struct of handler.
type redisSubscriberHandler struct {
	process    consumer.HandleFunc
	subscriber *rConsumer
	evtType    reflect.Type
}

// newRedisSubscriberHandler create an instance from consumer.
func newRedisSubscriberHandler(evtType reflect.Type, subscriber *rConsumer, handler consumer.HandleFunc) *redisSubscriberHandler {
	h := redisSubscriberHandler{
		subscriber: subscriber,
		process:    handler,
		evtType:    evtType,
	}
	return &h
}

// ConsumeClaim implements the method of interface.
func (h *redisSubscriberHandler) ConsumeClaim(ctx context.Context, r *rConsumer, results []redis.XStream) error {
	for _, result := range results {
		select {
		case <-ctx.Done():
			return nil
		default:
			stream := result.Stream
			msgs := result.Messages

			for _, msg := range msgs {
				newMsg := retriable.NewRedisMessage(msg, stream, h.subscriber.marshaller)
				topic := h.subscriber.getTopic(newMsg.GetTopicName())
				since := newMsg.GetSinceTime()

				if topic.Pending-since > time.Millisecond*100 {
					// pause consume
					time.Sleep(topic.Pending - since)
				}

				if err := h.handleMessage(newMsg); err != nil {
					// Append more header
					newMsg.SetHeaderByKey([]byte("_retry_error"), []byte(err.Error()))

					if ok := errors.Is(err, retriable.ErrorWithoutRetry); ok {
						if err = h.subscriber.sendDLQ(newMsg); err != nil {
							return fmt.Errorf("error send msg to dlq: %w", err)
						}
					} else if err = h.subscriber.sendRetry(newMsg); err != nil {
						return fmt.Errorf("error sending message to redis for topic %s: %w", newMsg.GetTopicName(), err)
					}
				}

				// Acknowledge the message in the correct stream
				if err := r.Ack(context.Background(), stream, msg.ID); err != nil {
					return fmt.Errorf("error when ack: %w", err)
				}
			}
		}
	}

	return nil
}

func (h *redisSubscriberHandler) handleMessage(msg retriable.Message) error {
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
