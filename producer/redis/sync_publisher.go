package redis

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"time"
)

type syncPublisher struct {
	client *redis.Client
}

func newSyncPublisher(brokers []string) (*syncPublisher, error) {
	client := redis.NewClient(&redis.Options{
		Addr: brokers[0],
	})

	_, err := client.Ping(context.Background()).Result()
	if err != nil {
		return nil, fmt.Errorf("error ping: %v", err)
	}

	return &syncPublisher{
		client: client,
	}, nil
}

func (ap *syncPublisher) SendMessage(ctx context.Context, msg *ProducerMessage) error {
	msg.Timestamp = time.Now()
	_, err := ap.client.XAdd(ctx, &redis.XAddArgs{
		Stream: msg.Stream,
		Values: msg.ToValues(),
	}).Result()
	return err
}

func (ap *syncPublisher) Close() error {
	return ap.client.Close()
}
