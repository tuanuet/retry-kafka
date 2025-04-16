package main

import (
	"context"
	"fmt"
	"github.com/tuanuet/retry-kafka/consumer/redis"
	"github.com/tuanuet/retry-kafka/retriable"
	"log"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// User example models
type User struct {
	ID   uint32
	Name string
	Age  int32
}

// UserEvent should implement Event interface
type UserEvent struct {
	User
}

func (u UserEvent) GetTopicName() string {
	return "evt.kafka.user"
}

func (u UserEvent) GetPartitionValue() string {
	return fmt.Sprintf("%d", u.ID)
}

func main() {
	c := redis.NewConsumer(
		"test_consumer",
		&UserEvent{},
		[]string{"localhost:6379"},
		//redis.WithRetries(nil),
	)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	ctx, cancel := context.WithCancel(context.Background())

	if err := c.Consume(ctx, func(evt retriable.Event, headers []*retriable.Header) error {
		//u := evt.(*UserEvent)
		//fmt.Println(u)
		time.Sleep(500 * time.Millisecond)
		return fmt.Errorf("err")
	}); err != nil {
		log.Fatal(err.Error())
	}

	<-signals
	cancel()

	fmt.Println("signal received")

	time.Sleep(1 * time.Second)
}
