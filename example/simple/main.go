package main

import (
	"context"
	"fmt"
	"github.com/tuanuet/retry-kafka/producer"
	"os"
	"os/signal"
	"syscall"

	"github.com/tuanuet/retry-kafka/consumer"
	"github.com/tuanuet/retry-kafka/retriable"
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
	publisher := producer.NewProducer(&UserEvent{}, []string{"localhost:9092"})

	if err := publisher.SendMessage(&UserEvent{
		User{
			ID:   1,
			Name: "tuan",
			Age:  27,
		},
	}, nil); err != nil {
		panic(err)
	}

	c := consumer.NewConsumer("test_consumer", &UserEvent{}, []string{"localhost:9092"})

	//err := c.Consume(context.Background(), func(evt retriable.Event, headers []*retriable.Header) error {
	//	u := evt.(*UserEvent)
	//	fmt.Println(u.Age)
	//
	//	return errors.New("aaaa")
	//})

	err := c.Consume(context.Background(), func(evt retriable.Event, headers []*retriable.Header) error {
		fmt.Println("===========================================================")
		u := evt.(*UserEvent)
		fmt.Println("===========================================================")
		fmt.Println(u.ID)
		return retriable.ErrorWithoutRetry
	})

	//err := c.BatchConsume(context.Background(), func(evts []retriable.Event, headers [][]*retriable.Header) error {
	//	us := make([]*UserEvent, len(evts))
	//	for _, evt := range evts {
	//		u := evt.(*UserEvent)
	//		fmt.Println(u.ID)
	//		us = append(us, u)
	//	}
	//
	//	return errors.New("aaaa")
	//})
	if err != nil {
		panic(err)
	}
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)

	<-signals
	println("close function!")
}
