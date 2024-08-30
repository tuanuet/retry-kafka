package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/tuanuet/retry-kafka/consumer"
	"github.com/tuanuet/retry-kafka/producer"
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

	for i := 0; i < 10; i++ {
		if err := publisher.SendMessage(&UserEvent{
			User{
				ID:   uint32(i),
				Name: "tuan",
				Age:  27,
			},
		}, nil); err != nil {
			panic(err)
		}
	}

	fmt.Println("done")

	c := consumer.NewConsumer(
		"test_consumer",
		&UserEvent{},
		[]string{"localhost:9092"},
		consumer.WithBatchFlush(10, 500*time.Millisecond),
		consumer.WithRetries([]consumer.RetryOption{
			{Pending: 10 * time.Second},
			{Pending: 15 * time.Second},
		}),
	)

	//err := c.Consume(context.Background(), func(evt retriable.Event, headers []*retriable.Header) error {
	//	u := evt.(*UserEvent)
	//	fmt.Println(u.Age)
	//
	//	return errors.New("aaaa")
	//})

	//err := c.Consume(context.Background(), func(evt retriable.Event, headers []*retriable.Header) error {
	//	fmt.Println("===========================================================")
	//	u := evt.(*UserEvent)
	//	fmt.Println(u.ID)
	//	return errors.New("errr")
	//})
	//go func() {
	//	err := c.BatchConsume(context.Background(), func(evts []retriable.Event, headers [][]*retriable.Header) error {
	//		fmt.Println("============================================")
	//		fmt.Println(len(evts))
	//		fmt.Println(len(evts))
	//	us := make([]*UserEvent, 0)
	//	for _, evt := range evts {
	//		u := evt.(*UserEvent)
	//			us = append(us, u)
	//		}
	//
	//		return errors.New("aaaa")
	//	})
	//
	//	fmt.Println("[2] doing")
	//	if err != nil {
	//		panic(err)
	//	}
	//}()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	<-signals
	c.Close()
	println("close function!")
}
