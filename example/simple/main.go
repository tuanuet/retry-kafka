package main

import (
	"context"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/tuanuet/retry-kafka/retriable"
	"log"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/tuanuet/retry-kafka/consumer"
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
	//go func() {
	//	log.Println(http.ListenAndServe("localhost:6060", nil))
	//}()
	//
	//publisher := producer.NewProducer(&UserEvent{}, []string{"localhost:9092"}, producer.WithAsync())
	//
	//for i := 0; i < 1000; i++ {
	//	if err := publisher.SendMessage(&UserEvent{
	//		User{
	//			ID:   uint32(i),
	//			Name: "tuan",
	//			Age:  27,
	//		},
	//	}, nil); err != nil {
	//		panic(err)
	//	}
	//}
	//
	//fmt.Println("done")
	//return
	c := consumer.NewConsumer(
		"test_consumer",
		&UserEvent{},
		[]string{"localhost:9092"},
		consumer.WithRetries(nil),
		consumer.WithBalanceStrategy(sarama.NewBalanceStrategyRoundRobin()),
		consumer.WithMaxProcessDuration(3*time.Second),
		consumer.WithSessionTimeout(10*time.Second),
		consumer.WithHeartHeartbeat(3*time.Second),
	)

	//go func() {
	//	http.HandleFunc("/", func(writer http.ResponseWriter, request *http.Request) {
	//		rebalance, err := c.ShouldReBalance()
	//		if err != nil {
	//			writer.Write([]byte(err.Error()))
	//			return
	//		}
	//
	//		if rebalance {
	//			writer.Write([]byte("rebalance"))
	//		} else {
	//			writer.Write([]byte("not rebalance"))
	//		}
	//	})
	//
	//	fmt.Println("Server listening on port 1234 ...")
	//	fmt.Println(http.ListenAndServe(":1234", nil))
	//}()

	if err := c.Consume(context.Background(), func(evt retriable.Event, headers []*retriable.Header) error {
		u := evt.(*UserEvent)
		fmt.Println(u)
		time.Sleep(2000 * time.Millisecond)
		return nil
	}); err != nil {
		log.Fatal(err.Error())
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	<-signals
	fmt.Println("signal received")
	c.Close()
	println("close function!")
}
