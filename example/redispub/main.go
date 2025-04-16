package main

import (
	"fmt"
	redisp "github.com/tuanuet/retry-kafka/producer/redis"
	"github.com/tuanuet/retry-kafka/retriable"
	_ "net/http/pprof"
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
	publisher := redisp.NewProducer(&UserEvent{}, []string{"localhost:6379"})

	for i := 0; i < 10; i++ {
		if err := publisher.SendMessage(&UserEvent{
			User{
				ID:   uint32(i),
				Name: "tuan",
				Age:  27,
			},
		}, []*retriable.Header{
			{
				Key:   []byte("key"),
				Value: []byte(fmt.Sprintf("%d", i)),
			},
		}); err != nil {
			panic(err)
		}
	}

	fmt.Println("done")
	return
}
