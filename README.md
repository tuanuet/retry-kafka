# retry-kafka

## Kiến trúc Retry & Dead Letter Queue (DLQ)

Hệ thống sử dụng cơ chế **retry** và **DLQ** để đảm bảo không mất dữ liệu và tăng độ tin cậy:

- **Retry:** Khi consumer xử lý message thất bại, message sẽ được chuyển sang một hàng đợi retry (có thể là topic Kafka/stream Redis riêng). Mỗi lần retry sẽ có delay tăng dần (configurable). Số lần retry tối đa có thể cấu hình.
- **DLQ (Dead Letter Queue):** Nếu message vẫn lỗi sau số lần retry tối đa, nó sẽ được chuyển sang Dead Letter Queue. Bạn có thể monitor, phân tích, hoặc xử lý thủ công các message này.

### Luồng xử lý
```
Producer --> [Main Queue] --> Consumer
                          |--> Retry Queue(s) --> Consumer
                          |--> DLQ
```

- **Main Queue:** Topic/stream chính nhận message mới.
- **Retry Queue(s):** Có thể có nhiều hàng đợi retry với các mức delay khác nhau (ví dụ: 10s, 1m, 5m...).
- **DLQ:** Lưu trữ các message không thể xử lý thành công.

### Ưu điểm
- Không mất dữ liệu tạm thởi do lỗi hệ thống/người dùng.
- Có thể phân tích nguyên nhân lỗi qua DLQ.
- Linh hoạt cấu hình số lần và thời gian retry.

Hệ thống này cung cấp cơ chế gửi và xử lý message với khả năng retry, hỗ trợ cả hai backend: **Kafka** và **Redis**.

## Mục đích

- Đảm bảo message không bị mất khi gặp lỗi tạm thởi.
- Hỗ trợ cả Redis và Kafka để linh hoạt theo nhu cầu hệ thống.
- Dễ dàng mở rộng, tích hợp.

## Kiến trúc tổng quan

```
[Producer] --(Kafka/Redis)--> [Consumer] --(Xử lý/Retry/DQL)-->
```

- **Producer**: Gửi message vào Kafka hoặc Redis.
- **Consumer**: Nhận message, xử lý, retry nếu lỗi, gửi vào Dead Queue nếu không thể xử lý.

## Hướng dẫn sử dụng nhanh

### 1. Định nghĩa Event

```go
type MyEvent struct {
    ID   int
    Name string
}

func (e MyEvent) GetTopicName() string        { return "my-topic" }
func (e MyEvent) GetPartitionValue() string   { return fmt.Sprintf("%d", e.ID) }
```

### 2. Producer (Kafka/Redis)

```go
import (
    "github.com/tuanuet/retry-kafka/producer/kafka"
    "github.com/tuanuet/retry-kafka/producer/redis"
)

// Kafka
producer := kafka.NewProducer(kafka.Config{
    Brokers: []string{"localhost:9092"},
    Topic:   "my-topic",
})
err := producer.SendMessage(MyEvent{ID: 1, Name: "foo"})

// Redis
producer := redis.NewProducer(redis.Config{
    Addr:  "localhost:6379",
    Topic: "my-redis-stream",
})
err := producer.SendMessage(MyEvent{ID: 2, Name: "bar"})
```

### 3. Consumer (Kafka/Redis)

```go
import (
    "context"
    "github.com/tuanuet/retry-kafka/consumer/kafka"
    "github.com/tuanuet/retry-kafka/consumer/redis"
)

// Kafka
consumer := kafka.NewConsumer(kafka.Config{
    Brokers: []string{"localhost:9092"},
    Topic:   "my-topic",
    GroupID: "my-group",
})
err := consumer.Consume(context.Background(), func(evt retriable.Event, headers []*retriable.Header) error {
    myEvt := evt.(*MyEvent)
    // Xử lý logic ở đây
    return nil // hoặc return error để retry
})

// Redis
consumer := redis.NewConsumer(redis.Config{
    Addr:  "localhost:6379",
    Topic: "my-redis-stream",
})
err := consumer.Consume(context.Background(), func(evt retriable.Event, headers []*retriable.Header) error {
    myEvt := evt.(*MyEvent)
    // Xử lý logic ở đây
    return nil
})
```

### 4. Batch Consume (nếu muốn)

```go
err := consumer.BatchConsume(context.Background(), func(evts []retriable.Event, headers [][]*retriable.Header) error {
    for _, evt := range evts {
        myEvt := evt.(*MyEvent)
        // Xử lý từng event
    }
    return nil
})
```

### 5. Sử dụng WithMarshaller cho Producer & Consumer (tuỳ chọn)

Bạn có thể truyền custom marshaller (ví dụ: JSON, protobuf,...) khi khởi tạo Producer hoặc Consumer để tuỳ biến serialization/deserialization. Ví dụ với Kafka:

```go
import (
    "github.com/tuanuet/retry-kafka/producer/kafka"
    "github.com/tuanuet/retry-kafka/consumer/kafka"
    "github.com/tuanuet/retry-kafka/marshaller"
)

producer := kafka.NewProducer(
    kafka.Config{...},
    kafka.WithMarshaller(marshaller.NewJSONMarshaller()),
)

consumer := kafka.NewConsumer(
    kafka.Config{...},
    kafka.WithMarshaller(marshaller.NewJSONMarshaller()),
)
```

> **Lưu ý:** Redis cũng hỗ trợ WithMarshaller với cách dùng tương tự.
> Nên dùng cùng loại marshaller cho cả Producer và Consumer để tránh lỗi giải mã dữ liệu.

## Khi nào nên dùng Redis, khi nào dùng Kafka?

- **Kafka**: Sử dụng khi cần throughput lớn, đảm bảo thứ tự, phân tán cao, lưu trữ lâu dài.
- **Redis**: Phù hợp cho các hệ thống nhỏ, latency thấp, hoặc khi bạn đã có Redis sẵn.

## Đóng góp & mở rộng

- Fork repo, tạo PR hoặc issue nếu bạn muốn đóng góp.
- Hỗ trợ thêm backend khác dễ dàng nhờ kiến trúc interface.

## ChangeLog
- [2025-04-16] Support redis-stream from version 2.0.0