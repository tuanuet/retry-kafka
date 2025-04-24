# retry-kafka

## Introduction

**retry-kafka** is a flexible, high-reliability message processing library for Go, designed to make robust event-driven architectures easy. 

- **What:** It provides unified producer and consumer interfaces with automatic retry and dead-letter queue (DLQ) support, working seamlessly with both Kafka and Redis backends.
- **Why:** Message loss, transient errors, and operational hiccups are common in distributed systems. retry-kafka ensures your messages are never lost, giving you fine-grained control over retries, error handling, and observability. It empowers you to build resilient, production-grade pipelines with minimal effort.
- **How:** Just define your event type, plug in your backend of choice, and configure retry policies as needed. retry-kafka handles message delivery, retries with backoff, DLQ routing, and serialization for you—so you can focus on your business logic, not infrastructure plumbing.

## Retry & Dead Letter Queue (DLQ) Architecture

The system uses **retry** and **DLQ** mechanisms to ensure no data loss and increased reliability:

- **Retry:** When a consumer fails to process a message, the message is moved to a retry queue (which can be a separate Kafka topic or Redis stream). Each retry attempt has an increasing delay (configurable). The maximum number of retries is also configurable.
- **DLQ (Dead Letter Queue):** If the message still fails after the maximum number of retries, it is moved to the Dead Letter Queue. You can monitor, analyze, or manually process these messages.

### Processing Flow
```
Producer --> [Main Queue] --> Consumer
                          |--> Retry Queue(s) --> Consumer
                          |--> DLQ
```

- **Main Queue:** The main topic/stream that receives new messages.
- **Retry Queue(s):** There can be multiple retry queues with different delay levels (e.g., 10s, 1m, 5m...).
- **DLQ:** Stores messages that could not be successfully processed.

### Advantages
- No temporary data loss due to system/user errors.
- Can analyze error causes via DLQ.
- Flexible configuration for number of retries and retry intervals.

This system provides a message sending and processing mechanism with retry capability, supporting both **Kafka** and **Redis** backends.

## Purpose

- Ensure messages are not lost in case of temporary errors.
- Support both Redis and Kafka for system flexibility.
- Easy to extend and integrate.

## Overview Architecture

```
[Producer] --(Kafka/Redis)--> [Consumer] --(Processing/Retry/DLQ)-->
```

- **Producer**: Sends messages to Kafka or Redis.
- **Consumer**: Receives messages, processes them, retries on error, and sends to Dead Queue if unprocessable.

## Install v2

You can install retry-kafka v2 using Go modules:

```bash
go get github.com/tuanuet/retry-kafka/v2
```

Make sure your `go.mod` contains the following import:

```
module github.com/your/project

go 1.20

require (
    github.com/tuanuet/retry-kafka/v2 latest
)
```

Then import it in your code:

```go
import "github.com/tuanuet/retry-kafka/v2"
```

## Quick Start Guide

### 1. Define Event

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
package yourpackage
import (
    "github.com/tuanuet/retry-kafka/v2/producer/kafka"
    "github.com/tuanuet/retry-kafka/v2/producer/redis"
)

// Kafka
producer := kafka.NewProducer(&MyEvent{}, []string{"localhost:9092"})
err := producer.SendMessage(&MyEvent{ID: 1, Name: "foo"}, nil)

// Redis
producer := redis.NewProducer(&MyEvent{}, []string{"localhost:6379"})
err := producer.SendMessage(&MyEvent{ID: 2, Name: "bar"}, nil)
```

### 3. Consumer (Kafka/Redis)

```go
package yourpackage

import (
    "github.com/tuanuet/retry-kafka/v2/consumer/kafka"
    "github.com/tuanuet/retry-kafka/v2/consumer/redis"
)

// Kafka
consumer := kafka.NewConsumer("my-group", &MyEvent{}, []string{"localhost:9092"})
err := consumer.Consume(context.Background(), func(evt retriable.Event, headers []*retriable.Header) error {
    myEvt := evt.(*MyEvent)
    // Business logic here
    return nil // or return error to retry
})

// Redis
consumer := redis.NewConsumer("my-group", &MyEvent{}, []string{"localhost:6379"})
err := consumer.Consume(context.Background(), func(evt retriable.Event, headers []*retriable.Header) error {
    myEvt := evt.(*MyEvent)
    // Business logic here
    return nil
})
```

### 4. Batch Consume (optional)

```go
err := consumer.BatchConsume(context.Background(), func(evts []retriable.Event, headers [][]*retriable.Header) error {
    for _, evt := range evts {
        myEvt := evt.(*MyEvent)
        // Process each event
    }
    return nil
})
```

### 5. Using With... Options for Producer & Consumer (optional)

You can provide custom configuration options (such as marshaller, async, partition, etc.) when initializing the Producer or Consumer. Below are common `With...` options for each type:

#### Producer Options
- **Kafka**
  - `kafka.WithMarshaller(marshaller.Marshaller)`: Specify a custom marshaller (e.g., JSON, protobuf).
  - `kafka.WithAsync()`: Enable async publishing to Kafka.
- **Redis**
  - `redis.WithMarshaller(marshaller.Marshaller)`: Specify a custom marshaller.
  - `redis.WithPartitionNum(num int32)`: Set the partition number for Redis streams.

#### Consumer Options
- **Kafka**
  - `kafka.WithMarshaller(marshaller.Marshaller)`: Specify a custom marshaller.
  - `kafka.WithBatchFlush(size int32, timeFlush time.Duration)`: Control batch flush size and interval.
  - `kafka.WithRetries(opts []consumer.RetryOption)`: Custom retry options.
  - `kafka.WithMessageBytes(bytes int32)`: Limit memory usage per message.
  - `kafka.WithMaxProcessDuration(duration time.Duration)`: Set max processing time per message.
  - `kafka.WithBalanceStrategy(strategy)`: Set consumer group balancing strategy.
  - `kafka.WithSessionTimeout(duration time.Duration)`: Set session timeout.
  - `kafka.WithHeartbeatInterval(duration time.Duration)`: Set heartbeat interval.
  - `kafka.WithKafkaVersion(version string)`: Specify Kafka version.
  - `kafka.WithLongProcessing(isLongProcessing bool)`: Enable long processing mode.
  - `kafka.WithLogger(logger Logger)`: Set custom logger.
  - `kafka.WithEnableRetry(enable bool)`: Enable/disable retry.
  - `kafka.WithEnableDlq(enable bool)`: Enable/disable DLQ.
- **Redis**
  - `redis.WithMarshaller(marshaller.Marshaller)`: Specify a custom marshaller.
  - `redis.WithRetries(opts []consumer.RetryOption)`: Custom retry options.
  - `redis.WithLogger(logger Logger)`: Set custom logger.
  - `redis.WithMaxProcessDuration(duration time.Duration)`: Set max processing time per message.
  - `redis.WithEnableRetry(enable bool)`: Enable/disable retry.
  - `redis.WithEnableDlq(enable bool)`: Enable/disable DLQ.
  - `redis.WithUnOrder(unOrder bool)`: Enable unordered (M:N) subscribe mode.

#### Example: Custom Marshaller
```go
import (
    "github.com/tuanuet/retry-kafka/v2/producer/kafka"
    "github.com/tuanuet/retry-kafka/v2/consumer/kafka"
    "github.com/tuanuet/retry-kafka/v2/marshaller"
)

producer := kafka.NewProducer(
    &MyEvent{},
    []string{"localhost:9092"},
    kafka.WithMarshaller(marshaller.NewJSONMarshaller()),
)

consumer := kafka.NewConsumer(
    "my-group",
    &MyEvent{},
    []string{"localhost:9092"},
    kafka.WithMarshaller(marshaller.NewJSONMarshaller()),
)
```

> **Note:** Redis also supports `WithMarshaller` and other options similarly.
> You should use the same marshaller type for both Producer and Consumer to avoid data decoding errors.

## When to use Redis, when to use Kafka?

- **Kafka:** Use when you need high throughput, guaranteed ordering, high distribution, and long-term storage.
- **Redis:** Suitable for small systems, low latency, or if you already have Redis available.

## Contribution & Extension

- Fork the repo, create a PR or issue if you want to contribute.
- Adding new backends is easy thanks to the interface-based architecture.

## ChangeLog
- [2025-04-16] Support redis-stream from version 2.0.0

## Sponsor

If you find **retry-kafka** helpful and want to support its ongoing development, please consider becoming a sponsor! Your sponsorship helps me dedicate more time to improving the library, adding new features, and providing better documentation and support for the community.

- ⭐ Star the repo to show your support!
- 💬 Share feedback, ideas, or feature requests via issues or discussions.
- 🤝 Interested in sponsoring or partnering? Reach out via [GitHub Sponsors](https://github.com/sponsors/tuanuet) or email me directly.

Thank you for helping make open source better for everyone!